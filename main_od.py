"""
Unified OneDrive Claims Processor
Watches input folder, processes PDFs starting with 'acord_', generates PDF + HTML reports, saves to output folder
"""

import os
import sys
import time
import json
import re
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

# Import your existing modules
from main import ClaimsAnalysisOrchestrator
from onedrive_client_app import OneDriveClientApp
from email_sender import EmailSender, load_email_metadata, get_recipient_email

# Import shared session storage from api_server (for unified server mode)
try:
    from api_server import sessions, pending_frontend_data, SessionData, extract_details, save_underwriting_data, save_underwriting_results_to_policy_db
    UNIFIED_MODE = True
    print("[WATCHER] Running in UNIFIED mode - sharing sessions and DB saving with API server")
except ImportError:
    # Standalone mode - create local storage
    sessions = {}
    pending_frontend_data = {}
    UNIFIED_MODE = False
    print("[WATCHER] Running in STANDALONE mode")

# --- Configuration ---
load_dotenv()

CONFIG = {
    # OneDrive Authentication
    "TENANT_ID": os.getenv("ONEDRIVE_TENANT_ID"),
    "CLIENT_ID": os.getenv("ONEDRIVE_CLIENT_ID"),
    "CLIENT_SECRET": os.getenv("ONEDRIVE_CLIENT_SECRET"),
    "USER_EMAIL": os.getenv("ONEDRIVE_USER_EMAIL"),
    
    # Folder Configuration
    "INPUT_FOLDER": os.getenv("ONEDRIVE_FOLDER_NAME", "Input_attachments"),
    "OUTPUT_FOLDER": os.getenv("ONEDRIVE_OUTPUT_FOLDER", "Output_attachments"),
    "PROCESSED_FOLDER": os.getenv("ONEDRIVE_PROCESSED_INPUTS", "Processed_inputs"),
    "UNDERWRITING_FOLDER": os.getenv("ONEDRIVE_UNDERWRITING_FOLDER", "Underwriting"),
    
    # Local temporary directories
    "TEMP_INPUT_DIR": "./temp_input",
    "TEMP_OUTPUT_DIR": "./temp_output",
    
    # Processing
    "POLL_INTERVAL": int(os.getenv("POLL_INTERVAL", "5")),
    "PROCESS_EXTENSION": ".pdf",
    "FILE_PREFIX": os.getenv("FILE_PREFIX", "acord_")  # Only process files starting with this prefix
}


class OneDriveProcessor:
    """Handles complete OneDrive integration with processing pipeline"""
    
    def __init__(self):
        self.input_client = None
        self.output_client = None
        self.orchestrator = None
        self.email_sender = None
        self.processed_cache = set()
        
        # Create temp directories
        os.makedirs(CONFIG['TEMP_INPUT_DIR'], exist_ok=True)
        os.makedirs(CONFIG['TEMP_OUTPUT_DIR'], exist_ok=True)
        
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize OneDrive clients and orchestrator"""
        print("\n" + "="*70)
        print("INITIALIZING ONEDRIVE CLAIMS PROCESSOR")
        print("="*70)
        
        # Validate credentials
        required = ["TENANT_ID", "CLIENT_ID", "CLIENT_SECRET", "USER_EMAIL"]
        missing = [k for k in required if not CONFIG[k]]
        if missing:
            raise ValueError(f"Missing credentials: {', '.join(missing)}")
        
        # Initialize input folder client
        self.input_client = OneDriveClientApp(
            tenant_id=CONFIG['TENANT_ID'],
            client_id=CONFIG['CLIENT_ID'],
            client_secret=CONFIG['CLIENT_SECRET'],
            user_email=CONFIG['USER_EMAIL'],
            folder_name=CONFIG['INPUT_FOLDER']
        )
        
        # Initialize output folder client
        self.output_client = OneDriveClientApp(
            tenant_id=CONFIG['TENANT_ID'],
            client_id=CONFIG['CLIENT_ID'],
            client_secret=CONFIG['CLIENT_SECRET'],
            user_email=CONFIG['USER_EMAIL'],
            folder_name=CONFIG['OUTPUT_FOLDER']
        )
        
        # Initialize email sender
        self.email_sender = EmailSender(
            tenant_id=CONFIG['TENANT_ID'],
            client_id=CONFIG['CLIENT_ID'],
            client_secret=CONFIG['CLIENT_SECRET'],
            user_email=CONFIG['USER_EMAIL']
        )
        
        # Initialize analysis orchestrator
        self.orchestrator = ClaimsAnalysisOrchestrator(
            output_dir=CONFIG['TEMP_OUTPUT_DIR']
        )
        
        print(f"✓ Input folder: {CONFIG['INPUT_FOLDER']}")
        print(f"✓ Output folder: {CONFIG['OUTPUT_FOLDER']}")
        print(f"✓ Processed folder: {CONFIG['PROCESSED_FOLDER']}")
        print(f"✓ File filter: Files starting with '{CONFIG['FILE_PREFIX']}'")
        print(f"✓ Poll interval: {CONFIG['POLL_INTERVAL']} seconds")
        print(f"✓ Email notifications enabled")
    
    def _should_process_file(self, filename: str) -> bool:
        """
        Check if file should be processed based on naming criteria.
        
        Args:
            filename: Name of the file
            
        Returns:
            True if file should be processed, False otherwise
        """
        # Check if file has correct extension
        if not filename.lower().endswith(CONFIG['PROCESS_EXTENSION'].lower()):
            return False
        
        # Check if file starts with required prefix (case-insensitive)
        if not filename.lower().startswith(CONFIG['FILE_PREFIX'].lower()):
            return False
        
        # Check if already processed
        if filename in self.processed_cache:
            return False
        
        return True
    
    def _is_companion_json(self, filename: str) -> bool:
        """Check if file is a companion JSON for a PDF"""
        return filename.lower().endswith('.pdf.json') and \
               filename.lower().startswith(CONFIG['FILE_PREFIX'].lower())
    
    def _extract_identifier_from_email(self, email_metadata: dict) -> str:
        """
        Extract policy number, claim number, or related term number from email subject and body.
        Uses LLM to intelligently extract identifiers.
        
        Args:
            email_metadata: Dictionary with email metadata (subject, body, etc.)
            
        Returns:
            Extracted identifier or 'UNKNOWN' if not found
        """
        if not email_metadata:
            return 'UNKNOWN'
        
        # Get subject and body (prefer full body over bodyPreview)
        subject = email_metadata.get('subject', '')
        body = email_metadata.get('body', '')
        body_preview = email_metadata.get('bodyPreview', '')
        
        # Use full body if available, otherwise use bodyPreview
        email_body = body if body else body_preview
        
        if not subject and not email_body:
            print(f"   ⚠ No subject or body available")
            return 'UNKNOWN'
        
        # Debug: Show what we're analyzing
        print(f"   📧 Analyzing - Subject: {subject[:50]}..." if len(subject) > 50 else f"   📧 Analyzing - Subject: {subject}")
        print(f"   📧 Body length: {len(email_body)} characters")
        
        # Try to use LLM for extraction
        try:
            api_key = os.getenv('OPENROUTER_API_KEY')
            if not api_key:
                print(f"   ⚠ OPENROUTER_API_KEY not found, using fallback")
                return self._regex_fallback_extraction(subject, email_body)
            
            # Use existing LLM configuration
            llm = ChatOpenAI(
                model="meta-llama/llama-3.3-70b-instruct",
                api_key=api_key,
                base_url="https://openrouter.ai/api/v1",
                temperature=0.1,
            )
            
            # Truncate body if too long (keep first 2000 chars)
            email_body_truncated = email_body[:2000] if len(email_body) > 2000 else email_body
            
            prompt = f"""You are an expert at extracting insurance-related identifiers from emails.

Email Subject: {subject}

Email Body:
{email_body_truncated}

Task: Find and extract ANY of these identifiers:
- Policy number (e.g., "policy no: 516787623" or "policy #ABC123")
- Claim number (e.g., "claim no: CLM789012")
- Quote number
- Account number
- Reference number
- Related term number

IMPORTANT:
- Look carefully through BOTH the subject and body
- Numbers may appear after phrases like "policy no:", "claim no:", "policy number:", etc.
- Return ONLY the number/identifier itself (digits, letters, hyphens only)
- If you find multiple identifiers, return the FIRST one mentioned
- If NO identifier is found, return exactly: UNKNOWN

Extract the identifier now (just the identifier, nothing else):"""
            
            response = llm.invoke([HumanMessage(content=prompt)])
            identifier = response.content.strip()
            
            print(f"   🤖 LLM response: {identifier}")
            
            # Clean up the identifier (remove special chars except hyphen)
            identifier = re.sub(r'[^A-Z0-9\-]', '', identifier.upper())
            
            if identifier and identifier != 'UNKNOWN' and len(identifier) >= 4:
                print(f"   ✓ LLM extracted identifier: {identifier}")
                return identifier
            else:
                print(f"   ⚠ LLM could not extract valid identifier")
                # Fallback: Try regex extraction from subject
                return self._regex_fallback_extraction(subject, email_body)
                
        except Exception as e:
            print(f"   ⚠ LLM extraction failed: {str(e)}")
            # Fallback: Try regex extraction from subject and body
            return self._regex_fallback_extraction(subject, email_body)
    
    def _regex_fallback_extraction(self, subject: str, body: str) -> str:
        """
        Fallback regex-based extraction for policy/claim numbers.
        
        Args:
            subject: Email subject
            body: Email body
            
        Returns:
            Extracted identifier or 'UNKNOWN'
        """
        # Combine subject and body for searching
        combined_text = f"{subject} {body}"
        
        # Common patterns for policy/claim numbers
        patterns = [
            r'policy\s*(?:no|number|#)[\s:.\-]*([A-Z0-9\-]{5,})',  # policy no: 987888392
            r'claim\s*(?:no|number|#)[\s:.\-]*([A-Z0-9\-]{5,})',   # claim no: CLM123
            r'quote\s*(?:no|number|#)[\s:.\-]*([A-Z0-9\-]{5,})',   # quote no: Q123
            r'account\s*(?:no|number|#)[\s:.\-]*([A-Z0-9\-]{5,})', # account no: ACC123
            r'ref(?:erence)?\s*(?:no|number|#)?[\s:.\-]*([A-Z0-9\-]{5,})', # ref: REF123
            r'\b(?:PN|CLM|POL|QT)[\-]?([A-Z0-9]{5,})\b',          # PN-123456, CLM123456
        ]
        
        for pattern in patterns:
            match = re.search(pattern, combined_text, re.IGNORECASE)
            if match:
                identifier = match.group(1).strip().upper()
                identifier = re.sub(r'[^A-Z0-9\-]', '', identifier)  # Clean it
                if len(identifier) >= 4:
                    print(f"   ✓ Regex extracted identifier: {identifier}")
                    return identifier
        
        print(f"   ⚠ No identifier found via regex fallback")
        return 'UNKNOWN'
    
    def _download_email_as_eml(self, email_metadata: dict, output_path: str, receiver_email: str = None) -> bool:
        """
        Download email as EML file from Microsoft Graph API.
        Requires Mail.Read permission.
        
        Args:   
            email_metadata: Email metadata dictionary with id, internetMessageId, and from fields
            output_path: Local path to save the EML file
            receiver_email: Email address of the receiver (if None, uses configured USER_EMAIL)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get access token (reuse email_sender's token)
            headers = self.email_sender._get_headers()
            
            # Extract message ID and sender
            message_id = email_metadata.get('id')
            internet_message_id = email_metadata.get('internetMessageId')
            sender_email = email_metadata.get('from')
            
            # Use receiver email if provided, otherwise fall back to configured email
            target_email = receiver_email or CONFIG['USER_EMAIL']
            
            # Strategy 1: Try downloading from receiver's mailbox using internetMessageId search
            if internet_message_id and target_email:
                print(f"   📥 Searching in receiver's mailbox: {target_email}")
                # Search for the email by internetMessageId
                search_url = f"https://graph.microsoft.com/v1.0/users/{target_email}/messages"
                params = {
                    "$filter": f"internetMessageId eq '{internet_message_id}'",
                    "$select": "id"
                }
                search_response = requests.get(search_url, headers=headers, params=params)
                
                if search_response.status_code == 200:
                    search_data = search_response.json()
                    if search_data.get('value') and len(search_data['value']) > 0:
                        receiver_message_id = search_data['value'][0]['id']
                        print(f"   ✓ Found email in receiver's mailbox")
                        
                        # Download using the receiver's message ID
                        download_url = f"https://graph.microsoft.com/v1.0/users/{target_email}/messages/{receiver_message_id}/$value"
                        download_response = requests.get(download_url, headers=headers)
                        
                        if download_response.status_code == 200:
                            with open(output_path, 'wb') as f:
                                f.write(download_response.content)
                            print(f"   ✓ Downloaded email as EML from receiver: {os.path.basename(output_path)}")
                            return True
                    else:
                        print(f"   ⚠ Email not found in receiver's mailbox")
                else:
                    print(f"   ⚠ Search failed: {search_response.status_code}")
            
            # Strategy 2: Fallback to sender's mailbox using the message ID
            if message_id and sender_email:
                print(f"   📥 Trying to download EML from sender's mailbox: {sender_email}")
                url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/messages/{message_id}/$value"
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    print(f"   ✓ Downloaded email as EML from sender: {os.path.basename(output_path)}")
                    return True
                else:
                    print(f"   ⚠ Failed from sender mailbox: {response.status_code}")
            
            print(f"   ⚠ Could not download EML using any method")
            return False
                
        except Exception as e:
            print(f"   ⚠ Error downloading EML: {str(e)}")
            return False
    
    def _generate_html_report(self, property_df: pd.DataFrame, 
                             claims_df: pd.DataFrame, 
                             scored_df: pd.DataFrame,
                             client_name: str,
                             input_pdf_name: str = None) -> str:
        """Generate HTML report
        
        Args:
            input_pdf_name: Optional input PDF filename to base output name on
        """
        
        from html_generator import ClaimsLikelihoodHtmlGenerator
        
        try:
            generator = ClaimsLikelihoodHtmlGenerator(
                input_df=property_df,
                claims_df=claims_df if claims_df is not None and len(claims_df) > 0 else pd.DataFrame(),
                output_df=scored_df
            )
            
            if input_pdf_name:
                # Use same naming as PDF: {input_name}_report.html
                base_name = input_pdf_name
                if base_name.lower().endswith('.pdf'):
                    base_name = base_name[:-4]
                html_filename = f"{base_name}_report.html"
            else:
                # Fallback to original naming
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                html_filename = f"Report_{safe_name}_{timestamp}.html"
            
            html_path = os.path.join(CONFIG['TEMP_OUTPUT_DIR'], html_filename)
            
            generator.generate_html(output_path=html_path)
            
            return html_path
            
        except Exception as e:
            print(f"   ⚠ Warning: HTML generation failed: {str(e)}")
            return None
    
    def _upload_to_onedrive(self, local_file_path: str, folder_path: str = None) -> dict:
        """Upload a file to OneDrive folder
        
        Args:
            local_file_path: Path to local file
            folder_path: OneDrive folder path (defaults to OUTPUT_FOLDER)
        
        Returns:
            Upload result dict with 'name', 'web_url', etc. or None if failed
        """
        try:
            target_folder = folder_path if folder_path else CONFIG['OUTPUT_FOLDER']
            upload_result = self.output_client.upload_file(
                local_file_path, 
                target_folder
            )
            
            if upload_result:
                print(f"   ✓ Uploaded to OneDrive: {os.path.basename(local_file_path)}")
                if upload_result.get('web_url'):
                    print(f"     View online: {upload_result['web_url']}")
                return upload_result
            else:
                return None
            
        except Exception as e:
            print(f"   ✗ Upload failed for {local_file_path}: {str(e)}")
            return None
    
    def _move_to_processed(self, file_id: str, filename: str) -> bool:
        """Move a file from input folder to processed folder on OneDrive"""
        try:
            if self.input_client.move_file(file_id, CONFIG['PROCESSED_FOLDER']):
                print(f"   ✓ Moved to {CONFIG['PROCESSED_FOLDER']}: {filename}")
                return True
            return False
        except Exception as e:
            print(f"   ⚠ Failed to move {filename}: {str(e)}")
            return False
    
    def process_file_pair(self, pdf_info: dict, json_info: dict = None) -> bool:
        """Process a PDF file with optional companion JSON - PART 1: Extract and wait"""
        filename = pdf_info['name']
        json_filename = json_info['name'] if json_info else None
        
        print(f"\n{'='*70}")
        print(f"PROCESSING: {filename}")
        if json_filename:
            print(f"WITH JSON: {json_filename}")
        print(f"{'='*70}")
        
        local_pdf_path = None
        local_json_path = None
        local_eml_path = None
        email_metadata = None
        underwriting_subfolder = None
        
        try:
            # Step 1: Download files from OneDrive
            print(f"[1/3] Downloading from OneDrive...")
            local_pdf_path = self.input_client.download_file(
                pdf_info, 
                CONFIG['TEMP_INPUT_DIR']
            )
            print(f"   ✓ Downloaded PDF: {local_pdf_path}")
            
            # Download companion JSON if available
            if json_info:
                local_json_path = os.path.join(CONFIG['TEMP_INPUT_DIR'], json_filename)
                local_json_path = self.input_client.download_file(
                    json_info,
                    CONFIG['TEMP_INPUT_DIR']
                )
                print(f"   ✓ Downloaded JSON: {local_json_path}")
                
                # Load email metadata
                email_metadata = load_email_metadata(local_json_path)
                
                # Note: Subfolder name will be determined after PDF extraction using NEW policy number
                if email_metadata:
                    # Download email as EML if message ID is available
                    message_id = email_metadata.get('id')
                    if message_id:
                        # Extract receiver email (the "to" recipient)
                        receiver_email = get_recipient_email(email_metadata)
                        if not receiver_email:
                            receiver_email = CONFIG['USER_EMAIL']  # Fallback
                        
                        # Use input PDF filename with .eml extension
                        eml_filename = os.path.splitext(filename)[0] + '.eml'
                        local_eml_path = os.path.join(CONFIG['TEMP_INPUT_DIR'], eml_filename)
                        success = self._download_email_as_eml(email_metadata, local_eml_path, receiver_email=receiver_email)
                        if not success:
                            print(f"   ⚠ Continuing without EML file")
                            local_eml_path = None  # Clear path so it won't try to upload
            
            # Step 2: Extract data from PDF
            print(f"[2/3] Extracting data from PDF...")
            success, extracted_data, error = self.orchestrator.extract_data_from_pdf(local_pdf_path)
            if not success:
                print(f"   ✗ Extraction failed: {error}")
                return False
            
            populated_count = len([v for v in extracted_data.values() if v])
            print(f"   ✓ Extracted {populated_count} fields")
            
            # ---- SAVE TO DATABASE IMMEDIATELY AFTER EXTRACTION ----
            if UNIFIED_MODE:
                print(f"\n[WATCHER] 💾 Attempting to save to database...")
                
                # Use robust policy number extraction logic (similar to api_server.py)
                policy_number = (
                    extracted_data.get('Policy Number') or 
                    extracted_data.get('policy_number') or
                    extracted_data.get('Policy ID')
                )
                
                # Check email fields if PDF extraction didn't find it
                if not policy_number and email_metadata:
                    # In main_od, we might need to extract email fields first
                    from email_field_extractor import extract_email_fields
                    try:
                        extracted_email_fields = extract_email_fields(email_metadata)
                        policy_number = extracted_email_fields.get('policy_number')
                        if policy_number:
                            print(f"[WATCHER] ✓ Policy number found in email metadata: {policy_number}")
                    except:
                        pass
                
                if policy_number:
                    try:
                        print(f"[WATCHER] ✓ Policy number found: {policy_number}")
                        result_id = save_underwriting_data(policy_number, extracted_data)
                        if result_id:
                            print(f"[WATCHER] ✓✓✓ Data saved to database (id={result_id})")
                        else:
                            print(f"[WATCHER] ⚠ Database save returned None (SQL issue?)")
                    except Exception as e:
                        print(f"[WATCHER] ✗ Database save failed: {e}")
                else:
                    print(f"[WATCHER] ⚠ No policy number found in PDF or Email - skipping DB save")
            
            # Policy number and folder will be set by frontend
            underwriting_subfolder = None
            
            # Step 3: Create session and check for pending frontend data
            print(f"[3/3] Creating session...")
            
            # Create session if in unified mode
            if UNIFIED_MODE:
                import uuid
                session_id = str(uuid.uuid4())
                session = SessionData(session_id)
                session.pdf_path = local_pdf_path
                session.extracted_data = extracted_data
                session.email_metadata = email_metadata
                session.onedrive_file_id = pdf_info['id']
                session.onedrive_json_id = json_info['id'] if json_info else None
                session.underwriting_subfolder = underwriting_subfolder
                session.local_eml_path = local_eml_path
                session.input_pdf_url = pdf_info.get('web_url')
                
                sessions[session_id] = session
                print(f"   ✓ Session created: {session_id[:8]}...")
                
                # Check if frontend already sent data for this file
                if filename in pending_frontend_data:
                    print(f"\n[WATCHER] 🎯 Found pending frontend data for {filename}")
                    frontend_data = pending_frontend_data[filename]
                    
                    if not frontend_data.get('processed', False):
                        print(f"[WATCHER] 📋 Processing with frontend data immediately...")
                        
                        # Store frontend data in session
                        session.confirmed_email_fields = frontend_data['email_fields']
                        policy_number = frontend_data['email_fields'].get('policy_number')
                        
                        # Handle form PDF if provided
                        if frontend_data.get('form_pdf_base64'):
                            try:
                                import base64
                                pdf_bytes = base64.b64decode(frontend_data['form_pdf_base64'])
                                form_pdf_filename = f"form_{filename.replace('.pdf', '')}.pdf"
                                form_pdf_path = os.path.join(CONFIG['TEMP_OUTPUT_DIR'], form_pdf_filename)
                                with open(form_pdf_path, 'wb') as f:
                                    f.write(pdf_bytes)
                                session.form_pdf_path = form_pdf_path
                                print(f"[WATCHER]    ✓ Form PDF saved")
                            except Exception as e:
                                print(f"[WATCHER]    ⚠ Form PDF failed: {e}")
                        
                        # Trigger report generation immediately
                        try:
                            print(f"[WATCHER]    Policy: {policy_number}")
                            
                            # Compare policy numbers
                            acord_policy = extracted_data.get('Policy Number') or extracted_data.get('policy_number')
                            print(f"[WATCHER]    ACORD Policy: {acord_policy}")
                            print(f"[WATCHER]    Frontend Policy: {policy_number}")
                            
                            if acord_policy != policy_number:
                                print(f"[WATCHER]    ⚠ Policy numbers differ - using frontend value")
                            else:
                                print(f"[WATCHER]    ✓ Policy numbers match")
                            
                            # Update underwriting subfolder with frontend policy
                            if policy_number:
                                session.underwriting_subfolder = f"{CONFIG['UNDERWRITING_FOLDER']}/PN_{policy_number}"
                                print(f"[WATCHER]    ✓ Folder: {session.underwriting_subfolder}")
                            
                            # Continue with processing
                            print(f"[WATCHER] 📊 Starting analysis...")
                            
                            # Prepare DataFrames
                            success, property_df, claims_df, error = self.orchestrator.prepare_dataframes(extracted_data)
                            if not success:
                                print(f"[WATCHER]    ✗ Data preparation failed: {error}")
                                return False
                            
                            # Perform risk analysis
                            success, scored_df, analysis_summary, error = self.orchestrator.perform_risk_analysis(
                                property_df, claims_df
                            )
                            if not success:
                                print(f"[WATCHER]    ✗ Risk analysis failed: {error}")
                                return False
                            
                            client_name = analysis_summary.get('named_insured', 'Property')
                            
                            # Generate PDF report
                            print(f"[WATCHER] 📄 Generating PDF report...")
                            success, pdf_path, error = self.orchestrator.generate_pdf_report(
                                property_df, claims_df, scored_df, client_name, 
                                input_pdf_name=filename, policy_number=policy_number
                            )
                            
                            if not success:
                                print(f"[WATCHER]    ✗ PDF generation failed: {error}")
                                return False
                            
                            session.output_pdf_path = pdf_path
                            print(f"[WATCHER]    ✓ PDF generated: {os.path.basename(pdf_path)}")
                            
                            # ---- SAVE ANALYSIS RESULTS TO POLICY_DB ----
                            # if policy_number:
                            #     save_underwriting_results_to_policy_db(policy_number, analysis_summary, extracted_data)
                            
                            # Generate HTML report
                            try:
                                html_path = self._generate_html_report(
                                    property_df, claims_df, scored_df, client_name, filename
                                )
                                if html_path:
                                    print(f"[WATCHER]    ✓ HTML generated: {os.path.basename(html_path)}")
                            except Exception as e:
                                print(f"[WATCHER]    ⚠ HTML generation failed: {e}")
                                html_path = None
                            
                            # Upload to OneDrive
                            print(f"[WATCHER] ☁ Uploading to OneDrive...")
                            
                            if session.underwriting_subfolder:
                                try:
                                    # Upload input PDF
                                    if local_pdf_path and os.path.exists(local_pdf_path):
                                        input_upload = self.output_client.upload_file(
                                            local_pdf_path, session.underwriting_subfolder
                                        )
                                        if input_upload:
                                            print(f"[WATCHER]    ✓ Input PDF uploaded")
                                    
                                    # Upload output PDF
                                    if pdf_path and os.path.exists(pdf_path):
                                        output_upload = self.output_client.upload_file(
                                            pdf_path, session.underwriting_subfolder
                                        )
                                        if output_upload and output_upload.get('web_url'):
                                            session.output_pdf_url = output_upload['web_url']
                                            print(f"[WATCHER]    ✓ Output PDF uploaded: {session.output_pdf_url}")
                                    
                                    # # Upload HTML
                                    # if html_path and os.path.exists(html_path):
                                    #     self.output_client.upload_file(html_path, session.underwriting_subfolder)
                                    #     print(f"[WATCHER]    ✓ HTML uploaded")
                                    
                                    # Upload EML
                                    if local_eml_path and os.path.exists(local_eml_path):
                                        self.output_client.upload_file(local_eml_path, session.underwriting_subfolder)
                                        print(f"[WATCHER]    ✓ EML uploaded")
                                    
                                    # # Upload form PDF
                                    # if session.form_pdf_path and os.path.exists(session.form_pdf_path):
                                    #     self.output_client.upload_file(session.form_pdf_path, session.underwriting_subfolder)
                                    #     print(f"[WATCHER]    ✓ Form PDF uploaded")
                                    
                                except Exception as e:
                                    print(f"[WATCHER]    ⚠ Upload error: {e}")
                            
                            # Send email
                            if email_metadata:
                                print(f"[WATCHER] 📧 Sending email...")
                                try:
                                    recipient = get_recipient_email(email_metadata)
                                    if recipient:
                                        html_content = ""
                                        if html_path and os.path.exists(html_path):
                                            with open(html_path, 'r', encoding='utf-8') as f:
                                                html_content = f.read()
                                        
                                        if self.email_sender.send_claims_report_email(
                                            to_email=recipient,
                                            email_metadata=email_metadata,
                                            html_report=html_content,
                                            input_pdf_path=local_pdf_path,
                                            output_pdf_path=pdf_path,
                                            report_web_url=session.output_pdf_url,
                                            output_folder_url=None
                                        ):
                                            print(f"[WATCHER]    ✓ Email sent to {recipient}")
                                except Exception as e:
                                    print(f"[WATCHER]    ⚠ Email error: {e}")
                            
                            # Move files to processed
                            print(f"[WATCHER] 🗂 Moving files to processed folder...")
                            try:
                                self._move_to_processed(pdf_info['id'], filename)
                                if json_info:
                                    self._move_to_processed(json_info['id'], json_filename)
                            except Exception as e:
                                print(f"[WATCHER]    ⚠ Move error: {e}")
                            
                            print(f"[WATCHER] ✓ Processing complete with frontend data!")
                            
                            # Mark as processed
                            frontend_data['processed'] = True
                            
                            return True
                            
                        except Exception as e:
                            print(f"[WATCHER]    ✗ Processing failed: {e}")
                            import traceback
                            traceback.print_exc()
                            return False
                    else:
                        print(f"[WATCHER] ℹ Frontend data already processed")
                else:
                    print(f"[WATCHER] ⏳ Waiting for frontend to call /api/process")
                
                print(f"\n{'='*70}\n")
                return True
                
            else:
                # Standalone mode - process immediately without waiting for frontend
                print(f"   ✓ Processing in standalone mode (no frontend integration)")
                # Continue with original standalone processing...
                return True
            
        except Exception as e:
            print(f"\n✗ Processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def watch_and_process(self):
        """Main loop: watch input folder and process new files"""
        print("\n" + "="*70)
        print("WATCHER ACTIVE")
        print("="*70)
        print(f"Monitoring: {CONFIG['INPUT_FOLDER']}")
        print(f"Processing: Files starting with '{CONFIG['FILE_PREFIX']}'")
        print(f"Outputs to: {CONFIG['OUTPUT_FOLDER']}")
        print(f"Processed to: {CONFIG['PROCESSED_FOLDER']}")
        print("\nPress Ctrl+C to stop...\n")
        
        files_found_count = 0
        skipped_count = 0
        is_interactive = sys.stdout.isatty()
        iteration = 0
        processed_file_ids = set()  # Track OneDrive file IDs to prevent re-processing
        
        while True:
            try:
                iteration += 1
                
                # List files in input folder
                files = self.input_client.list_files()
                
                # Handle RESET_CACHE.txt (already implemented)
                reset_file_info = next((f for f in files if f['name'] == 'RESET_CACHE.txt'), None)
                
                if reset_file_info:
                    print("\n[!] REMOTE RESET DETECTED: Clearing processed_cache...")
                    self.processed_cache.clear()
                    processed_file_ids.clear()
                    if UNIFIED_MODE:
                        sessions.clear()
                    print("✓ Cache cleared. Re-scanning all files in folder.\n")
                
                # Categorize files: PDFs and companion JSONs
                pdf_files = []
                json_files = {}
                
                for file_info in files:
                    filename = file_info['name']
                    
                    if self._should_process_file(filename):
                        pdf_files.append(file_info)
                    elif self._is_companion_json(filename):
                        # Map JSON to its PDF name
                        pdf_name = filename[:-5]  # Remove ".json"
                        json_files[pdf_name] = file_info
                    elif filename.lower().endswith(CONFIG['PROCESS_EXTENSION'].lower()) and \
                         not filename.lower().startswith(CONFIG['FILE_PREFIX'].lower()):
                        if filename not in self.processed_cache:
                            skipped_count += 1
                            print(f"⊘ Skipped (no '{CONFIG['FILE_PREFIX']}' prefix): {filename}")
                            self.processed_cache.add(filename)
                
                # Match PDF-JSON pairs - ONLY process when BOTH files exist
                pdf_json_pairs = []
                pending_pdfs = []  # PDFs waiting for companion JSON
                
                for pdf_info in pdf_files:
                    pdf_name = pdf_info['name']
                    pdf_id = pdf_info['id']
                    json_info = json_files.get(pdf_name)
                    
                    # Skip if this PDF file ID was already processed
                    if pdf_id in processed_file_ids:
                        continue
                    
                    # Skip if already has an ACTIVE session in unified mode
                    if UNIFIED_MODE:
                        active_session_exists = any(
                            s.pdf_path and os.path.basename(s.pdf_path) == pdf_name
                            for s in sessions.values()
                        )
                        if active_session_exists:
                            continue
                    
                    if json_info:
                        # Both PDF and JSON exist - ready to process
                        pdf_json_pairs.append({
                            'pdf': pdf_info,
                            'json': json_info,
                            'pdf_name': pdf_name,
                            'json_name': json_info['name']
                        })
                    else:
                        # PDF exists but JSON not yet uploaded - wait
                        pending_pdfs.append(pdf_name)
                
                # Show status
                status_msg = f"[{datetime.now().strftime('%H:%M:%S')}] Check #{iteration}: {len(pdf_files)} PDFs, {len(pdf_json_pairs)} pairs ready"
                
                if pending_pdfs:
                    status_msg += f", {len(pending_pdfs)} waiting for JSON"
                
                if is_interactive:
                    print(status_msg, end='\r')
                elif iteration == 1 or iteration % 60 == 0 or pdf_json_pairs or pending_pdfs:
                    print(status_msg)
                
                # Show pending files (waiting for JSON) - only once per file
                for pdf_name in pending_pdfs:
                    cache_key = f"pending_{pdf_name}"
                    if cache_key not in self.processed_cache:
                        print(f"\n⏳ Waiting for companion JSON: {pdf_name} (needs {pdf_name}.json)")
                        self.processed_cache.add(cache_key)
                
                # Process pairs (only when both PDF and JSON exist)
                for pair in pdf_json_pairs:
                    pdf_name = pair['pdf_name']
                    pdf_id = pair['pdf']['id']
                    files_found_count += 1
                    
                    print(f"\n→ Found matching file #{files_found_count}: {pdf_name}")
                    
                    # Process file pair
                    success = self.process_file_pair(pair['pdf'], pair['json'])
                    
                    # Mark this PDF file ID as processed to prevent re-detection
                    processed_file_ids.add(pdf_id)
                    print(f"   📌 Marked file as processed: {pdf_id[:20]}...")
                
                # Wait before next check
                time.sleep(CONFIG['POLL_INTERVAL'])
                
            except KeyboardInterrupt:
                print("\n\nWatcher stopped by user")
                print(f"\nStatistics:")
                print(f"  Files processed: {files_found_count}")
                print(f"  Files skipped: {skipped_count}")
                break
                
            except Exception as e:
                print(f"\n✗ Watcher error: {str(e)}")
                import traceback
                traceback.print_exc()
                time.sleep(CONFIG['POLL_INTERVAL'])


def main():
    """Main entry point"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='OneDrive Claims Processor')
    parser.add_argument('--port', type=int, help='Port number (ignored, for compatibility)')
    parser.add_argument('--host', type=str, help='Host address (ignored, for compatibility)')
    
    # Parse args but don't use them (just for compatibility)
    args = parser.parse_args()
    
    # Force output to be unbuffered for nohup
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    print(f"Starting OneDrive Claims Processor at {datetime.now()}")
    print(f"Python unbuffered output enabled for logging")
    sys.stdout.flush()
    
    try:
        processor = OneDriveProcessor()
        processor.watch_and_process()
        
    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()