"""
Unified OneDrive Claims Processor
Watches input folder, processes PDFs starting with 'acord_', generates PDF + HTML reports, 
saves to output folder, and sends email notifications.
"""

import os
import sys
import time
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Import your existing modules
from main import ClaimsAnalysisOrchestrator
from onedrive_client_app import OneDriveClientApp

# NEW: Imports for Email and HTML generation
from email_sender import EmailSender, load_email_metadata, get_recipient_email
from html_generator import ClaimsLikelihoodHtmlGenerator

# --- Configuration ---
load_dotenv()

CONFIG = {
    # OneDrive Authentication
    "TENANT_ID": os.getenv("ONEDRIVE_TENANT_ID"),
    "CLIENT_ID": os.getenv("ONEDRIVE_CLIENT_ID"),
    "CLIENT_SECRET": os.getenv("ONEDRIVE_CLIENT_SECRET"),
    "USER_EMAIL": os.getenv("ONEDRIVE_USER_EMAIL"),
    
    # Folder Configuration
    "INPUT_FOLDER": os.getenv("ONEDRIVE_INPUT_FOLDER", "Claims_Input"),
    "OUTPUT_FOLDER": os.getenv("ONEDRIVE_OUTPUT_FOLDER", "Claims_Output"),
    
    # Local temporary directories
    "TEMP_INPUT_DIR": "./temp_input",
    "TEMP_OUTPUT_DIR": "./temp_output",
    
    # Processing
    "POLL_INTERVAL": int(os.getenv("POLL_INTERVAL", "10")),
    "PROCESS_EXTENSION": ".pdf",
    "FILE_PREFIX": os.getenv("FILE_PREFIX", "acord_")  # Only process files starting with this prefix
}


class OneDriveProcessor:
    """Handles complete OneDrive integration with processing pipeline"""
    
    def __init__(self):
        self.input_client = None
        self.output_client = None
        self.email_sender = None  # NEW: Email sender
        self.orchestrator = None
        self.processed_cache = set()
        
        # Create temp directories
        os.makedirs(CONFIG['TEMP_INPUT_DIR'], exist_ok=True)
        os.makedirs(CONFIG['TEMP_OUTPUT_DIR'], exist_ok=True)
        
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize OneDrive clients, Email Sender, and Orchestrator"""
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
        
        # NEW: Initialize Email Sender
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
        print(f"✓ File filter: Files starting with '{CONFIG['FILE_PREFIX']}'")
        print(f"✓ Email Sender initialized for: {CONFIG['USER_EMAIL']}")
        print(f"✓ Poll interval: {CONFIG['POLL_INTERVAL']} seconds")
    
    def _should_process_file(self, filename: str) -> bool:
        """Check if file should be processed based on naming criteria."""
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
    
    def _generate_html_report(self, property_df: pd.DataFrame, 
                             claims_df: pd.DataFrame, 
                             scored_df: pd.DataFrame,
                             client_name: str) -> str:
        """Generate HTML report using the dedicated generator module"""
        try:
            generator = ClaimsLikelihoodHtmlGenerator(
                input_df=property_df,
                claims_df=claims_df if claims_df is not None and len(claims_df) > 0 else pd.DataFrame(),
                output_df=scored_df
            )
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            html_filename = f"Report_{safe_name}_{timestamp}.html"
            html_path = os.path.join(CONFIG['TEMP_OUTPUT_DIR'], html_filename)
            
            generator.generate_html(output_path=html_path)
            
            return html_path
            
        except Exception as e:
            print(f"   ⚠ Warning: HTML generation failed: {str(e)}")
            return None
    
    def _get_or_create_output_folder(self) -> str:
        """Get output folder ID, creating it if it doesn't exist (RESTORED ORIGINAL LOGIC)"""
        try:
            # Method 1: Try direct path access first (most reliable)
            try:
                direct_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/root:/{CONFIG['OUTPUT_FOLDER']}"
                response = requests.get(direct_url, headers=self.output_client._get_headers())
                if response.status_code == 200:
                    folder_info = response.json()
                    if "folder" in folder_info:
                        # print(f"   ✓ Found existing output folder: {CONFIG['OUTPUT_FOLDER']}")
                        return folder_info["id"]
            except:
                pass
            
            # Method 2: List all children in root and find exact match
            try:
                list_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/root/children"
                response = requests.get(list_url, headers=self.output_client._get_headers())
                response.raise_for_status()
                
                items = response.json().get("value", [])
                for item in items:
                    if item.get("name") == CONFIG['OUTPUT_FOLDER'] and "folder" in item:
                        print(f"   ✓ Found existing output folder: {CONFIG['OUTPUT_FOLDER']}")
                        return item["id"]
            except Exception as e:
                print(f"   ⚠ Error listing root folders: {str(e)}")
            
            # Method 3: Try search API
            try:
                search_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/root/search(q='{CONFIG['OUTPUT_FOLDER']}')"
                response = requests.get(search_url, headers=self.output_client._get_headers())
                response.raise_for_status()
                
                items = response.json().get("value", [])
                for item in items:
                    if item.get("name") == CONFIG['OUTPUT_FOLDER'] and "folder" in item:
                        print(f"   ✓ Found existing output folder: {CONFIG['OUTPUT_FOLDER']}")
                        return item["id"]
            except Exception as e:
                print(f"   ⚠ Error searching for folder: {str(e)}")
            
            # Folder doesn't exist, create it
            print(f"   ⚠ Output folder not found, creating: {CONFIG['OUTPUT_FOLDER']}")
            create_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/root/children"
            
            folder_data = {
                "name": CONFIG['OUTPUT_FOLDER'],
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail"
            }
            
            response = requests.post(create_url, headers=self.output_client._get_headers(), json=folder_data)
            
            # If conflict (folder already exists), try to find it again
            if response.status_code == 409:
                print(f"   ⚠ Folder already exists (conflict), searching again...")
                # Try direct access again
                direct_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/root:/{CONFIG['OUTPUT_FOLDER']}"
                response = requests.get(direct_url, headers=self.output_client._get_headers())
                response.raise_for_status()
                folder_info = response.json()
                return folder_info["id"]
            
            response.raise_for_status()
            folder_info = response.json()
            print(f"   ✓ Created output folder: {CONFIG['OUTPUT_FOLDER']}")
            return folder_info["id"]
            
        except Exception as e:
            raise Exception(f"Failed to get/create output folder '{CONFIG['OUTPUT_FOLDER']}': {str(e)}")
    
    def _upload_to_onedrive(self, local_file_path: str) -> bool:
        """Upload a file to OneDrive output folder"""
        try:
            filename = os.path.basename(local_file_path)
            
            # Get or create output folder
            folder_id = self._get_or_create_output_folder()
            
            # Upload file
            upload_url = f"https://graph.microsoft.com/v1.0/users/{CONFIG['USER_EMAIL']}/drive/items/{folder_id}:/{filename}:/content"
            
            with open(local_file_path, 'rb') as f:
                file_content = f.read()
            
            headers = self.output_client._get_headers()
            headers['Content-Type'] = 'application/octet-stream'
            
            response = requests.put(upload_url, headers=headers, data=file_content)
            response.raise_for_status()
            
            print(f"   ✓ Uploaded to OneDrive: {filename}")
            return True
            
        except Exception as e:
            print(f"   ✗ Upload failed for {local_file_path}: {str(e)}")
            return False
    
    def process_file(self, file_info: dict) -> bool:
        """Process a single file through the complete pipeline"""
        filename = file_info['name']
        local_json_path = None
        local_pdf_path = None
        
        print(f"\n{'='*70}")
        print(f"PROCESSING: {filename}")
        print(f"{'='*70}")
        
        try:
            # Step 1: Download from OneDrive
            print(f"[1/7] Downloading PDF from OneDrive...")
            local_pdf_path = self.input_client.download_file(
                file_info, 
                CONFIG['TEMP_INPUT_DIR']
            )
            print(f"   ✓ Downloaded: {local_pdf_path}")
            
            # Step 2: Extract data
            print(f"[2/7] Extracting data from PDF...")
            success, extracted_data, error = self.orchestrator.extract_data_from_pdf(local_pdf_path)
            if not success:
                print(f"   ✗ Extraction failed: {error}")
                return False
            
            # Step 3: Prepare data
            print(f"[3/7] Preparing data...")
            success, property_df, claims_df, error = self.orchestrator.prepare_dataframes(extracted_data)
            if not success:
                print(f"   ✗ Data preparation failed: {error}")
                return False
            
            # Step 4: Risk analysis
            print(f"[4/7] Performing risk analysis...")
            success, scored_df, analysis_summary, error = self.orchestrator.perform_risk_analysis(
                property_df, claims_df
            )
            if not success:
                print(f"   ✗ Risk analysis failed: {error}")
                return False
            
            client_name = analysis_summary.get('named_insured', 'Property')
            
            # Step 5: Generate reports
            print(f"[5/7] Generating reports...")
            
            # Generate PDF report
            success, pdf_path, error = self.orchestrator.generate_pdf_report(
                property_df, claims_df, scored_df, client_name
            )
            if not success:
                print(f"   ✗ PDF generation failed: {error}")
                return False
            
            # Generate HTML report (Required for Email Body)
            html_path = self._generate_html_report(
                property_df, claims_df, scored_df, client_name
            )
            
            # Step 6: Upload to OneDrive
            print(f"[6/7] Uploading to OneDrive output folder...")
            html_uploaded = False
            
            if html_path and os.path.exists(html_path):
                html_uploaded = self._upload_to_onedrive(html_path)
            
            if html_uploaded:
                print(f"   ✓ HTML report uploaded")
            else:
                print(f"   ⚠ HTML upload failed or file not generated")

            # NEW: Step 7: Send Email Notification
            print(f"[7/7] Sending email notification...")
            email_sent = False
            
            # 7a. Look for companion JSON file (e.g., acord_form.pdf.json)
            json_filename = filename + ".json"
            files_in_drive = self.input_client.list_files()
            json_file_info = next((f for f in files_in_drive if f['name'] == json_filename), None)
            
            if json_file_info:
                print(f"   ✓ Found companion metadata: {json_filename}")
                local_json_path = self.input_client.download_file(json_file_info, CONFIG['TEMP_INPUT_DIR'])
                
                # 7b. Load metadata
                email_metadata = load_email_metadata(local_json_path)
                recipient = get_recipient_email(email_metadata)
                
                if recipient:
                    # 7c. Read HTML content for email body
                    html_content = ""
                    if html_path and os.path.exists(html_path):
                        with open(html_path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                    else:
                        html_content = "<p>Analysis completed. Please check the output folder for details.</p>"
                    
                    # 7d. Send Email
                    email_sent = self.email_sender.send_claim_likelihood_report_email(
                        to_email=recipient,
                        email_metadata=email_metadata,
                        html_report=html_content
                    )
                    
                    if email_sent:
                        print(f"   ✓ Email sent to: {recipient}")
                    else:
                        print(f"   ✗ Email sending failed")
                else:
                    print(f"   ⚠ No recipient found in metadata")
            else:
                print(f"   ⚠ No companion JSON file found (skipping email)")

            print(f"\n✓ PROCESSING COMPLETE")
            print(f"   Risk Score: {analysis_summary['overall_score']:.1f}%")
            return True
            
        except Exception as e:
            print(f"\n✗ Processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            # Cleanup temporary files
            for fpath in [local_pdf_path, local_json_path]:
                if fpath and os.path.exists(fpath):
                    try:
                        os.remove(fpath)
                    except:
                        pass
    
    def watch_and_process(self):
        """Main loop: watch input folder and process new files"""
        print("\n" + "="*70)
        print("WATCHER ACTIVE")
        print("="*70)
        print(f"Monitoring: {CONFIG['INPUT_FOLDER']}")
        print(f"Processing: Files starting with '{CONFIG['FILE_PREFIX']}'")
        print(f"Outputs to: {CONFIG['OUTPUT_FOLDER']}")
        print("\nPress Ctrl+C to stop...\n")
        
        files_found_count = 0
        skipped_count = 0
        
        while True:
            try:
                # List files in input folder
                files = self.input_client.list_files()
                
                # Check for remote reset trigger
                reset_file_info = next((f for f in files if f['name'] == 'RESET_CACHE.txt'), None)
                if reset_file_info:
                    print("\n[!] REMOTE RESET DETECTED: Clearing processed_cache...")
                    self.processed_cache.clear()
                    print("✓ Cache cleared. Re-scanning all files in folder.\n")

                # Filter files that should be processed
                processable_files = []
                for file_info in files:
                    filename = file_info['name']
                    
                    if self._should_process_file(filename):
                        processable_files.append(file_info)
                    elif filename.lower().endswith(CONFIG['PROCESS_EXTENSION'].lower()) and \
                         not filename.lower().startswith(CONFIG['FILE_PREFIX'].lower()):
                        # File is PDF but doesn't match prefix
                        if filename not in self.processed_cache:
                            skipped_count += 1
                            print(f"⊘ Skipped (no '{CONFIG['FILE_PREFIX']}' prefix): {filename}")
                            self.processed_cache.add(filename)
                
                # Process filtered files
                for file_info in processable_files:
                    filename = file_info['name']
                    files_found_count += 1
                    
                    print(f"\n→ Found matching file #{files_found_count}: {filename}")
                    
                    # Process file
                    success = self.process_file(file_info)
                    
                    # Mark as processed regardless of success
                    self.processed_cache.add(filename)
                
                # Wait before next check
                time.sleep(CONFIG['POLL_INTERVAL'])
                
            except KeyboardInterrupt:
                print("\n\nWatcher stopped by user")
                break
                
            except Exception as e:
                print(f"\n✗ Watcher error: {str(e)}")
                time.sleep(CONFIG['POLL_INTERVAL'])


def main():
    """Main entry point"""
    # Parse command line arguments (compatibility)
    import argparse
    parser = argparse.ArgumentParser(description='OneDrive Claims Processor')
    parser.add_argument('--port', type=int, help='Port number')
    parser.add_argument('--host', type=str, help='Host address')
    args = parser.parse_args()
    
    # Force output to be unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    print(f"Starting OneDrive Claims Processor at {datetime.now()}")
    
    try:
        processor = OneDriveProcessor()
        processor.watch_and_process()
        
    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()