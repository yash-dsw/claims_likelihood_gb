"""
Unified OneDrive Claims Processor
Watches input folder, processes PDFs starting with 'acord_', generates PDF + HTML reports, saves to output folder
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
from email_sender import EmailSender, load_email_metadata, get_recipient_email

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
    
    def _upload_to_onedrive(self, local_file_path: str) -> bool:
        """Upload a file to OneDrive output folder"""
        try:
            upload_result = self.output_client.upload_file(
                local_file_path, 
                CONFIG['OUTPUT_FOLDER']
            )
            
            if upload_result:
                print(f"   ✓ Uploaded to OneDrive: {os.path.basename(local_file_path)}")
                return True
            else:
                return False
            
        except Exception as e:
            print(f"   ✗ Upload failed for {local_file_path}: {str(e)}")
            return False
    
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
        """Process a PDF file with optional companion JSON"""
        filename = pdf_info['name']
        json_filename = json_info['name'] if json_info else None
        
        print(f"\n{'='*70}")
        print(f"PROCESSING: {filename}")
        if json_filename:
            print(f"WITH JSON: {json_filename}")
        print(f"{'='*70}")
        
        local_pdf_path = None
        local_json_path = None
        email_metadata = None
        
        try:
            # Step 1: Download files from OneDrive
            print(f"[1/7] Downloading from OneDrive...")
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
            
            # Step 2-5: Run complete analysis (handled by orchestrator)
            print(f"[2/7] Extracting data from PDF...")
            success, extracted_data, error = self.orchestrator.extract_data_from_pdf(local_pdf_path)
            if not success:
                print(f"   ✗ Extraction failed: {error}")
                return False
            
            print(f"[3/7] Preparing data...")
            success, property_df, claims_df, error = self.orchestrator.prepare_dataframes(extracted_data)
            if not success:
                print(f"   ✗ Data preparation failed: {error}")
                return False
            
            print(f"[4/7] Performing risk analysis...")
            success, scored_df, analysis_summary, error = self.orchestrator.perform_risk_analysis(
                property_df, claims_df
            )
            if not success:
                print(f"   ✗ Risk analysis failed: {error}")
                return False
            
            client_name = analysis_summary.get('named_insured', 'Property')
            
            print(f"[5/7] Generating reports...")
            
            # Generate PDF report
            success, pdf_path, error = self.orchestrator.generate_pdf_report(
                property_df, claims_df, scored_df, client_name, input_pdf_name=filename
            )
            if not success:
                print(f"   ✗ PDF generation failed: {error}")
                pdf_path = None
            
            # Generate HTML report
            html_path = self._generate_html_report(
                property_df, claims_df, scored_df, client_name, input_pdf_name=filename
            )
            
            # Step 6: Upload to OneDrive
            print(f"[6/7] Uploading to OneDrive output folder...")
            
            html_uploaded = False
            if html_path and os.path.exists(html_path):
                html_uploaded = self._upload_to_onedrive(html_path)
            
            if pdf_path and os.path.exists(pdf_path):
                self._upload_to_onedrive(pdf_path)
            
            # Step 7: Send email notification
            if email_metadata and self.email_sender:
                print(f"[7/7] Sending email notification...")
                recipient = get_recipient_email(email_metadata)
                if recipient:
                    print(f"   Sending to: {recipient}")
                    
                    # Read HTML content for email body
                    html_content = ""
                    if html_path and os.path.exists(html_path):
                        with open(html_path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                    
                    if self.email_sender.send_claims_report_email(
                        to_email=recipient,
                        email_metadata=email_metadata,
                        html_report=html_content,
                        input_pdf_path=local_pdf_path,
                        output_pdf_path=pdf_path
                    ):
                        print(f"   ✓ Email sent successfully to {recipient}")
                    else:
                        print(f"   ⚠ Failed to send email to {recipient}")
                else:
                    print(f"   ⚠ No recipient email found in metadata")
            else:
                print(f"[7/7] No email metadata - skipping notification")
            
            # Move processed files on OneDrive to Processed_inputs
            print(f"\n📋 Moving source files to {CONFIG['PROCESSED_FOLDER']}...")
            self._move_to_processed(pdf_info['id'], filename)
            if json_info:
                self._move_to_processed(json_info['id'], json_filename)
            
            print(f"\n✓ PROCESSING COMPLETE")
            print(f"   Risk Score: {analysis_summary['overall_score']:.1f}%")
            print(f"   Risk Level: {analysis_summary['risk_level']}")
            return True
            
        except Exception as e:
            print(f"\n✗ Processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            # Cleanup temporary files
            if local_pdf_path and os.path.exists(local_pdf_path):
                try:
                    os.remove(local_pdf_path)
                    print(f"   ✓ Deleted local PDF")
                except:
                    pass
            if local_json_path and os.path.exists(local_json_path):
                try:
                    os.remove(local_json_path)
                    print(f"   ✓ Deleted local JSON")
                except:
                    pass
            
            # Clear temp_output folder
            try:
                for f in os.listdir(CONFIG['TEMP_OUTPUT_DIR']):
                    file_path = os.path.join(CONFIG['TEMP_OUTPUT_DIR'], f)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                print(f"   ✓ Cleared temp_output folder")
            except Exception as e:
                pass
    
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
                
                # Match PDF-JSON pairs
                pdf_json_pairs = []
                for pdf_info in pdf_files:
                    pdf_name = pdf_info['name']
                    json_info = json_files.get(pdf_name)
                    
                    if json_info:
                        pdf_json_pairs.append({
                            'pdf': pdf_info,
                            'json': json_info,
                            'pdf_name': pdf_name,
                            'json_name': json_info['name']
                        })
                    else:
                        # Process PDF without JSON (no email will be sent)
                        pdf_json_pairs.append({
                            'pdf': pdf_info,
                            'json': None,
                            'pdf_name': pdf_name,
                            'json_name': None
                        })
                
                # Show status
                status_msg = f"[{datetime.now().strftime('%H:%M:%S')}] Check #{iteration}: {len(pdf_files)} PDFs, {len(pdf_json_pairs)} pairs"
                
                if is_interactive:
                    print(status_msg, end='\r')
                elif iteration == 1 or iteration % 60 == 0 or pdf_json_pairs:
                    print(status_msg)
                
                # Process pairs
                for pair in pdf_json_pairs:
                    pdf_name = pair['pdf_name']
                    files_found_count += 1
                    
                    print(f"\n→ Found matching file #{files_found_count}: {pdf_name}")
                    
                    # Process file pair
                    success = self.process_file_pair(pair['pdf'], pair['json'])
                    
                    # Note: We don't add to processed_cache anymore since files are moved to Processed_inputs
                    # If the same filename appears again, it's a new upload and should be processed
                
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