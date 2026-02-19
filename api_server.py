"""
Flask API Server for Claims Likelihood Analysis
Splits the workflow into two parts:
1. Extract details from PDF and return them
2. Receive updated details, compare, and continue processing
"""

import os
import json
import uuid
import base64
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from extract_pdf_fields import extract_pdf_form_fields
from main import ClaimsAnalysisOrchestrator
from onedrive_client_app import OneDriveClientApp
from email_field_extractor import extract_email_fields

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Enable CORS with explicit permissions for ngrok and local development
CORS(app, 
     resources={r"/*": {"origins": "*"}},
     allow_headers=["Content-Type", "Authorization", "ngrok-skip-browser-warning", "Accept"],
     methods=["GET", "POST", "OPTIONS", "PUT", "DELETE"],
     expose_headers=["Content-Type", "Authorization"],
     supports_credentials=False,
     max_age=3600)

@app.before_request
def handle_preflight():
    """Handle preflight OPTIONS requests"""
    if request.method == "OPTIONS":
        response = jsonify({'status': 'ok'})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, DELETE'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, ngrok-skip-browser-warning, Accept'
        response.headers['Access-Control-Max-Age'] = '3600'
        return response, 200

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, DELETE'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, ngrok-skip-browser-warning, Accept'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Type, Authorization'
    return response


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS')
}


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def test_db_connection():
    """Test database connection"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def get_all_policies():
    """Retrieve all policies from database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM policy_db ORDER BY created_at DESC")
                policies = cur.fetchall()
                
                result = []
                for p in policies:
                    p_dict = dict(p)
                    for k, v in p_dict.items():
                        if hasattr(v, 'isoformat'):
                            p_dict[k] = v.isoformat()
                    result.append(p_dict)
                return result
    except Exception as e:
        print(f"❌ Error fetching policies: {e}")
        return []


def get_policy_by_id(policy_id):
    """Retrieve policy information by policy_id"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM policy_db WHERE policy_id = %s", (policy_id,))
                policy = cur.fetchone()
                if policy:
                    p_dict = dict(policy)
                    for k, v in p_dict.items():
                        if hasattr(v, 'isoformat'):
                            p_dict[k] = v.isoformat()
                    return p_dict
                return None
    except Exception as e:
        print(f"❌ Error fetching policy {policy_id}: {e}")
        return None

def save_underwriting_data(policy_id, extracted_data):
    """Save or update all 30 extracted ACORD fields to underwriting_data table (UPSERT)"""
    print(f"\n{'='*70}")
    print(f"[DB SAVE] >>> FUNCTION CALLED: save_underwriting_data()")
    print(f"{'='*70}")
    print(f"[DB SAVE] Policy ID argument: '{policy_id}' (type: {type(policy_id).__name__})")
    print(f"[DB SAVE] Extracted data type: {type(extracted_data).__name__}")
    print(f"[DB SAVE] Extracted data keys ({len(extracted_data)} total): {list(extracted_data.keys())}")
    print(f"[DB SAVE] Sample of extracted data (first 8 items):")
    for i, (key, value) in enumerate(list(extracted_data.items())[:8]):
        value_preview = str(value)[:80] + '...' if len(str(value)) > 80 else str(value)
        print(f"  {i+1}. {key} = {value_preview}")
    
    try:
        print(f"\n[DB SAVE] Step 1: Processing Loss History...")
        # Build loss_history as JSON
        loss_history_raw = extracted_data.get('Loss History', [])
        print(f"[DB SAVE] Loss History raw type: {type(loss_history_raw).__name__}")
        print(f"[DB SAVE] Loss History raw value: {loss_history_raw}")
        loss_history_json = json.dumps(loss_history_raw) if loss_history_raw else None
        print(f"[DB SAVE] Loss History JSON: {loss_history_json[:100] if loss_history_json else None}...")

        print(f"\n[DB SAVE] Step 2: Getting database connection...")
        print(f"[DB SAVE] DB Config: host={DB_CONFIG['host']}, port={DB_CONFIG['port']}, db={DB_CONFIG['database']}")
        with get_db_connection() as conn:
            print(f"[DB SAVE] ✓ Database connection established")
            with conn.cursor() as cur:
                print(f"\n[DB SAVE] Step 3: Checking if record exists...")
                print(f"[DB SAVE] Executing: SELECT id FROM underwriting_data WHERE policy_id = '{policy_id}'")
                # Check if record exists for this policy_id
                cur.execute("""
                    SELECT id FROM underwriting_data WHERE policy_id = %s
                """, (policy_id,))
                existing = cur.fetchone()
                print(f"[DB SAVE] Query result: {existing}")
                
                if existing:
                    # UPDATE existing record
                    print(f"[DB SAVE] Step 4: UPDATING existing record (id={existing[0]})...")
                    print(f"[DB SAVE] Building UPDATE query...")
                    cur.execute("""
                        UPDATE underwriting_data SET
                            named_insured = %s, mailing_address = %s, city = %s, state = %s,
                            naics_code = %s, legal_entity_type = %s, fein = %s, years_in_business = %s,
                            business_description = %s, prior_carrier = %s,
                            loss_history_count = %s, loss_history_total = %s, loss_history = %s,
                            premises_number = %s, building_number = %s, street_address = %s,
                            subject_of_insurance = %s, coverage_limit = %s,
                            construction_type = %s, year_built = %s, total_area_sqft = %s,
                            num_stories = %s, sprinklered_pct = %s,
                            improvements_wiring = %s, improvements_roofing = %s, improvements_plumbing = %s,
                            burglar_alarm_type = %s, fire_protection_class = %s,
                            distance_fire_hydrant = %s, distance_fire_station = %s
                        WHERE policy_id = %s
                        RETURNING id
                    """, (
                        extracted_data.get('Named Insured', ''),
                        extracted_data.get('Mailing Address', ''),
                        extracted_data.get('City', ''),
                        extracted_data.get('State', ''),
                        extracted_data.get('NAICS Code', ''),
                        extracted_data.get('Legal Entity Type', ''),
                        extracted_data.get('FEIN', ''),
                        extracted_data.get('Years in Business', ''),
                        extracted_data.get('Business Description', ''),
                        extracted_data.get('Prior Carrier', ''),
                        str(extracted_data.get('Loss History - Count', '')),
                        str(extracted_data.get('Loss History - Total Amount', '')),
                        loss_history_json,
                        extracted_data.get('Premises #', ''),
                        extracted_data.get('Bldg #', ''),
                        extracted_data.get('Street Address', ''),
                        extracted_data.get('Subject of Insurance', ''),
                        extracted_data.get('Coverage Limit', ''),
                        extracted_data.get('Construction Type', ''),
                        extracted_data.get('Year Built', ''),
                        extracted_data.get('Total Area (Sq Ft)', ''),
                        extracted_data.get('# of Stories', ''),
                        extracted_data.get('Sprinklered %', ''),
                        extracted_data.get('Building Improvements - Wiring', ''),
                        extracted_data.get('Building Improvements - Roofing', ''),
                        extracted_data.get('Building Improvements - Plumbing', ''),
                        extracted_data.get('Burglar Alarm Type', ''),
                        extracted_data.get('Fire Protection Class', ''),
                        extracted_data.get('Distance to Fire Hydrant', ''),
                        extracted_data.get('Distance to Fire Station', ''),
                        policy_id
                    ))
                    result = cur.fetchone()
                    print(f"[DB SAVE] ✓ UPDATE executed successfully")
                    print(f"[DB SAVE] Result: {result}")
                    print(f"[DB SAVE] ✓✓✓ Underwriting data UPDATED for policy {policy_id} (id={result[0]})")
                    print(f"{'='*70}\n")
                    return result[0] if result else None
                else:
                    # INSERT new record
                    print(f"[DB SAVE] Step 4: INSERTING new record...")
                    print(f"[DB SAVE] Building INSERT query with {31} fields...")
                    cur.execute("""
                        INSERT INTO underwriting_data (
                            policy_id,
                            named_insured, mailing_address, city, state,
                            naics_code, legal_entity_type, fein, years_in_business,
                            business_description, prior_carrier,
                            loss_history_count, loss_history_total, loss_history,
                            premises_number, building_number, street_address,
                            subject_of_insurance, coverage_limit,
                            construction_type, year_built, total_area_sqft,
                            num_stories, sprinklered_pct,
                            improvements_wiring, improvements_roofing, improvements_plumbing,
                            burglar_alarm_type, fire_protection_class,
                            distance_fire_hydrant, distance_fire_station
                        ) VALUES (
                            %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s
                        )
                        RETURNING id
                    """, (
                        policy_id,
                        extracted_data.get('Named Insured', ''),
                        extracted_data.get('Mailing Address', ''),
                        extracted_data.get('City', ''),
                        extracted_data.get('State', ''),
                        extracted_data.get('NAICS Code', ''),
                        extracted_data.get('Legal Entity Type', ''),
                        extracted_data.get('FEIN', ''),
                        extracted_data.get('Years in Business', ''),
                        extracted_data.get('Business Description', ''),
                        extracted_data.get('Prior Carrier', ''),
                        str(extracted_data.get('Loss History - Count', '')),
                        str(extracted_data.get('Loss History - Total Amount', '')),
                        loss_history_json,
                        extracted_data.get('Premises #', ''),
                        extracted_data.get('Bldg #', ''),
                        extracted_data.get('Street Address', ''),
                        extracted_data.get('Subject of Insurance', ''),
                        extracted_data.get('Coverage Limit', ''),
                        extracted_data.get('Construction Type', ''),
                        extracted_data.get('Year Built', ''),
                        extracted_data.get('Total Area (Sq Ft)', ''),
                        extracted_data.get('# of Stories', ''),
                        extracted_data.get('Sprinklered %', ''),
                        extracted_data.get('Building Improvements - Wiring', ''),
                        extracted_data.get('Building Improvements - Roofing', ''),
                        extracted_data.get('Building Improvements - Plumbing', ''),
                        extracted_data.get('Burglar Alarm Type', ''),
                        extracted_data.get('Fire Protection Class', ''),
                        extracted_data.get('Distance to Fire Hydrant', ''),
                        extracted_data.get('Distance to Fire Station', '')
                    ))
                    result = cur.fetchone()
                    print(f"[DB SAVE] ✓ INSERT executed successfully")
                    print(f"[DB SAVE] Result: {result}")
                    print(f"[DB SAVE] ✓✓✓ Underwriting data INSERTED for policy {policy_id} (id={result[0]})")
                    print(f"{'='*70}\n")
                    return result[0] if result else None
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"[DB SAVE] ✗✗✗ EXCEPTION CAUGHT in save_underwriting_data()")
        print(f"{'='*70}")
        print(f"[DB SAVE] Error type: {type(e).__name__}")
        print(f"[DB SAVE] Error message: {str(e)}")
        print(f"[DB SAVE] Policy ID that failed: '{policy_id}'")
        print(f"[DB SAVE] Full traceback:")
        import traceback
        traceback.print_exc()
        print(f"{'='*70}\n")
        return None



def save_policy_to_db(policy_data):
    """Insert or update policy in database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get columns for INSERT statement
                cols = list(policy_data.keys())
                vals = [policy_data[c] for c in cols]
                
                placeholders = ", ".join(["%s"] * len(cols))
                update_placeholders = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != 'policy_id'])
                
                query = f"""
                    INSERT INTO policy_db ({", ".join(cols)})
                    VALUES ({placeholders})
                    ON CONFLICT (policy_id) DO UPDATE SET
                    {update_placeholders}
                    RETURNING policy_id
                """
                
                cur.execute(query, vals)
                result = cur.fetchone()
                return result[0] if result else None
    except Exception as e:
        print(f"❌ Error saving policy: {e}")
        raise e


def save_underwriting_results_to_policy_db(policy_id, analysis_summary, original_data):
    """Save detailed risk analysis results to policy_db table"""
    print(f"\n[DB] Saving analysis results to policy_db for {policy_id}...")
    try:
        # Extract component scores from analysis_summary
        # Note: RiskScores object attributes are available in analysis_summary if passed correctly
        property_risk = analysis_summary.get('property_risk', 0)
        claims_risk = analysis_summary.get('claims_risk', 0)
        geographic_risk = analysis_summary.get('geographic_risk', 0)
        protection_risk = analysis_summary.get('protection_risk', 0)
        
        # Build update record
        update_data = {
            'claims_likelihood_score': analysis_summary.get('overall_score'),
            'claims_likelihood_level': analysis_summary.get('risk_level'),
            'underwriting_recommendation': analysis_summary.get('recommendation'),
            'underwriting_notes': " | ".join(analysis_summary.get('top_factors', [])),
            'property_risk_score': property_risk,
            'claims_history_risk_score': claims_risk,
            'geographic_risk_score': geographic_risk,
            'protection_risk_score': protection_risk,
            # Specific details from original extracted data (passed via property_df/final_data)
            'roof_condition': original_data.get('Verified Roof Condition') or original_data.get('Roof Condition', 'N/A'),
            'wildfire_risk': original_data.get('Wildfire Risk Score') or original_data.get('Wildfire Risk', 0),
            'fema_flood_zone': original_data.get('FEMA Flood Zone', 'X'),
            'earthquake_zone': original_data.get('Earthquake Zone', 'Zone 0'),
            'crime_score': original_data.get('Crime Score', 0),
        }
        
        # Handle claims details mapping
        # Try to find claim count and total loss from claims_factors strings if needed, 
        # but better to pass them if possible. For now we use extracted ACORD values.
        update_data['claim_count'] = int(original_data.get('Loss History - Count') or 0)
        update_data['total_loss_amount'] = float(original_data.get('Loss History - Total Amount') or 0.0)
        update_data['loss_types'] = original_data.get('Loss History - Type', 'N/A')

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cols = list(update_data.keys())
                vals = [update_data[c] for c in cols]
                
                set_clause = ", ".join([f"{c} = %s" for c in cols])
                query = f"UPDATE policy_db SET {set_clause} WHERE policy_id = %s"
                
                cur.execute(query, vals + [policy_id])
                print(f"✓ Analysis results saved to policy_db for {policy_id}")
                return True
    except Exception as e:
        print(f"❌ Error saving analysis results to policy_db: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_underwriting_data_by_policy(policy_id):
    """Retrieve underwriting data for a specific policy"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM underwriting_data WHERE policy_id = %s", (policy_id,))
                data = cur.fetchone()
                if data:
                    uw_dict = dict(data)
                    # Convert datetime objects to ISO format
                    for k, v in uw_dict.items():
                        if hasattr(v, 'isoformat'):
                            uw_dict[k] = v.isoformat()
                    return uw_dict
                return None
    except Exception as e:
        print(f"❌ Error fetching underwriting data for policy {policy_id}: {e}")
        return None


def get_policy_input_attachment(policy_id):
    """Retrieve the original input files (PDF, report PDF, EML) from the policy's OneDrive folder"""
    try:
        # Predictable folder path
        folder_path = f"Underwriting/PN_{policy_id}"
        
        # Use Output folder client because Underwriting is usually there
        client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
        print(f"[DEBUG] Fetching SharePoint folder for PN_{policy_id}...")
        print(f"[DEBUG] Client email: {client.user_email}")
        
        # Get folder info to get ID
        folder_url = f"https://graph.microsoft.com/v1.0/users/{client.user_email}/drive/root:/{folder_path}"
        headers = client._get_headers()
        
        response = requests.get(folder_url, headers=headers)
        if response.status_code != 200:
            print(f"   ⚠ Folder not found for policy {policy_id}: {folder_path} (Status: {response.status_code})")
            return None
        print(f"   ✓ Folder found for policy {policy_id}")
            
        folder_data = response.json()
        folder_id = folder_data.get('id')
        folder_web_url = folder_data.get('webUrl')
        
        # List children
        children_url = f"https://graph.microsoft.com/v1.0/users/{client.user_email}/drive/items/{folder_id}/children"
        response = requests.get(children_url, headers=headers)
        response.raise_for_status()
        
        children = response.json().get('value', [])
        
        print(f"   [DEBUG] Found {len(children)} items in folder")
        
        # Find all relevant files: .pdf, .docx, .eml, .xlsx, .msg, .jpg, .png
        files = []
        allowed_extensions = ('.pdf', '.docx', '.doc', '.eml')
        
        for item in children:
            name = item.get('name', '').lower()
            actual_name = item.get('name')
            print(f"   [DEBUG] Checking file: {actual_name}")
            
            # Check if it's a file (has 'file' property or size) and matches allowed extensions
            # We no longer enforce 'acord_' prefix for all files
            if name.endswith(allowed_extensions):
                print(f"   [DEBUG] ✓ Match! Adding: {actual_name}")
                files.append({
                    'name': actual_name,
                    'web_url': item.get('webUrl'),
                    'id': item.get('id')
                })
            else:
                print(f"   [DEBUG] ✗ No match: {actual_name}")
        
        print(f"   [DEBUG] Total files matched: {len(files)}")
        for f in files:
            print(f"   [DEBUG]   - {f['name']}")
        
        if files:
            # Return all files found
            result = {
                'files': files,
                'folder_url': folder_web_url,
                # For backward compatibility, also return the first file as 'attachment'
                'attachment': files[0] if files else None
            }
            print(f"   [DEBUG] Returning {len(files)} files")
            return result
                
        # If no files found, still return the folder URL if folder exists
        print(f"   [DEBUG] No files found, returning folder URL only")
        return {
            'folder_url': folder_web_url
        }
    except Exception as e:
        print(f"❌ Error fetching input attachment for policy {policy_id}: {e}")
        return None


# Configuration
CONFIG = {
    "UPLOAD_FOLDER": "./temp_input",
    "OUTPUT_FOLDER": "./temp_output",
    "SESSION_TIMEOUT_MINUTES": 30,
    "TENANT_ID": os.getenv("ONEDRIVE_TENANT_ID"),
    "CLIENT_ID": os.getenv("ONEDRIVE_CLIENT_ID"),
    "CLIENT_SECRET": os.getenv("ONEDRIVE_CLIENT_SECRET"),
    "USER_EMAIL": os.getenv("ONEDRIVE_USER_EMAIL"),
    "INPUT_FOLDER": os.getenv("ONEDRIVE_FOLDER_NAME", "Input_attachments"),
    "OUTPUT_FOLDER_OD": os.getenv("ONEDRIVE_OUTPUT_FOLDER", "Output_attachments"),
}

# Create folders
os.makedirs(CONFIG['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(CONFIG['OUTPUT_FOLDER'], exist_ok=True)

# In-memory session storage (for production, use Redis or database)
sessions = {}

# Cached orchestrator instance (to avoid re-initialization overhead)
_orchestrator_cache = None

# Pending files queue - files detected by watcher waiting for processing
pending_files = {}

# Store frontend data that arrives before watcher completes (keyed by filename)
pending_frontend_data = {}


class SessionData:
    """Store session data for a processing request"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = datetime.now()
        self.pdf_path = None
        self.output_pdf_path = None  # Local path to the generated output PDF
        self.output_pdf_url = None  # OneDrive web URL to the output PDF
        self.extracted_data = None
        self.property_df = None
        self.claims_df = None
        self.updated_data = None
        self.onedrive_file_id = None
        self.onedrive_json_id = None
        self.onedrive_docx_id = None  # Loss run Word document file ID
        self.onedrive_docx_json_id = None  # Loss run JSON companion file ID
        self.email_metadata = None
        self.extracted_email_fields = None
        self.confirmed_email_fields = None
        self.underwriting_subfolder = None
        self.local_eml_path = None
        self.form_pdf_path = None  # Path to uploaded form PDF from frontend
        self.input_pdf_url = None  # OneDrive web URL to the original input PDF
        self.extra_attachment_paths = []  # Paths to extra attachments (loss run DOCX etc.)
        # Async processing state
        self.processing_status = 'idle'  # idle | processing | done | error
        self.processing_error = None
        self.analysis_summary = None
        self.sse_queue = __import__('queue').Queue()  # SSE push channel
    
    def is_expired(self) -> bool:
        """Check if session has expired"""
        timeout = timedelta(minutes=CONFIG['SESSION_TIMEOUT_MINUTES'])
        return datetime.now() - self.created_at > timeout
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'session_id': self.session_id,
            'created_at': self.created_at.isoformat(),
            'has_pdf': self.pdf_path is not None,
            'has_output_pdf': self.output_pdf_path is not None,
            'has_output_pdf_url': self.output_pdf_url is not None,
            'has_extracted_data': self.extracted_data is not None,
            'has_updated_data': self.updated_data is not None,
            'has_email_fields': self.extracted_email_fields is not None,
            'has_confirmed_email_fields': self.confirmed_email_fields is not None,
            'has_input_pdf_url': self.input_pdf_url is not None,
            'processing_status': self.processing_status,
            'processing_error': self.processing_error,
        }


def cleanup_expired_sessions():
    """Remove expired sessions and old pending frontend data"""
    # Clean up expired sessions
    expired = [sid for sid, session in sessions.items() if session.is_expired()]
    for sid in expired:
        sessions.pop(sid, None)
    
    # Clean up old pending frontend data (older than 30 minutes)
    timeout = timedelta(minutes=CONFIG['SESSION_TIMEOUT_MINUTES'])
    expired_pending = []
    for filename, data in pending_frontend_data.items():
        if 'received_at' in data:
            received_time = datetime.fromisoformat(data['received_at'])
            if datetime.now() - received_time > timeout:
                expired_pending.append(filename)
    
    for filename in expired_pending:
        pending_frontend_data.pop(filename, None)


def get_orchestrator() -> ClaimsAnalysisOrchestrator:
    """Get cached orchestrator instance or create new one"""
    global _orchestrator_cache
    if _orchestrator_cache is None:
        _orchestrator_cache = ClaimsAnalysisOrchestrator(CONFIG['OUTPUT_FOLDER'])
    return _orchestrator_cache


def _save_extracted_data_to_db(extracted_data: dict, extracted_email_fields: Optional[dict] = None) -> None:
    """Persist extracted ACORD data when policy number is available."""
    policy_number = (
        extracted_data.get('Policy Number')
        or extracted_data.get('policy_number')
        or extracted_data.get('Policy ID')
        or (extracted_email_fields.get('policy_number') if extracted_email_fields else None)
    )
    if not policy_number:
        print("[DATABASE] No policy number found; skipping underwriting_data save")
        return
    try:
        save_underwriting_data(policy_number, extracted_data)
        print(f"[DATABASE] Saved extracted data for policy {policy_number}")
    except Exception as e:
        print(f"[DATABASE] Save failed for policy {policy_number}: {e}")


def get_onedrive_client(folder_name: str) -> OneDriveClientApp:
    """Create OneDrive client instance"""
    return OneDriveClientApp(
        tenant_id=CONFIG['TENANT_ID'],
        client_id=CONFIG['CLIENT_ID'],
        client_secret=CONFIG['CLIENT_SECRET'],
        user_email=CONFIG['USER_EMAIL'],
        folder_name=folder_name
    )


def _download_email_as_eml(email_metadata: dict, output_path: str) -> bool:
    """
    Download email as EML file from Microsoft Graph API.
    Uses itemId (from Outlook add-in) to fetch the raw MIME message.
    Falls back to internetMessageId search if itemId is not available.
    """
    try:
        from email_sender import EmailSender
        email_sender = EmailSender(
            tenant_id=CONFIG['TENANT_ID'],
            client_id=CONFIG['CLIENT_ID'],
            client_secret=CONFIG['CLIENT_SECRET'],
            user_email=CONFIG['USER_EMAIL']
        )
        headers = email_sender._get_headers()

        user_email = CONFIG['USER_EMAIL']
        item_id = email_metadata.get('id') or email_metadata.get('itemId')
        internet_message_id = email_metadata.get('internetMessageId')
        receiver_email = email_metadata.get('userEmail') or user_email

        # Strategy 1: Direct download using itemId from the receiver's mailbox
        if item_id and receiver_email:
            print(f"   [EML] Trying direct download via itemId from {receiver_email}")
            url = f"https://graph.microsoft.com/v1.0/users/{receiver_email}/messages/{item_id}/$value"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(resp.content)
                print(f"   [EML] ✓ Downloaded via itemId: {os.path.basename(output_path)}")
                return True
            else:
                print(f"   [EML] itemId download failed: {resp.status_code}")

        # Strategy 2: Search by internetMessageId
        if internet_message_id and receiver_email:
            print(f"   [EML] Searching by internetMessageId in {receiver_email}")
            search_url = f"https://graph.microsoft.com/v1.0/users/{receiver_email}/messages"
            params = {"$filter": f"internetMessageId eq '{internet_message_id}'", "$select": "id"}
            search_resp = requests.get(search_url, headers=headers, params=params)
            if search_resp.status_code == 200:
                values = search_resp.json().get('value', [])
                if values:
                    found_id = values[0]['id']
                    url = f"https://graph.microsoft.com/v1.0/users/{receiver_email}/messages/{found_id}/$value"
                    resp = requests.get(url, headers=headers)
                    if resp.status_code == 200:
                        with open(output_path, 'wb') as f:
                            f.write(resp.content)
                        print(f"   [EML] ✓ Downloaded via internetMessageId: {os.path.basename(output_path)}")
                        return True

        print(f"   [EML] ⚠ Could not download EML (no valid ID found)")
        return False
    except Exception as e:
        print(f"   [EML] ⚠ Error: {e}")
        return False


# Error handlers to ensure all responses are JSON
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors with JSON response"""
    return jsonify({
        'error': 'Not Found',
        'message': 'The requested resource was not found',
        'status': 404
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 Method Not Allowed with JSON response"""
    return jsonify({
        'error': 'Method Not Allowed',
        'message': 'The method is not allowed for the requested URL',
        'status': 405
    }), 405


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors with JSON response"""
    return jsonify({
        'error': 'Internal Server Error',
        'message': 'An internal server error occurred',
        'status': 500
    }), 500


@app.errorhandler(Exception)
def handle_exception(error):
    """Handle all uncaught exceptions with JSON response"""
    return jsonify({
        'error': 'Internal Server Error',
        'message': str(error),
        'status': 500
    }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_sessions': len(sessions)
    })


@app.route('/debug-cors', methods=['GET', 'POST', 'OPTIONS'])
def debug_cors():
    """Endpoint to verify CORS headers from browser"""
    return jsonify({
        'status': 'ok',
        'message': 'CORS is working correctly',
        'method': request.method,
        'headers_received': dict(request.headers),
        'origin': request.headers.get('Origin', 'No Origin header')
    })


@app.route('/api/extract', methods=['POST'])
def extract_details():
    """
    PART 1: Extract details from PDF
    
    Accepts:
    - Multipart form with 'file' (PDF)
    - OR JSON with 'onedrive_filename' to fetch from OneDrive
    
    Returns:
    - session_id: Unique identifier for this processing session
    - extracted_data: Dictionary of all extracted fields
    """
    cleanup_expired_sessions()
    
    try:
        session_id = str(uuid.uuid4())
        session = SessionData(session_id)
        
        pdf_path = None
        email_metadata = None
        
        # Check if file uploaded directly
        if 'file' in request.files:
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            # Save uploaded file
            filename = f"{session_id}_{file.filename}"
            pdf_path = os.path.join(CONFIG['UPLOAD_FOLDER'], filename)
            file.save(pdf_path)
            print(f"✓ Saved uploaded file: {pdf_path}")

            # Save extra attachments (loss run DOCX etc.) - skip images
            IMAGE_EXTS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.webp', '.ico'}
            extra_files = request.files.getlist('extra_attachments')
            for extra_file in extra_files:
                if extra_file and extra_file.filename:
                    ext = os.path.splitext(extra_file.filename)[1].lower()
                    if ext in IMAGE_EXTS:
                        print(f"⚠ Skipping image attachment: {extra_file.filename}")
                        continue
                    extra_filename = extra_file.filename
                    extra_path = os.path.join(CONFIG['UPLOAD_FOLDER'], extra_filename)
                    extra_file.save(extra_path)
                    session.extra_attachment_paths.append(extra_path)
                    print(f"✓ Saved extra attachment: {extra_path}")

            # Download EML if email_metadata provided
            email_metadata_str = request.form.get('email_metadata')
            if email_metadata_str:
                try:
                    email_metadata = json.loads(email_metadata_str)
                    session.email_metadata = email_metadata
                    eml_filename = f"{session_id}_{file.filename.replace('.pdf', '')}.eml"
                    eml_path = os.path.join(CONFIG['UPLOAD_FOLDER'], eml_filename)
                    eml_ok = _download_email_as_eml(email_metadata, eml_path)
                    if eml_ok:
                        session.local_eml_path = eml_path
                except Exception as e:
                    print(f"⚠ EML download skipped: {e}")
        
        # Check if OneDrive filename provided
        elif request.is_json:
            data = request.get_json()
            onedrive_filename = data.get('onedrive_filename')
            
            if not onedrive_filename:
                return jsonify({'error': 'No file or onedrive_filename provided'}), 400
            
            # Download from OneDrive
            try:
                od_client = get_onedrive_client(CONFIG['INPUT_FOLDER'])
                files = od_client.list_files()
                
                # Find the file
                file_info = None
                json_info = None
                for f in files:
                    if f['name'] == onedrive_filename:
                        file_info = f
                    elif f['name'] == f"{onedrive_filename}.json":
                        json_info = f
                
                if not file_info:
                    return jsonify({'error': f'File not found in OneDrive: {onedrive_filename}'}), 404
                
                # Download PDF
                pdf_path = od_client.download_file(file_info, CONFIG['UPLOAD_FOLDER'])
                session.onedrive_file_id = file_info['id']
                session.input_pdf_url = file_info.get('web_url')
                print(f"✓ Downloaded from OneDrive: {pdf_path}")
                
                # Download companion JSON if exists
                if json_info:
                    json_path = od_client.download_file(json_info, CONFIG['UPLOAD_FOLDER'])
                    session.onedrive_json_id = json_info['id']
                    
                    # Load email metadata
                    from email_sender import load_email_metadata
                    email_metadata = load_email_metadata(json_path)
                    session.email_metadata = email_metadata
                    print(f"✓ Loaded email metadata from: {json_path}")
                
            except Exception as e:
                return jsonify({'error': f'OneDrive download failed: {str(e)}'}), 500
        
        else:
            return jsonify({'error': 'No file provided. Send as multipart/form-data or JSON with onedrive_filename'}), 400
        
        if not pdf_path:
            return jsonify({'error': 'Failed to obtain PDF file'}), 500
        
        # Extract data from PDF
        print(f"\n{'='*70}")
        print(f"EXTRACTION REQUEST - Session: {session_id}")
        print(f"{'='*70}")
        
        extracted_data = extract_pdf_form_fields(pdf_path)
        
        if not extracted_data or all(not v for v in extracted_data.values()):
            return jsonify({'error': 'No data could be extracted from PDF'}), 400
        
        populated_count = len([v for v in extracted_data.values() if v])
        print(f"✓ Extracted {populated_count} fields from PDF")
        
        # Extract email fields if metadata available
        extracted_email_fields = None
        if email_metadata:
            print(f"\n[EMAIL EXTRACTION] Processing email metadata...")
            try:
                extracted_email_fields = extract_email_fields(email_metadata)
                session.extracted_email_fields = extracted_email_fields
                print(f"✓ Email fields extracted successfully")
            except Exception as e:
                print(f"✗ Email field extraction failed: {str(e)}")
                # Continue without email fields
        
        # Store in session
        session.pdf_path = pdf_path
        session.extracted_data = extracted_data
        sessions[session_id] = session

        # ---- SAVE TO DATABASE IMMEDIATELY AFTER EXTRACTION ----
        print(f"\n{'='*70}")
        print(f"[DATABASE] ATTEMPTING TO SAVE TO DATABASE")
        print(f"{'='*70}")
        print(f"[DATABASE] Checking for policy number in extracted data...")
        print(f"[DATABASE] Available keys: {list(extracted_data.keys())}")
        
        # Extract policy number from various possible fields
        print(f"[DATABASE] Trying 'Policy Number': {extracted_data.get('Policy Number')}")
        print(f"[DATABASE] Trying 'policy_number': {extracted_data.get('policy_number')}")
        print(f"[DATABASE] Trying 'Policy ID': {extracted_data.get('Policy ID')}")
        print(f"[DATABASE] Email fields available: {extracted_email_fields is not None}")
        if extracted_email_fields:
            print(f"[DATABASE] Email fields keys: {list(extracted_email_fields.keys())}")
            print(f"[DATABASE] Trying email 'policy_number': {extracted_email_fields.get('policy_number')}")
        
        policy_number = (
            extracted_data.get('Policy Number') or 
            extracted_data.get('policy_number') or
            extracted_data.get('Policy ID') or
            (extracted_email_fields.get('policy_number') if extracted_email_fields else None)
        )
        
        print(f"[DATABASE] >>> Final policy_number: {policy_number}")
        
        if policy_number:
            try:
                print(f"[DATABASE] ✓ Policy number found: {policy_number}")
                print(f"[DATABASE] Preparing to save data to underwriting_data table...")
                print(f"[DATABASE] >>> policy_id (policy number) = '{policy_number}'")
                print(f"\n[DATABASE] DATA BEING SENT TO save_underwriting_data():")
                print(f"{'-'*70}")
                
                # Show what will be saved to database
                db_fields = [
                    'Named Insured', 'Mailing Address', 'City', 'State',
                    'NAICS Code', 'Legal Entity Type', 'FEIN', 'Years in Business',
                    'Business Description', 'Prior Carrier',
                    'Loss History - Count', 'Loss History - Total Amount', 'Loss History',
                    'Premises #', 'Bldg #', 'Street Address',
                    'Subject of Insurance', 'Coverage Limit',
                    'Construction Type', 'Year Built', 'Total Area (Sq Ft)',
                    '# of Stories', 'Sprinklered %',
                    'Building Improvements - Wiring', 'Building Improvements - Roofing', 
                    'Building Improvements - Plumbing',
                    'Burglar Alarm Type', 'Fire Protection Class',
                    'Distance to Fire Hydrant', 'Distance to Fire Station'
                ]
                
                for field in db_fields:
                    value = extracted_data.get(field)
                    if value is None:
                        value_display = "[NULL]"
                    elif value == "":
                        value_display = "[EMPTY]"
                    elif isinstance(value, list):
                        value_display = f"[LIST: {len(value)} items] {str(value)[:60]}..."
                    else:
                        value_str = str(value)
                        value_display = value_str if len(value_str) <= 60 else value_str[:60] + '...'
                    print(f"  {field:35s} = {value_display}")
                
                print(f"{'-'*70}")
                print(f"[DATABASE] Calling save_underwriting_data()...")
                underwriting_id = save_underwriting_data(policy_number, extracted_data)
                if underwriting_id:
                    print(f"\n{'='*70}")
                    print(f"[DATABASE] ✓✓✓ SUCCESS - Data saved to PostgreSQL")
                    print(f"[DATABASE] Policy: {policy_number}")
                    print(f"[DATABASE] Record ID: {underwriting_id}")
                    print(f"{'='*70}\n")
                else:
                    print(f"\n{'='*70}")
                    print(f"[DATABASE] ⚠⚠⚠ WARNING - save_underwriting_data returned None")
                    print(f"[DATABASE] This usually means the INSERT/UPDATE didn't return an ID")
                    print(f"{'='*70}\n")
            except Exception as e:
                print(f"\n{'='*70}")
                print(f"[DATABASE] ✗✗✗ EXCEPTION during database save")
                print(f"[DATABASE] Error: {e}")
                print(f"[DATABASE] Full traceback:")
                import traceback
                traceback.print_exc()
                print(f"{'='*70}\n")
        else:
            print(f"\n{'='*70}")
            print(f"[DATABASE] ✗✗✗ SKIPPING DATABASE SAVE")
            print(f"[DATABASE] Reason: No policy number found in extracted data")
            print(f"[DATABASE] Searched fields: Policy Number, policy_number, Policy ID, email fields")
            print(f"[DATABASE] Please ensure the PDF contains a policy number field")
            print(f"{'='*70}\n")
        # Prepare response - convert Loss History to serializable format
        response_data = extracted_data.copy()
        
        # Ensure Loss History is JSON-serializable
        if 'Loss History' in response_data and isinstance(response_data['Loss History'], list):
            response_data['Loss History'] = response_data['Loss History']
        
        print(f"✓ Session created: {session_id}")
        print(f"{'='*70}\n")
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'extracted_data': response_data,
            'extracted_email_fields': extracted_email_fields,
            'fields_extracted': populated_count,
            'has_email_metadata': email_metadata is not None,
            'has_email_fields': extracted_email_fields is not None,
            'message': 'Data extracted successfully. You can now modify the fields and send them back via /api/process'
        }), 200
    
    except Exception as e:
        print(f"✗ Extraction error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/extract/<session_id>', methods=['GET'])
def get_extracted_details(session_id: str):
    """
    GET endpoint to retrieve extracted details by session_id
    
    Returns:
    - extracted_data: Dictionary of all extracted fields
    """
    cleanup_expired_sessions()
    
    session = sessions.get(session_id)
    
    if not session:
        return jsonify({'error': 'Session not found or expired'}), 404
    
    if not session.extracted_data:
        return jsonify({'error': 'No extracted data available for this session'}), 400
    
    response_data = session.extracted_data.copy()
    
    return jsonify({
        'success': True,
        'session_id': session_id,
        'extracted_data': response_data,
        'session_info': session.to_dict()
    }), 200


@app.route('/api/email-fields/<session_id>', methods=['GET'])
def get_email_fields(session_id: str):
    """
    GET endpoint to retrieve extracted email fields by session_id
    
    Returns:
    - extracted_email_fields: Dictionary of extracted email fields
    - confirmed_email_fields: Dictionary of confirmed fields (if already posted)
    """
    cleanup_expired_sessions()
    
    session = sessions.get(session_id)
    
    if not session:
        return jsonify({'error': 'Session not found or expired'}), 404
    
    if not session.extracted_email_fields:
        return jsonify({'error': 'No email fields extracted for this session'}), 400
    
    return jsonify({
        'success': True,
        'session_id': session_id,
        'extracted_email_fields': session.extracted_email_fields,
        'confirmed_email_fields': session.confirmed_email_fields,
        'is_confirmed': session.confirmed_email_fields is not None
    }), 200


@app.route('/api/email-fields', methods=['POST'])
def confirm_email_fields():
    """
    POST endpoint to confirm/update email fields
    
    Expects JSON OR multipart/form-data:
    
    JSON format:
    {
        "session_id": "...",
        "email_fields": {
            "broker_email": "...",
            "broker_name": "...",
            "underwriter_email": "...",
            "underwriter_name": "...",
            "policy_number": "...",
            "broker_agency_name": "...",
            "broker_agency_id": "..."
        }
    }
    
    OR by filename:
    {
        "filename": "acord_test.pdf",
        "email_fields": { ... }
    }
    
    OR directly from insurance form (new format):
    {
        "session_id": "...",
        "brokerEmail": "...",
        "brokerName": "...",
        "underwriterEmail": "...",
        "underwriterName": "...",
        "policyNumber": "...",
        "agencyName": "...",
        "agencyId": "...",
        "emailSummary": "...",
        "comments": "...",
        "timestamp": "..."
    }
    
    Multipart/form-data format (with optional form_pdf file):
    - form_pdf: File upload (optional)
    - All other fields as form data
    
    Returns:
    - success: Boolean
    - confirmed_email_fields: The confirmed data
    """
    cleanup_expired_sessions()
    
    try:
        # DEBUG: Show what we're receiving
        print(f"\n{'='*70}")
        print(f"DEBUG: POST /api/email-fields REQUEST RECEIVED")
        print(f"{'='*70}")
        print(f"Content-Type: {request.content_type}")
        print(f"Method: {request.method}")
        print(f"Headers: {dict(request.headers)}")
        
        # Check if this is multipart/form-data (with file) or JSON
        if request.content_type and 'multipart/form-data' in request.content_type:
            print(f"\n📦 Request Type: MULTIPART/FORM-DATA")
            
            # Show all files in request
            print(f"\n📎 Files in request:")
            if request.files:
                for key in request.files:
                    file = request.files[key]
                    print(f"   - {key}: {file.filename} ({file.content_type if hasattr(file, 'content_type') else 'unknown type'})")
            else:
                print(f"   (No files)")
            
            # Show all form fields
            print(f"\n📝 Form fields in request:")
            if request.form:
                for key, value in request.form.items():
                    print(f"   - {key}: {value[:100] if len(str(value)) > 100 else value}")
            else:
                print(f"   (No form fields)")
            
            # Handle multipart form data
            data = request.form.to_dict()
            
            # Handle form_pdf file if present
            form_pdf_path = None
            if 'form_pdf' in request.files:
                form_pdf = request.files['form_pdf']
                if form_pdf and form_pdf.filename:
                    # Save the form PDF to temp folder
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    form_pdf_filename = f"form_response.pdf"
                    form_pdf_path = os.path.join(CONFIG['UPLOAD_FOLDER'], form_pdf_filename)
                    form_pdf.save(form_pdf_path)
                    print(f"\n✓ Saved form_pdf: {form_pdf_path}")
                else:
                    print(f"\n⚠ form_pdf field exists but no filename")
            else:
                print(f"\n⚠ No 'form_pdf' file in request.files")
        else:
            print(f"\n📄 Request Type: JSON")
            # Handle JSON data
            data = request.get_json()
            form_pdf_path = None
            
            print(f"\n📝 JSON data keys: {list(data.keys()) if data else 'None'}")
            if data:
                for key, value in data.items():
                    if isinstance(value, dict):
                        print(f"   - {key}: {type(value).__name__} with {len(value)} keys")
                    else:
                        print(f"   - {key}: {str(value)[:100] if len(str(value)) > 100 else value}")
                
                # Check if form_pdf is present as base64 string in JSON
                if 'form_pdf' in data and data['form_pdf']:
                    try:
                        print(f"\n📦 Detected form_pdf in JSON (base64 encoded)")
                        form_pdf_base64 = data['form_pdf']
                        
                        # Decode base64 to bytes
                        form_pdf_bytes = base64.b64decode(form_pdf_base64)
                        
                        # Generate filename
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        form_pdf_filename = f"form_response.pdf"
                        form_pdf_path = os.path.join(CONFIG['UPLOAD_FOLDER'], form_pdf_filename)
                        
                        # Save decoded PDF
                        with open(form_pdf_path, 'wb') as f:
                            f.write(form_pdf_bytes)
                        
                        file_size = len(form_pdf_bytes)
                        print(f"   ✓ Decoded and saved form_pdf: {form_pdf_path} ({file_size} bytes)")
                        
                        # Remove form_pdf from data dict so it doesn't interfere with field processing
                        data = {k: v for k, v in data.items() if k != 'form_pdf'}
                        
                    except Exception as e:
                        print(f"   ✗ Error decoding form_pdf: {str(e)}")
                        form_pdf_path = None
        
        print(f"{'='*70}\n")
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Check if data comes from the new insurance form (camelCase fields)
        # or the old format (email_fields object)
        email_fields = data.get('email_fields')
        
        if not email_fields:
            # Check if fields are directly in the request body (new insurance form format)
            if 'brokerEmail' in data or 'broker_email' in data:
                # Normalize field names to the internal format
                email_fields = {
                    'broker_email': data.get('brokerEmail') or data.get('broker_email', 'Not Found'),
                    'broker_name': data.get('brokerName') or data.get('broker_name', 'Not Found'),
                    'underwriter_email': data.get('underwriterEmail') or data.get('underwriter_email', 'Not Found'),
                    'underwriter_name': data.get('underwriterName') or data.get('underwriter_name', 'Not Found'),
                    'policy_number': data.get('policyNumber') or data.get('policy_number', 'Not Found'),
                    'broker_agency_name': data.get('agencyName') or data.get('broker_agency_name', 'Not Found'),
                    'broker_agency_id': data.get('agencyId') or data.get('broker_agency_id', 'Not Found'),
                    'email_summary': data.get('emailSummary', ''),
                    'comments': data.get('comments', ''),
                    'timestamp': data.get('timestamp', '')
                }
            else:
                return jsonify({'error': 'email_fields or form data is required'}), 400
        
        # Find session by session_id or filename
        session = None
        session_id = data.get('session_id')
        filename = data.get('filename')
        
        if session_id:
            session = sessions.get(session_id)
        elif filename:
            # Find by filename
            for sid, sess in sessions.items():
                if sess.pdf_path and os.path.basename(sess.pdf_path) == filename:
                    session = sess
                    session_id = sid
                    break
        else:
            return jsonify({'error': 'Either filename or session_id is required'}), 400
        
        if not session:
            if filename:
                return jsonify({'error': f'No session found for filename: {filename}'}), 404
            else:
                return jsonify({'error': 'Session not found or expired'}), 404
        
        # Store confirmed email fields
        session.confirmed_email_fields = email_fields
        
        # Store form_pdf_path if provided
        if form_pdf_path:
            session.form_pdf_path = form_pdf_path
        
        print(f"\n{'='*70}")
        print(f"EMAIL FIELDS CONFIRMED - Session: {session_id}")
        print(f"{'='*70}")
        print(f"  Broker: {email_fields.get('broker_name')} ({email_fields.get('broker_email')})")
        print(f"  Underwriter: {email_fields.get('underwriter_name')} ({email_fields.get('underwriter_email')})")
        print(f"  Policy: {email_fields.get('policy_number')}")
        print(f"  Agency: {email_fields.get('broker_agency_name')} (ID: {email_fields.get('broker_agency_id')})")
        if email_fields.get('email_summary'):
            print(f"  Summary: {email_fields.get('email_summary')[:60]}{'...' if len(email_fields.get('email_summary', '')) > 60 else ''}")
        if email_fields.get('comments'):
            print(f"  Comments: {email_fields.get('comments')[:60]}{'...' if len(email_fields.get('comments', '')) > 60 else ''}")
        if email_fields.get('timestamp'):
            print(f"  Timestamp: {email_fields.get('timestamp')}")
        if form_pdf_path:
            print(f"  Form PDF: {os.path.basename(form_pdf_path)}")
        print(f"{'='*70}\n")
        
        # Create/update underwriting subfolder and upload form_pdf if available
        confirmed_policy = email_fields.get('policy_number')
        if confirmed_policy and confirmed_policy != 'Not Found':
            underwriting_folder = os.getenv("ONEDRIVE_UNDERWRITING_FOLDER", "Underwriting")
            session.underwriting_subfolder = f"{underwriting_folder}/PN_{confirmed_policy}"
            print(f"✓ Underwriting subfolder set: {session.underwriting_subfolder}")
            
            # Upload form_pdf to underwriting subfolder if OneDrive is configured
            # if form_pdf_path and os.path.exists(form_pdf_path):
            #     if all([CONFIG['TENANT_ID'], CONFIG['CLIENT_ID'], CONFIG['CLIENT_SECRET'], CONFIG['USER_EMAIL']]):
            #         print(f"\n[Upload] Uploading form PDF to underwriting subfolder...")
            #         try:
            #             uw_client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
            #             form_pdf_upload = uw_client.upload_file(
            #                 form_pdf_path,
            #                 session.underwriting_subfolder
            #             )
            #             if form_pdf_upload:
            #                 print(f"   ✓ Form PDF uploaded to {session.underwriting_subfolder}")
            #             else:
            #                 print(f"   ⚠ Form PDF upload failed")
            #         except Exception as e:
            #             print(f"   ⚠ Form PDF upload error: {str(e)}")
            #     else:
            #         print(f"   ⚠ OneDrive not configured - form PDF not uploaded")
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'confirmed_email_fields': email_fields,
            'form_pdf_uploaded': form_pdf_path is not None,
            'underwriting_subfolder': session.underwriting_subfolder,
            'message': 'Email fields confirmed successfully'
        }), 200
    
    except Exception as e:
        print(f"✗ Email fields confirmation error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/process', methods=['POST'])
def process_with_updated_details():
    """
    Process underwriting and generate report with frontend-provided email fields.
    
    Frontend sends email fields directly (simplified flow).
    
    Expects JSON:
    {
        "filename": "acord_test.pdf",  // Required to identify which file
        "email_fields": {           // Required: email fields from frontend
            "policy_number": "...",
            "subject": "...",
            "document_name": "...",
            "comments": "...",
            "timestamp": "..."
        },
        "form_pdf": "base64_encoded_pdf"  // Optional: form PDF from frontend
    }
    
    OR:
    {
        "session_id": "...",
        "email_fields": { ... },
        "form_pdf": "base64_encoded_pdf"
    }
    
    Returns:
    - success: Boolean
    - analysis_summary: Risk analysis results
    - report_url: OneDrive URL to report
    """
    cleanup_expired_sessions()
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Extract email fields and form PDF from request
        email_fields = data.get('email_fields', {})
        form_pdf_base64 = data.get('form_pdf')
        
        if not email_fields:
            return jsonify({'error': 'email_fields is required in request body'}), 400
        
        # Find session
        session = None
        session_id = data.get('session_id')
        filename = data.get('filename')
        
        if session_id:
            session = sessions.get(session_id)
        elif filename:
            for sid, sess in sessions.items():
                if sess.pdf_path and os.path.basename(sess.pdf_path) == filename:
                    if sess.extracted_data:
                        session = sess
                        session_id = sid
                        break
        else:
            return jsonify({'error': 'Either filename or session_id is required'}), 400
        
        if not session:
            # Session doesn't exist yet - watcher hasn't finished processing
            # Store frontend data for when session is ready
            if filename:
                print(f"\n[PROCESS] No session found yet for {filename}")
                print(f"[PROCESS] Storing frontend data for later use...")

                file_prefix = os.getenv("FILE_PREFIX", "acord_")
                if not filename.lower().startswith(file_prefix.lower()):
                    print(f"[PROCESS] ⊘ Skipping file (doesn't start with '{file_prefix}'): {filename}")
                    print(f"[PROCESS] This file will be uploaded to OneDrive but not processed")
                    
                    return jsonify({
                        'success': True,
                        'status': 'skipped',
                        'message': f'File skipped - only files starting with "{file_prefix}" are processed',
                        'filename': filename,
                        'reason': 'filename_filter'
                    }), 200
                
                pending_frontend_data[filename] = {
                    'email_fields': email_fields,
                    'form_pdf_base64': form_pdf_base64,
                    'received_at': datetime.now().isoformat(),
                    'processed': False
                }
                
                print(f"[PROCESS] ✓ Frontend data stored. Will process when watcher completes.")
                
                return jsonify({
                    'success': True,
                    'status': 'pending',
                    'message': 'Data received. Processing will complete when file detection finishes.',
                    'filename': filename
                }), 200
            else:
                return jsonify({'error': 'Session not found or expired'}), 404
        
        if not session.extracted_data:
            return jsonify({'error': 'No extracted data found in session'}), 400
        
        # Store confirmed email fields from frontend
        session.confirmed_email_fields = email_fields
        policy_number = email_fields.get('policy_number')
        
        print(f"\n{'='*70}")
        print(f"PROCESSING REQUEST - Session: {session_id}")
        print(f"{'='*70}")
        print(f"Email fields received from frontend:")
        print(f"  Policy: {policy_number}")
        print(f"  Subject: {email_fields.get('subject', 'N/A')[:50]}...")
        print(f"  Document: {email_fields.get('document_name', 'N/A')}")
        
        # Save comments to policy_db
        comments = email_fields.get('comments')

        # If no comment received, add placeholder
        if not comments:
            comments = "No comments provided."

        if policy_number:
            print(f"  Comments: {comments}")
            try:
                save_policy_to_db({
                    'policy_id': policy_number,
                    'comments': comments
                })
                print(f"  ✓ Comments saved to policy_db for {policy_number}")
            except Exception as e:
                print(f"  ⚠ Failed to save comments: {str(e)}")
        
        # Handle form PDF upload if provided
        form_pdf_uploaded = False
        if form_pdf_base64:
            try:
                import base64
                
                # Decode base64 PDF
                pdf_bytes = base64.b64decode(form_pdf_base64)
                
                # Save form PDF locally first
                form_pdf_filename = f"form_{os.path.basename(session.pdf_path).replace('.pdf', '')}.pdf"
                form_pdf_path = os.path.join(CONFIG['OUTPUT_FOLDER'], form_pdf_filename)
                
                with open(form_pdf_path, 'wb') as f:
                    f.write(pdf_bytes)
                
                print(f"  ✓ Form PDF saved: {form_pdf_filename}")
                
                # Store path in session
                session.form_pdf_path = form_pdf_path
                form_pdf_uploaded = True
                    
            except Exception as e:
                print(f"  ⚠ Failed to process form PDF: {str(e)}")
        
        # Use ORIGINAL extracted ACORD data for ALL processing
        final_data = session.extracted_data.copy()
        print(f"\n✓ Using original ACORD extracted data for processing")
        print(f"   Fields count: {len([v for v in final_data.values() if v])}")
        
        # Compare policy numbers (simple and direct)
        acord_policy = final_data.get('Policy Number') or final_data.get('policy_number')
        print(f"\n[POLICY] Policy comparison:")
        print(f"  From ACORD PDF: {acord_policy}")
        print(f"  From Frontend: {policy_number}")
        
        if acord_policy != policy_number:
            print(f"  ⚠ Policy numbers differ - using frontend value for naming")
        else:
            print(f"  ✓ Policy numbers match")
        
        # Update underwriting subfolder with frontend policy number
        if policy_number:
            underwriting_folder = os.getenv("ONEDRIVE_UNDERWRITING_FOLDER", "Underwriting")
            session.underwriting_subfolder = f"{underwriting_folder}/PN_{policy_number}"
            print(f"  ✓ Underwriting folder: {session.underwriting_subfolder}")
        
        # ── KICK OFF BACKGROUND PROCESSING ────────────────────────────────────
        # Everything from here is independent of the form fields (policy_number
        # and comments are already saved above).  Return 202 immediately so the
        # user is not blocked.
        import time, threading

        session.processing_status = 'processing'

        def _do_processing():
            try:
                start_time = time.time()
                orchestrator = get_orchestrator()

                # 1/4 Prepare DataFrames
                print(f"\n[1/4] Preparing data for analysis...")
                prep_start = time.time()
                success, property_df, claims_df, error = orchestrator.prepare_dataframes(final_data)
                prep_time = time.time() - prep_start

                if not success:
                    session.processing_status = 'error'
                    session.processing_error = f'Data preparation failed: {error}'
                    return

                session.property_df = property_df
                session.claims_df = claims_df
                print(f"   ⏱ Preparation took {prep_time:.2f}s")

                # 2/4 Risk analysis
                print(f"[2/4] Performing risk analysis...")
                analysis_start = time.time()
                success, scored_df, analysis_summary, error = orchestrator.perform_risk_analysis(
                    property_df, claims_df
                )
                analysis_time = time.time() - analysis_start

                if not success:
                    session.processing_status = 'error'
                    session.processing_error = f'Risk analysis failed: {error}'
                    return

                client_name = analysis_summary.get('named_insured', 'Property')
                print(f"   ⏱ Risk analysis took {analysis_time:.2f}s")

                orchestrator.save_intermediate_data(scored_df, analysis_summary, client_name)

                # 3/4 Generate PDF + HTML in parallel
                print(f"[3/4] Generating reports (parallel)...")
                gen_start = time.time()
                _raw_pdf_filename = os.path.basename(session.pdf_path) if session.pdf_path else None
                _session_prefix = session_id + '_'
                pdf_filename = _raw_pdf_filename[len(_session_prefix):] if (_raw_pdf_filename and _raw_pdf_filename.startswith(_session_prefix)) else _raw_pdf_filename

                def generate_pdf_task():
                    pdf_gen_start = time.time()
                    success, pdf_path, error = orchestrator.generate_pdf_report(
                        property_df, claims_df, scored_df, client_name, input_pdf_name=pdf_filename, policy_number=policy_number
                    )
                    pdf_gen_time = time.time() - pdf_gen_start
                    return ('pdf', success, pdf_path, error, pdf_gen_time)

                def generate_html_task():
                    html_gen_start = time.time()
                    try:
                        from html_generator import ClaimsLikelihoodHtmlGenerator
                        generator = ClaimsLikelihoodHtmlGenerator(
                            input_df=property_df,
                            claims_df=claims_df if not claims_df.empty else pd.DataFrame(),
                            output_df=scored_df,
                            policy_number=policy_number
                        )
                        if pdf_filename:
                            base_name = pdf_filename[:-4] if pdf_filename.lower().endswith('.pdf') else pdf_filename
                            html_filename = f"{base_name}_report.html"
                        else:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            safe_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                            html_filename = f"Report_{safe_name}_{timestamp}.html"
                        html_path = os.path.join(CONFIG['OUTPUT_FOLDER'], html_filename)
                        generator.generate_html(output_path=html_path)
                        html_gen_time = time.time() - html_gen_start
                        return ('html', True, html_path, None, html_gen_time)
                    except Exception as e:
                        html_gen_time = time.time() - html_gen_start
                        return ('html', False, None, str(e), html_gen_time)

                pdf_path = None
                html_path = None
                with ThreadPoolExecutor(max_workers=2) as executor:
                    pdf_future = executor.submit(generate_pdf_task)
                    html_future = executor.submit(generate_html_task)

                    pdf_type, pdf_success, pdf_path, pdf_error, pdf_time = pdf_future.result()
                    if not pdf_success:
                        session.processing_status = 'error'
                        session.processing_error = f'PDF generation failed: {pdf_error}'
                        return
                    print(f"   ✓ PDF report generated ({pdf_time:.2f}s)")

                    html_type, html_success, html_path, html_error, html_time = html_future.result()
                    if html_success:
                        print(f"   ✓ HTML report generated ({html_time:.2f}s)")
                    else:
                        print(f"   ⚠ HTML generation failed: {html_error}")

                gen_time = time.time() - gen_start
                print(f"   ⏱ Report generation took {gen_time:.2f}s")

                # 4/4 OneDrive upload
                output_folder_url = None
                print(f"\n[4/4] Uploading to OneDrive...")
                try:
                    upload_start = time.time()

                    if session.underwriting_subfolder:
                        print(f"   📂 Uploading to Underwriting folder...")
                        print(f"   📁 Folder Path: {session.underwriting_subfolder}")

                        try:
                            uw_client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
                            uw_client._create_folder_if_not_exists(session.underwriting_subfolder)

                            upload_tasks = []
                            if session.pdf_path and os.path.exists(session.pdf_path):
                                upload_tasks.append(('input_pdf', session.pdf_path, 'Input PDF'))
                            if pdf_path and os.path.exists(pdf_path):
                                upload_tasks.append(('output_pdf', pdf_path, 'Output PDF'))
                            if session.local_eml_path and os.path.exists(session.local_eml_path):
                                upload_tasks.append(('eml', session.local_eml_path, 'EML'))
                            for extra_path in (session.extra_attachment_paths or []):
                                if os.path.exists(extra_path):
                                    display = os.path.basename(extra_path)
                                    upload_tasks.append(('extra', extra_path, display))

                            if upload_tasks:
                                print(f"   ⬆ Starting parallel upload of {len(upload_tasks)} files...")

                                def upload_file_task(task_info):
                                    task_type, file_path, display_name = task_info
                                    try:
                                        raw_name = os.path.basename(file_path)
                                        session_prefix = session_id + '_'
                                        clean_name = raw_name[len(session_prefix):] if raw_name.startswith(session_prefix) else raw_name
                                        clean_path = os.path.join(os.path.dirname(file_path), clean_name)
                                        renamed = False
                                        if clean_path != file_path and not os.path.exists(clean_path):
                                            os.rename(file_path, clean_path)
                                            renamed = True
                                        upload_path = clean_path if renamed else file_path
                                        result = uw_client.upload_file(upload_path, session.underwriting_subfolder)
                                        if renamed:
                                            os.rename(clean_path, file_path)
                                        return (task_type, True, result, display_name)
                                    except Exception as e:
                                        return (task_type, False, str(e), display_name)

                                with ThreadPoolExecutor(max_workers=len(upload_tasks)) as executor:
                                    futures = {executor.submit(upload_file_task, task): task for task in upload_tasks}
                                    for future in as_completed(futures):
                                        task_type, success, result, display_name = future.result()
                                        if success:
                                            print(f"   ✓ {display_name} uploaded")
                                            if task_type == 'output_pdf' and result and result.get('web_url'):
                                                report_url = result['web_url']
                                                session.output_pdf_url = report_url
                                                print(f"   📄 Web URL: {report_url}")
                                        else:
                                            print(f"   ⚠ {display_name} upload failed: {result}")

                            upload_time = time.time() - upload_start
                            print(f"   ⏱ Parallel upload completed in {upload_time:.2f}s")

                            try:
                                uw_folder_info = uw_client.get_folder_info(session.underwriting_subfolder)
                                if uw_folder_info and uw_folder_info.get('web_url'):
                                    output_folder_url = uw_folder_info['web_url']
                                    print(f"   📁 Underwriting URL: {output_folder_url}")
                            except Exception as e:
                                print(f"   ⚠ Could not get Underwriting folder URL: {str(e)}")

                        except Exception as e:
                            print(f"   ✗ Underwriting subfolder operations failed: {str(e)}")

                except Exception as e:
                    print(f"   ⚠ OneDrive upload warning: {str(e)}")

                # Store results in session so /api/stream can push them
                session.analysis_summary = analysis_summary
                session.processing_status = 'done'
                # Push SSE event — client receives this instantly
                import json as _json
                session.sse_queue.put(_json.dumps({
                    'status': 'done',
                    'policy_number': (session.confirmed_email_fields or {}).get('policy_number')
                }))

                total_time = time.time() - start_time
                print(f"\n{'='*70}")
                print(f"PROCESSING COMPLETE - Session: {session_id}")
                print(f"Risk Score: {analysis_summary['overall_score']:.1f}% ({analysis_summary['risk_level']})")
                print(f"⏱ TOTAL PROCESSING TIME: {total_time:.2f}s")
                print(f"{'='*70}\n")

                # Email + cleanup (already non-blocking within the background thread)
                if session.email_metadata:
                    print(f"\n[Background] Sending email notification...")
                    try:
                        from email_sender import EmailSender, get_recipient_email
                        email_sender = EmailSender(
                            tenant_id=CONFIG['TENANT_ID'],
                            client_id=CONFIG['CLIENT_ID'],
                            client_secret=CONFIG['CLIENT_SECRET'],
                            user_email=CONFIG['USER_EMAIL']
                        )
                        recipient = get_recipient_email(session.email_metadata)
                        print(f"   Sending to: {recipient}")
                        if recipient:
                            html_content = ""
                            if html_path and os.path.exists(html_path):
                                with open(html_path, 'r', encoding='utf-8') as f:
                                    html_content = f.read()
                            if email_sender.send_claims_report_email(
                                to_email=recipient,
                                email_metadata=session.email_metadata,
                                html_report=html_content,
                                input_pdf_path=session.pdf_path,
                                output_pdf_path=pdf_path,
                                report_web_url=session.output_pdf_url,
                                output_folder_url=output_folder_url
                            ):
                                print(f"   ✓ Email sent to {recipient}")
                            else:
                                print(f"   ⚠ Failed to send email")
                        else:
                            print(f"   ⚠ No recipient email found")
                    except Exception as e:
                        print(f"   ⚠ Email sending error: {str(e)}")

                # Move processed files (if from OneDrive)
                if session.onedrive_file_id:
                    try:
                        print(f"\n[Background] Moving input files to processed folder...")
                        processed_folder = os.getenv("ONEDRIVE_PROCESSED_INPUTS", "Processed_inputs")
                        input_client = get_onedrive_client(CONFIG['INPUT_FOLDER'])
                        input_client.move_file(session.onedrive_file_id, processed_folder)
                        print(f"   ✓ Moved input PDF to {processed_folder}")
                        if session.onedrive_json_id:
                            input_client.move_file(session.onedrive_json_id, processed_folder)
                            print(f"   ✓ Moved input JSON to {processed_folder}")
                        if session.onedrive_docx_id:
                            input_client.delete_file(session.onedrive_docx_id)
                            print(f"   ✓ Deleted loss run Word document from input folder")
                        if session.onedrive_docx_json_id:
                            input_client.delete_file(session.onedrive_docx_json_id)
                            print(f"   ✓ Deleted loss run JSON from input folder")
                    except Exception as e:
                        print(f"   ⚠ File move/delete error: {str(e)}")

                # Clean up temporary input files
                try:
                    print(f"\n[Background] Clearing temporary files...")
                    files_deleted = 0
                    if session.pdf_path and os.path.exists(session.pdf_path):
                        os.remove(session.pdf_path)
                        files_deleted += 1
                    if session.local_eml_path and os.path.exists(session.local_eml_path):
                        os.remove(session.local_eml_path)
                        files_deleted += 1
                    if session.pdf_path:
                        json_companion = session.pdf_path + '.json'
                        if os.path.exists(json_companion):
                            os.remove(json_companion)
                            files_deleted += 1
                    if session.form_pdf_path and os.path.exists(session.form_pdf_path):
                        os.remove(session.form_pdf_path)
                        files_deleted += 1
                    for extra_path in (session.extra_attachment_paths or []):
                        if os.path.exists(extra_path):
                            os.remove(extra_path)
                            files_deleted += 1
                    if files_deleted > 0:
                        print(f"   ✓ Cleaned up {files_deleted} temporary file(s)")
                except Exception as e:
                    print(f"   ⚠ Cleanup error: {str(e)}")

                print(f"\n[Background] All tasks completed for session {session_id}")

            except Exception as e:
                import traceback
                print(f"✗ Background processing error: {str(e)}")
                traceback.print_exc()
                session.processing_status = 'error'
                session.processing_error = str(e)
                import json as _json
                session.sse_queue.put(_json.dumps({'status': 'error', 'error': str(e)}))

        # Start background thread and return immediately
        bg_thread = threading.Thread(target=_do_processing, daemon=True)
        bg_thread.start()
        print(f"\n✓ Background processing started for session {session_id}")

        return jsonify({
            'success': True,
            'status': 'processing',
            'session_id': session_id,
            'policy_number': policy_number,
            'message': 'Processing started. Poll /api/status/{session_id} for results.'
        }), 202

    except Exception as e:
        print(f"✗ Processing error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500



@app.route('/api/status/<session_id>', methods=['GET'])
def get_processing_status(session_id):
    """
    Poll endpoint for background processing status.
    Returns: { status: 'processing'|'done'|'error', ... }
    """
    session = sessions.get(session_id)
    if not session:
        return jsonify({'status': 'not_found', 'error': 'Session not found or expired'}), 404

    resp = {
        'status': session.processing_status,
        'session_id': session_id,
    }
    if session.processing_status == 'done':
        resp['analysis_summary'] = session.analysis_summary
        resp['report_url'] = session.output_pdf_url
        resp['policy_number'] = (session.confirmed_email_fields or {}).get('policy_number')
    elif session.processing_status == 'error':
        resp['error'] = session.processing_error

    return jsonify(resp), 200


@app.route('/api/stream/<session_id>', methods=['GET'])
def stream_processing_status(session_id):
    """
    Server-Sent Events endpoint. Holds the connection open until the background
    thread pushes a result, then sends it as an SSE event and closes.
    The client uses  new EventSource(url)  — no polling needed.
    """
    session = sessions.get(session_id)
    if not session:
        return jsonify({'error': 'Session not found'}), 404

    print(f"[SSE] Client connected to stream for session {session_id}")

    def event_stream():
        import json as _json
        try:
            # Send immediate ping to test connection & flush headers
            yield ": ping\n\n"

            # If already done/error before the client connected, push immediately
            if session.processing_status in ('done', 'error'):

                data = _json.dumps({
                    'status': session.processing_status,
                    'error': session.processing_error,
                    'policy_number': (session.confirmed_email_fields or {}).get('policy_number')
                })
                yield f"data: {data}\n\n"
                return

            # Block until background thread pushes the result (timeout = 10 min)
            try:
                data = session.sse_queue.get(timeout=600)
            except Exception:
                data = _json.dumps({'status': 'error', 'error': 'Processing timed out'})
            yield f"data: {data}\n\n"
        except GeneratorExit:
            pass  # Client disconnected early

    response = app.response_class(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',   # disable nginx buffering if behind proxy
            'Access-Control-Allow-Origin': '*',
        }
    )
    return response


@app.route('/api/submit', methods=['POST'])


def submit_direct():
    """
    Direct intake endpoint from Outlook.
    Receives attachment + email metadata and runs the existing processing flow.
    """
    cleanup_expired_sessions()

    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400

        filename = data.get('filename')
        attachment_base64 = data.get('attachment_base64')
        email_metadata = data.get('email_metadata') or {}
        email_fields = data.get('email_fields') or {}
        form_pdf_base64 = data.get('form_pdf')

        if not filename:
            return jsonify({'error': 'filename is required'}), 400
        if not attachment_base64:
            return jsonify({'error': 'attachment_base64 is required'}), 400

        file_prefix = os.getenv("FILE_PREFIX", "acord_")
        if not filename.lower().startswith(file_prefix.lower()):
            return jsonify({
                'success': True,
                'status': 'skipped',
                'message': f'File skipped - only files starting with "{file_prefix}" are processed',
                'filename': filename,
                'reason': 'filename_filter'
            }), 200

        if "," in attachment_base64:
            attachment_base64 = attachment_base64.split(",", 1)[1]

        session_id = str(uuid.uuid4())
        session = SessionData(session_id)

        safe_filename = os.path.basename(filename)
        pdf_path = os.path.join(CONFIG['UPLOAD_FOLDER'], f"{session_id}_{safe_filename}")
        with open(pdf_path, 'wb') as f:
            f.write(base64.b64decode(attachment_base64))
        print(f"[SUBMIT] Saved uploaded file: {pdf_path}")

        # Save extra attachments (loss run DOCX etc.) sent from frontend - skip images
        IMAGE_EXTS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.webp', '.ico'}
        extra_attachments_data = data.get('extra_attachments', [])
        for extra_att in extra_attachments_data:
            try:
                extra_name = extra_att.get('name', 'attachment')
                ext = os.path.splitext(extra_name)[1].lower()
                if ext in IMAGE_EXTS:
                    print(f"[SUBMIT] Skipping image attachment: {extra_name}")
                    continue
                extra_b64 = extra_att.get('contentBytes', '')
                if extra_b64:
                    if ',' in extra_b64:
                        extra_b64 = extra_b64.split(',', 1)[1]
                    extra_safe = os.path.basename(extra_name)
                    extra_path = os.path.join(CONFIG['UPLOAD_FOLDER'], extra_safe)
                    with open(extra_path, 'wb') as f:
                        f.write(base64.b64decode(extra_b64))
                    session.extra_attachment_paths.append(extra_path)
                    print(f"[SUBMIT] Saved extra attachment: {extra_path}")
            except Exception as e:
                print(f"[SUBMIT] Extra attachment save failed: {e}")

        # Preserve email details as companion JSON (legacy-compatible shape)
        json_path = pdf_path + '.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(email_metadata, f, ensure_ascii=True, indent=2)
        # Store FULL email_metadata (including id, itemId, internetMessageId for EML download)
        session.email_metadata = email_metadata

        # Download EML via Graph API
        try:
            eml_filename = f"{session_id}_{safe_filename.replace('.pdf', '')}.eml"
            eml_path = os.path.join(CONFIG['UPLOAD_FOLDER'], eml_filename)
            eml_ok = _download_email_as_eml(email_metadata, eml_path)
            if eml_ok:
                session.local_eml_path = eml_path
                print(f"[SUBMIT] ✓ EML saved: {eml_path}")
            else:
                print(f"[SUBMIT] ⚠ EML download failed, continuing without EML")
        except Exception as e:
            print(f"[SUBMIT] ⚠ EML error: {e}")

        extracted_data = extract_pdf_form_fields(pdf_path)
        if not extracted_data or all(not v for v in extracted_data.values()):
            return jsonify({'error': 'No data could be extracted from PDF'}), 400

        extracted_email_fields = None
        if email_metadata:
            try:
                extracted_email_fields = extract_email_fields(email_metadata)
                session.extracted_email_fields = extracted_email_fields
            except Exception as e:
                print(f"[SUBMIT] Email field extraction failed: {e}")

        session.pdf_path = pdf_path
        session.extracted_data = extracted_data
        sessions[session_id] = session

        _save_extracted_data_to_db(extracted_data, extracted_email_fields)

        # Fallback email fields if frontend sent minimal payload
        if not email_fields:
            email_fields = extracted_email_fields or {}
        if not email_fields:
            email_fields = {
                'policy_number': extracted_data.get('Policy Number') or extracted_data.get('policy_number') or '',
                'document_name': safe_filename,
                'subject': email_metadata.get('subject', ''),
                'comments': '',
                'timestamp': datetime.now().isoformat()
            }

        # Reuse existing /api/process flow so behavior remains identical
        process_payload = {
            'session_id': session_id,
            'email_fields': email_fields
        }
        if form_pdf_base64:
            process_payload['form_pdf'] = form_pdf_base64

        with app.test_request_context('/api/process', method='POST', json=process_payload):
            return process_with_updated_details()

    except Exception as e:
        print(f"[SUBMIT] Direct submit error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


def compare_data(original: dict, updated: dict) -> dict:
    """
    Compare original extracted data with updated data
    
    Returns:
    {
        'changed_fields': {field_name: {'original': value, 'updated': value}},
        'unchanged_fields': [field_names],
        'new_fields': {field_name: value},
        'removed_fields': [field_names]
    }
    """
    changed = {}
    unchanged = []
    new_fields = {}
    removed = []
    
    # Check for changed and unchanged fields
    for key, original_value in original.items():
        if key in updated:
            updated_value = updated[key]
            
            # Handle Loss History specially (compare as JSON)
            if key == 'Loss History':
                original_json = json.dumps(original_value, sort_keys=True)
                updated_json = json.dumps(updated_value, sort_keys=True)
                
                if original_json != updated_json:
                    changed[key] = {
                        'original': original_value,
                        'updated': updated_value
                    }
                else:
                    unchanged.append(key)
            else:
                # Compare other fields as strings
                if str(original_value) != str(updated_value):
                    changed[key] = {
                        'original': original_value,
                        'updated': updated_value
                    }
                else:
                    unchanged.append(key)
        else:
            # Field was removed
            removed.append(key)
    
    # Check for new fields
    for key, value in updated.items():
        if key not in original:
            new_fields[key] = value
    
    return {
        'changed_fields': changed,
        'unchanged_fields': unchanged,
        'new_fields': new_fields,
        'removed_fields': removed,
        'total_changes': len(changed) + len(new_fields) + len(removed)
    }


@app.route('/api/sessions', methods=['GET'])
def list_sessions():
    """List all active sessions (for debugging)"""
    cleanup_expired_sessions()
    
    session_list = []
    for sid, session in sessions.items():
        session_list.append(session.to_dict())
    
    return jsonify({
        'active_sessions': len(sessions),
        'sessions': session_list
    }), 200


@app.route('/api/sessions/<session_id>', methods=['DELETE'])
def delete_session(session_id: str):
    """Delete a specific session"""
    if session_id in sessions:
        sessions.pop(session_id)
        return jsonify({'success': True, 'message': 'Session deleted'}), 200
    else:
        return jsonify({'error': 'Session not found'}), 404


@app.route('/api/pending', methods=['GET'])
def get_pending_files():
    """
    Get list of files detected by watcher that are pending frontend processing
    Frontend uses this to see what files need processing
    
    Returns 200 only when files are available, 404 otherwise
    """
    cleanup_expired_sessions()
    
    pending_list = []
    for session_id, session in sessions.items():
        if session.extracted_data and not session.updated_data:
            pending_list.append({
                'filename': os.path.basename(session.pdf_path) if session.pdf_path else None,
                'email_fields': session.extracted_email_fields,
                'detected_at': session.created_at.isoformat(),
                'has_email_fields': session.extracted_email_fields is not None,
                # Include session_id for reference, but frontend doesn't need to use it
                '_session_id': session_id
            })
    
    if len(pending_list) == 0:
        return jsonify({
            'success': False,
            'message': 'No pending files found. Waiting for watcher to detect files.'
        }), 404
    
    return jsonify({
        'success': True,
        'count': len(pending_list),
        'files': pending_list
    }), 200


@app.route('/api/pending/latest', methods=['GET'])
def get_latest_pending_file():
    """
    Get the most recent file detected by watcher
    Returns just one file with extracted data
    """
    cleanup_expired_sessions()
    
    # Find most recent pending session
    latest_session = None
    latest_time = None
    
    for session_id, session in sessions.items():
        if session.extracted_data and not session.updated_data:
            if latest_time is None or session.created_at > latest_time:
                latest_session = session
                latest_time = session.created_at
    
    if not latest_session:
        return jsonify({
            'success': False,
            'message': 'No pending files found'
        }), 404
    
    return jsonify({
        'success': True,
        'filename': os.path.basename(latest_session.pdf_path) if latest_session.pdf_path else None,
        'email_fields': latest_session.extracted_email_fields,
        'detected_at': latest_session.created_at.isoformat(),
        'has_email_fields': latest_session.extracted_email_fields is not None
    }), 200


@app.route('/api/output-pdf', methods=['GET'])
def get_output_pdf_path():
    """
    Get the OneDrive URL to the most recently generated output PDF
    Returns the web URL to the latest PDF report on OneDrive
    
    Query Parameters:
        session_id (optional): Get PDF URL for specific session
    
    Returns:
        200: PDF URL found
        404: No PDF generated or uploaded yet
    """
    cleanup_expired_sessions()
    
    session_id = request.args.get('session_id')
    
    # If session_id provided, return that specific session's output PDF URL
    if session_id:
        session = sessions.get(session_id)
        if not session:
            return jsonify({
                'success': False,
                'error': 'Session not found'
            }), 404
        
        # Get subfolder URL if underwriting_subfolder exists (even if upload isn't complete yet)
        folder_url = None
        if session.underwriting_subfolder:
            try:
                uw_client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
                uw_folder_info = uw_client.get_folder_info(session.underwriting_subfolder)
                if uw_folder_info and uw_folder_info.get('web_url'):
                    folder_url = uw_folder_info['web_url']
            except Exception as e:
                print(f"   ⚠ Could not get subfolder URL: {str(e)}")
        
        # If folder URL exists, return it (even if PDF URL isn't ready yet)
        if folder_url:
            print(f"[output-pdf] Returning subfolder URL for session {session_id}")
            print(f"   URL: {folder_url}")
            print(f"   Underwriting folder: {session.underwriting_subfolder}")
            print(f"   Upload status: {'Complete' if session.output_pdf_url else 'In progress (background)'}")
            
            return jsonify({
                'success': True,
                'pdf_url': folder_url,
                'filename': os.path.basename(session.output_pdf_path) if session.output_pdf_path else None,
                'session_id': session_id,
                'created_at': session.created_at.isoformat(),
                'underwriting_subfolder': session.underwriting_subfolder,
                'upload_status': 'complete' if session.output_pdf_url else 'in_progress'
            }), 200
        
        # Fallback: check if PDF URL is available
        if not session.output_pdf_url:
            return jsonify({
                'success': False,
                'error': 'Upload in progress - please retry in a moment',
                'upload_status': 'in_progress'
            }), 404
        
        # PDF URL is available - return it
        print(f"[output-pdf] Returning PDF URL for session {session_id}")
        print(f"   URL: {session.output_pdf_url}")
        
        return jsonify({
            'success': True,
            'pdf_url': session.output_pdf_url,
            'filename': os.path.basename(session.output_pdf_path) if session.output_pdf_path else None,
            'session_id': session_id,
            'created_at': session.created_at.isoformat(),
            'underwriting_subfolder': session.underwriting_subfolder,
            'upload_status': 'complete'
        }), 200
    
    # Otherwise, find the most recent session with underwriting_subfolder OR output_pdf_url
    latest_session = None
    latest_time = None
    
    for sid, session in sessions.items():
        # Prioritize sessions with underwriting_subfolder to return immediately (no need to wait for upload)
        if session.underwriting_subfolder or session.output_pdf_url:
            if latest_time is None or session.created_at > latest_time:
                latest_session = session
                latest_time = session.created_at
    
    if not latest_session:
        return jsonify({
            'success': False,
            'message': 'No output PDF ready or in progress in any active session'
        }), 404
    
    # Get subfolder URL if underwriting_subfolder exists
    folder_url = None
    if latest_session.underwriting_subfolder:
        try:
            uw_client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
            uw_folder_info = uw_client.get_folder_info(latest_session.underwriting_subfolder)
            if uw_folder_info and uw_folder_info.get('web_url'):
                folder_url = uw_folder_info['web_url']
                print(f"[output-pdf] Returning latest subfolder URL (upload in progress)")
                print(f"   Session: {latest_session.session_id}")
                print(f"   URL: {folder_url}")
                print(f"   Underwriting folder: {latest_session.underwriting_subfolder}")
                print(f"   Upload status: {'Complete' if latest_session.output_pdf_url else 'In progress (background)'}")
                
                return jsonify({
                    'success': True,
                    'pdf_url': folder_url,
                    'filename': os.path.basename(latest_session.output_pdf_path) if latest_session.output_pdf_path else None,
                    'session_id': latest_session.session_id,
                    'created_at': latest_session.created_at.isoformat(),
                    'underwriting_subfolder': latest_session.underwriting_subfolder,
                    'upload_status': 'complete' if latest_session.output_pdf_url else 'in_progress'
                }), 200
        except Exception as e:
            print(f"   ⚠ Could not get subfolder URL: {str(e)}")
    
    # Fallback: return PDF URL if available
    if latest_session.output_pdf_url:
        print(f"[output-pdf] Returning latest PDF URL")
        print(f"   Session: {latest_session.session_id}")
        print(f"   URL: {latest_session.output_pdf_url}")
        
        return jsonify({
            'success': True,
            'pdf_url': latest_session.output_pdf_url,
            'filename': os.path.basename(latest_session.output_pdf_path) if latest_session.output_pdf_path else None,
            'session_id': latest_session.session_id,
            'created_at': latest_session.created_at.isoformat(),
            'underwriting_subfolder': latest_session.underwriting_subfolder,
            'upload_status': 'complete'
        }), 200
    
    # No URL available yet
    return jsonify({
        'success': False,
        'message': 'Upload in progress - please retry in a moment',
        'upload_status': 'in_progress'
    }), 404


@app.route('/api/input-pdf', methods=['GET'])
def get_input_pdf():
    """
    Get the URL of the original input PDF for a session
    
    Query params:
    - session_id: Required
    """
    session_id = request.args.get('session_id')
    
    if not session_id:
        return jsonify({'error': 'session_id is required'}), 400
    
    session = sessions.get(session_id)
    if not session:
        return jsonify({'error': 'Session not found or expired'}), 404
        
    if not session.input_pdf_url:
        return jsonify({'error': 'Input PDF URL not available for this session'}), 404
        
    return jsonify({
        'success': True,
        'pdf_url': session.input_pdf_url,
        'filename': os.path.basename(session.pdf_path) if session.pdf_path else 'input.pdf',
        'session_id': session_id
    }), 200


# def register_watcher_file(pdf_path: str, extracted_data: dict, 
#                          onedrive_file_id: str = None, 
#                          onedrive_json_id: str = None,
#                          email_metadata: dict = None,
#                          extracted_email_fields: dict = None,
#                          underwriting_subfolder: str = None,
#                          local_eml_path: str = None,
#                          input_pdf_url: str = None) -> str:
#     """
#     Register a file detected by watcher for frontend processing
#     Called by watcher after extraction
    
#     Args:
#         pdf_path: Local path to PDF file
#         extracted_data: Extracted ACORD data
#         onedrive_file_id: OneDrive file ID for the PDF
#         onedrive_json_id: OneDrive file ID for the companion JSON
#         email_metadata: Email metadata dict
#         extracted_email_fields: Extracted email fields (broker, underwriter, etc.)
#         underwriting_subfolder: Path to underwriting subfolder (e.g., "Underwriting/PN_123456")
#         local_eml_path: Local path to downloaded .eml file
#         input_pdf_url: OneDrive web URL to the input PDF
    
#     Returns:
#         session_id for tracking
#     """
#     session_id = str(uuid.uuid4())
#     session = SessionData(session_id)
    
#     session.pdf_path = pdf_path
#     session.extracted_data = extracted_data
#     session.onedrive_file_id = onedrive_file_id
#     session.onedrive_json_id = onedrive_json_id
#     session.email_metadata = email_metadata
#     session.extracted_email_fields = extracted_email_fields
#     session.underwriting_subfolder = underwriting_subfolder
#     session.local_eml_path = local_eml_path
#     session.input_pdf_url = input_pdf_url
    
#     sessions[session_id] = session
    
#     print(f"✓ Registered watcher file: {os.path.basename(pdf_path)} → Session: {session_id}")
#     if extracted_email_fields:
#         print(f"  Email fields: Broker={extracted_email_fields.get('broker_name')}, Policy={extracted_email_fields.get('policy_number')}")
#     if underwriting_subfolder:
#         print(f"  Underwriting folder: {underwriting_subfolder}")
#     if local_eml_path:
#         print(f"  EML file: {os.path.basename(local_eml_path)}")
    
#     # ---- SAVE TO DATABASE IMMEDIATELY AFTER WATCHER REGISTRATION ----
#     print(f"\n{'='*70}")
#     print(f"[WATCHER DB SAVE] ATTEMPTING TO SAVE TO DATABASE")
#     print(f"{'='*70}")
#     print(f"[WATCHER DB SAVE] Checking for policy number in extracted data...")
#     print(f"[WATCHER DB SAVE] Available keys: {list(extracted_data.keys())}")
    
#     # Extract policy number from various possible fields
#     print(f"[WATCHER DB SAVE] Trying 'Policy Number': {extracted_data.get('Policy Number')}")
#     print(f"[WATCHER DB SAVE] Trying 'policy_number': {extracted_data.get('policy_number')}")
#     print(f"[WATCHER DB SAVE] Trying 'Policy ID': {extracted_data.get('Policy ID')}")
#     print(f"[WATCHER DB SAVE] Email fields available: {extracted_email_fields is not None}")
#     if extracted_email_fields:
#         print(f"[WATCHER DB SAVE] Email fields keys: {list(extracted_email_fields.keys())}")
#         print(f"[WATCHER DB SAVE] Trying email 'policy_number': {extracted_email_fields.get('policy_number')}")
    
#     policy_number = (
#         extracted_data.get('Policy Number') or 
#         extracted_data.get('policy_number') or
#         extracted_data.get('Policy ID') or
#         (extracted_email_fields.get('policy_number') if extracted_email_fields else None)
#     )
    
#     # FALLBACK: Use Agency Customer ID or Named Insured if no policy number
#     if not policy_number:
#         print(f"[WATCHER DB SAVE] No policy number found, trying fallback identifiers...")
#         policy_number = (
#             extracted_data.get('Agency Customer ID') or
#             extracted_data.get('FEIN') or
#             extracted_data.get('Named Insured')
#         )
#         if policy_number:
#             print(f"[WATCHER DB SAVE] Using fallback identifier: {policy_number}")
    
#     print(f"[WATCHER DB SAVE] >>> Final policy_number: {policy_number}")
    
#     if policy_number:
#         try:
#             print(f"[WATCHER DB SAVE] ✓ Policy number found: {policy_number}")
#             print(f"[WATCHER DB SAVE] Preparing to save data to underwriting_data table...")
#             print(f"[WATCHER DB SAVE] >>> policy_id (policy number) = '{policy_number}'")
#             print(f"\n[WATCHER DB SAVE] DATA BEING SENT TO save_underwriting_data():")
#             print(f"{'-'*70}")
            
#             # Show what will be saved to database
#             db_fields = [
#                 'Named Insured', 'Mailing Address', 'City', 'State',
#                 'NAICS Code', 'Legal Entity Type', 'FEIN', 'Years in Business',
#                 'Business Description', 'Prior Carrier',
#                 'Loss History - Count', 'Loss History - Total Amount', 'Loss History',
#                 'Premises #', 'Bldg #', 'Street Address',
#                 'Subject of Insurance', 'Coverage Limit',
#                 'Construction Type', 'Year Built', 'Total Area (Sq Ft)',
#                 '# of Stories', 'Sprinklered %',
#                 'Building Improvements - Wiring', 'Building Improvements - Roofing', 
#                 'Building Improvements - Plumbing',
#                 'Burglar Alarm Type', 'Fire Protection Class',
#                 'Distance to Fire Hydrant', 'Distance to Fire Station'
#             ]
            
#             for field in db_fields:
#                 value = extracted_data.get(field)
#                 if value is None:
#                     value_display = "[NULL]"
#                 elif value == "":
#                     value_display = "[EMPTY]"
#                 elif isinstance(value, list):
#                     value_display = f"[LIST: {len(value)} items] {str(value)[:60]}..."
#                 else:
#                     value_str = str(value)
#                     value_display = value_str if len(value_str) <= 60 else value_str[:60] + '...'
#                 print(f"  {field:35s} = {value_display}")
            
#             print(f"{'-'*70}")
#             print(f"[WATCHER DB SAVE] Calling save_underwriting_data()...")
#             underwriting_id = save_underwriting_data(policy_number, extracted_data)
#             if underwriting_id:
#                 print(f"\n{'='*70}")
#                 print(f"[WATCHER DB SAVE] ✓✓✓ SUCCESS - Data saved to PostgreSQL")
#                 print(f"[WATCHER DB SAVE] Policy: {policy_number}")
#                 print(f"[WATCHER DB SAVE] Record ID: {underwriting_id}")
#                 print(f"{'='*70}\n")
#             else:
#                 print(f"\n{'='*70}")
#                 print(f"[WATCHER DB SAVE] ⚠⚠⚠ WARNING - save_underwriting_data returned None")
#                 print(f"[WATCHER DB SAVE] This usually means the INSERT/UPDATE didn't return an ID")
#                 print(f"{'='*70}\n")
#         except Exception as e:
#             print(f"\n{'='*70}")
#             print(f"[WATCHER DB SAVE] ✗✗✗ EXCEPTION during database save")
#             print(f"[WATCHER DB SAVE] Error: {e}")
#             print(f"[WATCHER DB SAVE] Full traceback:")
#             import traceback
#             traceback.print_exc()
#             print(f"{'='*70}\n")
#     else:
#         print(f"\n{'='*70}")
#         print(f"[WATCHER DB SAVE] ✗✗✗ SKIPPING DATABASE SAVE")
#         print(f"[WATCHER DB SAVE] Reason: No policy number found in extracted data")
#         print(f"[WATCHER DB SAVE] Searched fields: Policy Number, policy_number, Policy ID, email fields")
#         print(f"[WATCHER DB SAVE] Please ensure the PDF contains a policy number field")
#         print(f"{'='*70}\n")
    
#     return session_id


# ============================================================================
# POLICY API ENDPOINTS
# ============================================================================

@app.route('/api/policies', methods=['GET'])
def api_get_policies():
    """Get all policies"""
    policies = get_all_policies()
    return jsonify({
        'success': True,
        'policies': policies,
        'count': len(policies)
    })


@app.route('/api/policies/<policy_id>', methods=['GET'])
def api_get_policy_detail(policy_id):
    """Get detailed policy info"""
    policy = get_policy_by_id(policy_id)
    if policy:
        # Also fetch underwriting data to get the loss history list and company name
        underwriting_data = get_underwriting_data_by_policy(policy_id)
        if underwriting_data:
            if 'loss_history' in underwriting_data:
                policy['loss_history'] = underwriting_data['loss_history']
            # Add company name (named_insured) to policy object
            if 'named_insured' in underwriting_data:
                policy['company_name'] = underwriting_data['named_insured']
        
        return jsonify({'success': True, 'policy': policy})
    return jsonify({'success': False, 'error': 'Policy not found'}), 404


@app.route('/api/policies', methods=['POST'])
def api_save_policy():
    """Save policy to database"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        policy_id = save_policy_to_db(data)
        return jsonify({'success': True, 'policy_id': policy_id}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/policies/<policy_id>/underwriting', methods=['GET'])
def api_get_underwriting_data(policy_id):
    """Get underwriting data including loss history for a policy"""
    underwriting_data = get_underwriting_data_by_policy(policy_id)
    if underwriting_data:
        return jsonify({'success': True, 'underwriting_data': underwriting_data})
    return jsonify({'success': False, 'error': 'No underwriting data found for this policy'}), 404


@app.route('/api/policies/<policy_id>/input-attachment', methods=['GET'])
def api_get_policy_input_attachment(policy_id):
    """Get the original input files (PDF, report PDF, EML) for a policy"""
    result = get_policy_input_attachment(policy_id)
    if result:
        return jsonify({
            'success': True,
            'files': result.get('files', []),
            'attachment': result.get('attachment'),  # For backward compatibility
            'folder_url': result.get('folder_url')
        })
    return jsonify({
        'success': False, 
        'error': 'Input attachment not found for this policy'
    }), 404


@app.route('/api/policies/<policy_id>/documents', methods=['POST'])
def api_upload_policy_document(policy_id):
    """Upload a document to the policy's OneDrive folder"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
            
        # Save file temporarily
        temp_path = os.path.join(CONFIG['UPLOAD_FOLDER'], file.filename)
        file.save(temp_path)
        
        # Determine OneDrive folder path
        folder_path = f"Underwriting/PN_{policy_id}"
        
        # Upload to OneDrive
        # Use Output folder client because Underwriting is usually there
        if all([CONFIG['TENANT_ID'], CONFIG['CLIENT_ID'], CONFIG['CLIENT_SECRET'], CONFIG['USER_EMAIL']]):
            client = get_onedrive_client(CONFIG['OUTPUT_FOLDER_OD'])
            result = client.upload_file(temp_path, folder_path)
            
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
            if result:
                return jsonify({
                    'success': True,
                    'url': result.get('web_url'),
                    'name': result.get('name'),
                    'id': result.get('id')
                })
            else:
                return jsonify({'success': False, 'error': 'Failed to upload to OneDrive'}), 500
        else:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'success': False, 'error': 'OneDrive credentials not configured'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# SERVE HTML PAGES
# ============================================================================

from flask import send_from_directory

@app.route('/policy-center')
def serve_policy_center():
    """Serve policy center page"""
    return send_from_directory('.', 'policy_center.html')


@app.route('/policy-new')
def serve_policy_new():
    """Serve new policy page"""
    return send_from_directory('.', 'policy_new.html')


@app.route('/sharepoint_logo.svg')
def serve_sharepoint_logo():
    """Serve SharePoint logo SVG"""
    return send_from_directory('.', 'sharepoint_logo.svg')


@app.route('/logo-cropped.svg')
def serve_logo_cropped():
    """Serve Corporate logo SVG"""
    return send_from_directory('.', 'logo-cropped.svg')

@app.route('/Golden_Bear_White.svg')
def serve_golden_bear_logo():
    """Serve Golden Bear White logo SVG"""
    return send_from_directory('.', 'Golden_Bear_white.svg')

@app.route('/icons8-magnifying-glass.svg')
def serve_magnifying_glass_icon():
    """Serve magnifying glass icon SVG"""
    return send_from_directory('.', 'icons8-magnifying-glass.svg')

@app.route('/policy-detail/<policy_id>')
def serve_policy_detail(policy_id):
    """Serve policy detail page"""
    return send_from_directory('.', 'policy_detail.html')


if __name__ == '__main__':
    print("\n" + "█"*70)
    print("  CLAIMS LIKELIHOOD ANALYSIS - API SERVER")
    print("█"*70)
    print(f"\nStarting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check DB
    db_ok = test_db_connection()
    print(f"Database: {'✓ Connected' if db_ok else '✗ Failed'}")
    
    print("\nAPI Endpoints:")
    print("  POST   /api/extract              - Extract data from PDF")
    print("  GET    /api/extract/<session_id> - Get extracted data")
    print("  GET    /api/email-fields/<sid>   - Get extracted email fields")
    print("  POST   /api/email-fields         - Confirm/update email fields")
    print("  GET    /api/pending              - Get all pending files")
    print("  GET    /api/output-pdf           - Get latest report URL")
    print("  POST   /api/submit               - Direct end-to-end processing (no watcher)")
    print("  POST   /api/process              - Finalize processing")
    print("  GET    /api/sessions             - List active sessions")
    print("\nUI Routes:")
    print("  GET    /policy-center            - Dashboard")
    print("  GET    /policy-new               - New Submission")
    print("  GET    /policy-detail/<id>       - Details Page")
    print("\n" + "="*70)
    
    port = int(os.getenv('PORT', 5003))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    print(f"\nAPI Server starting on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)

