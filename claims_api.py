"""
Flask API wrapper for OneDrive Claims Processor
This creates an HTTP endpoint that your Outlook add-in can call
Save this as: claims_api.py (NOT flask_api.py)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import tempfile
import base64
from datetime import datetime
from main_od import OneDriveProcessor
import threading

app = Flask(__name__)
CORS(app)  # Allow requests from Outlook add-in

# Initialize processor once
processor = None

def init_processor():
    global processor
    if processor is None:
        processor = OneDriveProcessor()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/process-email', methods=['POST'])
def process_email():
    """
    Process email attachment and trigger claims analysis
    
    Expected payload:
    {
        "subject": "Email subject",
        "from": "sender@email.com",
        "userEmail": "user@email.com",
        "attachments": [
            {
                "name": "acord_form.pdf",
                "content": "base64-encoded-content",
                "contentType": "application/pdf"
            }
        ]
    }
    """
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Extract email info
        subject = data.get('subject', 'Unknown')
        from_email = data.get('from', 'Unknown')
        user_email = data.get('userEmail', 'Unknown')
        attachments = data.get('attachments', [])
        
        print(f"\n{'='*70}")
        print(f"API REQUEST RECEIVED")
        print(f"{'='*70}")
        print(f"From: {from_email}")
        print(f"Subject: {subject}")
        print(f"Attachments: {len(attachments)}")
        
        # Filter for PDF attachments starting with 'acord_'
        valid_attachments = [
            att for att in attachments 
            if att.get('name', '').lower().startswith('acord_') 
            and att.get('name', '').lower().endswith('.pdf')
        ]
        
        if not valid_attachments:
            return jsonify({
                'success': False,
                'error': 'No valid ACORD PDF attachments found (must start with "acord_")'
            }), 400
        
        # Initialize processor if needed
        init_processor()
        
        results = []
        
        # Process each valid attachment
        for attachment in valid_attachments:
            filename = attachment.get('name')
            content_b64 = attachment.get('content')
            
            try:
                # Decode base64 content
                pdf_content = base64.b64decode(content_b64)
                
                # Upload to OneDrive input folder
                print(f"\nUploading {filename} to OneDrive...")
                upload_success = processor.input_client.upload_file_content(
                    filename, 
                    pdf_content
                )
                
                if upload_success:
                    results.append({
                        'filename': filename,
                        'status': 'uploaded',
                        'message': f'File uploaded to OneDrive and queued for processing'
                    })
                    print(f"✓ Uploaded: {filename}")
                else:
                    results.append({
                        'filename': filename,
                        'status': 'failed',
                        'message': 'Upload to OneDrive failed'
                    })
                    
            except Exception as e:
                results.append({
                    'filename': filename,
                    'status': 'error',
                    'message': str(e)
                })
                print(f"✗ Error processing {filename}: {str(e)}")
        
        # Return results
        success_count = sum(1 for r in results if r['status'] == 'uploaded')
        
        return jsonify({
            'success': success_count > 0,
            'message': f'Processed {success_count}/{len(valid_attachments)} files',
            'results': results
        }), 200
        
    except Exception as e:
        print(f"✗ API Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/trigger-processing', methods=['POST'])
def trigger_processing():
    """
    Alternative endpoint: Just notify that new files are available
    The watcher will pick them up automatically
    """
    try:
        data = request.json
        
        return jsonify({
            'success': True,
            'message': 'Processing watcher is active and will detect new files automatically'
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    # Start the Flask API
    # Run on 0.0.0.0 to accept connections from any IP
    # Use a port like 5000 or 8080
    port = int(os.getenv('API_PORT', 5001))
    
    print(f"\n{'='*70}")
    print(f"STARTING CLAIMS PROCESSOR API")
    print(f"{'='*70}")
    print(f"API URL: http://localhost:{port}")
    print(f"Endpoints:")
    print(f"  POST /process-email - Upload and process email attachments")
    print(f"  POST /trigger-processing - Notify of new files")
    print(f"  GET  /health - Health check")
    print(f"{'='*70}\n")
    
    app.run(host='0.0.0.0', port=port, debug=False)