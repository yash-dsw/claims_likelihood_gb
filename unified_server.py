"""
Unified Server - Runs both API Server and OneDrive Watcher simultaneously
Combines api_server.py and main_od.py into a single process
"""

import os
import sys
import threading
import time
import signal
import shutil
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def cleanup_temp_input():
    """Clear the temp_input folder on shutdown"""
    temp_input = os.path.join(os.path.dirname(__file__), 'temp_input')
    
    if os.path.exists(temp_input):
        try:
            # Remove all files in temp_input
            for filename in os.listdir(temp_input):
                file_path = os.path.join(temp_input, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')
            
            print(f"\n✓ Cleared temp_input folder")
        except Exception as e:
            print(f"\n✗ Error clearing temp_input: {e}")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\n" + "="*70)
    print("SHUTDOWN SIGNAL RECEIVED")
    print("="*70)
    
    # Clean up temp_input
    cleanup_temp_input()
    
    print("\nShutting down unified server...")
    print("  API server stopped")
    print("  Watcher thread will exit")
    print("\nGoodbye! 👋\n")
    sys.exit(0)


def run_api_server():
    """Run the Flask API server in the main thread"""
    print("\n" + "="*70)
    print("STARTING API SERVER")
    print("="*70)
    
    # Import and run API server
    from api_server import app
    
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    print(f"\nAPI Server starting on http://127.0.0.1:{port}")
    print("\nAPI Endpoints:")
    print("  POST   /api/extract              - Extract data from PDF")
    print("  GET    /api/extract/<session_id> - Get extracted data")
    print("  GET    /api/email-fields/<sid>   - Get extracted email fields")
    print("  POST   /api/email-fields         - Confirm/update email fields")
    print("  GET    /api/output-pdf           - Get path to latest output PDF")
    print("  POST   /api/process              - Process with updated data")
    print("  GET    /api/sessions             - List active sessions")
    print("  DELETE /api/sessions/<id>        - Delete session")
    print("  GET    /health                   - Health check")
    print("\n" + "="*70)
    
    # Run Flask app (this blocks)
    # Bind to 127.0.0.1 for maximum compatibility with ngrok
    app.run(host='127.0.0.1', port=port, debug=debug, use_reloader=False)



def run_onedrive_watcher():
    """Run the OneDrive watcher in a background thread"""
    # Give Flask a moment to start
    time.sleep(2)
    
    print("\n" + "="*70)
    print("STARTING ONEDRIVE WATCHER")
    print("="*70)
    
    try:
        from main_od import OneDriveProcessor
        
        processor = OneDriveProcessor()
        processor.watch_and_process()
        
    except Exception as e:
        print(f"\n✗ Watcher error: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point - starts both API server and watcher"""
    import argparse
    
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    parser = argparse.ArgumentParser(description='Unified Claims Processing Server')
    parser.add_argument('--api-only', action='store_true', help='Run API server only (no watcher)')
    parser.add_argument('--watcher-only', action='store_true', help='Run watcher only (no API)')
    parser.add_argument('--port', type=int, default=5000, help='API server port (default: 5000)')
    
    args = parser.parse_args()
    
    # Set port environment variable
    os.environ['PORT'] = str(args.port)
    
    print("\n" + "█"*70)
    print("  UNIFIED CLAIMS PROCESSING SERVER")
    print("  API Server + OneDrive Watcher")
    print("█"*70)
    print(f"\nStarting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check credentials
    required = ["ONEDRIVE_TENANT_ID", "ONEDRIVE_CLIENT_ID", "ONEDRIVE_CLIENT_SECRET", "ONEDRIVE_USER_EMAIL"]
    missing = [k for k in required if not os.getenv(k)]
    
    if missing and not args.api_only:
        print(f"\n⚠️  Warning: Missing OneDrive credentials: {', '.join(missing)}")
        print("   OneDrive watcher will not function properly.")
        print("   API server can still accept file uploads.\n")
    
    if args.watcher_only:
        # Run watcher only
        print("\nMode: Watcher Only")
        run_onedrive_watcher()
        
    elif args.api_only:
        # Run API only
        print("\nMode: API Only")
        run_api_server()
        
    else:
        # Run both (default)
        print("\nMode: Unified (API + Watcher)")
        print("\nStarting both services...")
        
        # Start OneDrive watcher in background thread
        watcher_thread = threading.Thread(
            target=run_onedrive_watcher,
            daemon=True,
            name="OneDriveWatcher"
        )
        watcher_thread.start()
        
        # Run API server in main thread (this blocks)
        try:
            run_api_server()
        except KeyboardInterrupt:
            # This will be caught by the signal handler
            pass


if __name__ == "__main__":
    # Force unbuffered output
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    main()
