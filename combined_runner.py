"""
Combined runner: Starts both Flask API and OneDrive watcher
Run this instead of main_od.py
"""

import sys
import os
import threading
from datetime import datetime

def run_api():
    """Run Flask API in a thread"""
    from claims_api import app
    port = int(os.getenv('API_PORT', 5004))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def run_watcher():
    """Run OneDrive watcher in a thread"""
    from main_od import OneDriveProcessor
    
    # Small delay to let API start first
    import time
    time.sleep(2)
    
    processor = OneDriveProcessor()
    processor.watch_and_process()

def main():
    """Start both services"""
    # Force unbuffered output
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    print(f"\n{'='*70}")
    print(f"STARTING COMBINED CLAIMS PROCESSOR SERVICE")
    print(f"{'='*70}")
    print(f"Started at: {datetime.now()}")
    print(f"\nStarting 2 services:")
    print(f"  1. Flask API (for Outlook add-in)")
    print(f"  2. OneDrive File Watcher")
    print(f"\nPress Ctrl+C to stop all services...")
    print(f"{'='*70}\n")
    sys.stdout.flush()
    
    try:
        # Start API in background thread
        api_thread = threading.Thread(target=run_api, daemon=True)
        api_thread.start()
        
        # Run watcher in main thread (so Ctrl+C works properly)
        run_watcher()
        
    except KeyboardInterrupt:
        print("\n\n{'='*70}")
        print("SERVICES STOPPED BY USER")
        print(f"{'='*70}\n")
        sys.stdout.flush()
        sys.exit(0)
        
    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)

if __name__ == "__main__":
    main()