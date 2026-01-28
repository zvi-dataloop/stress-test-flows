#!/usr/bin/env python3
"""
Local test script for DPK custom server
Run this to test the server locally before publishing
"""

import os
import sys
import dtlpy as dl

# Add the dpk directory to path so we can import main
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import StressTestServer

def main():
    print("=" * 60)
    print("Local DPK Server Test")
    print("=" * 60)
    
    # Configuration
    PROJECT_ID = '2e3fcb12-c387-4865-9409-fa9ffb9c8997'
    PORT = 3000
    
    # 1. Login
    print("\n1. Setting environment to env2 and logging in...")
    dl.setenv('env2')
    if dl.token_expired():
        print("   Please login...")
        dl.login()
    print("   ✓ Logged in")
    
    # 2. Set environment variables
    print(f"\n2. Setting environment variables...")
    os.environ['DL_PROJECT_ID'] = PROJECT_ID
    os.environ['PORT'] = str(PORT)
    os.environ['STRESS_TEST_SERVICE_ID'] = '696fab2d7ef2cb6fd8d1ad64'
    os.environ['STRESS_TEST_SERVICE_NAME'] = 'stress-test-service'
    
    project = dl.projects.get(project_id=PROJECT_ID)
    print(f"   ✓ Project: {project.name}")
    print(f"   ✓ Project ID: {PROJECT_ID}")
    
    # 3. Check static files
    print(f"\n3. Checking static files...")
    static_dir = os.path.join(os.path.dirname(__file__), 'panels', 'stress-test-flows-panel')
    if os.path.exists(static_dir):
        files = os.listdir(static_dir)
        print(f"   ✓ Static directory: {static_dir}")
        print(f"   ✓ Files found: {len(files)}")
        for f in files:
            print(f"     - {f}")
    else:
        print(f"   ⚠️  Static directory not found: {static_dir}")
        print(f"   Creating directory...")
        os.makedirs(static_dir, exist_ok=True)
        print(f"   ✓ Directory created")
    
    # 4. Start server
    print(f"\n4. Starting server on port {PORT}...")
    print(f"\n" + "=" * 60)
    print("SERVER STARTED")
    print("=" * 60)
    print(f"\nAccess the server at:")
    print(f"  http://localhost:{PORT}/")
    print(f"  http://localhost:{PORT}/index.html")
    print(f"  http://localhost:{PORT}/step6-execute.html")
    print(f"\nAPI endpoints:")
    print(f"  http://localhost:{PORT}/api/health")
    print(f"  http://localhost:{PORT}/api/project")
    print(f"  http://localhost:{PORT}/api/datasets")
    print(f"  http://localhost:{PORT}/api/pipelines")
    print(f"\nPress Ctrl+C to stop the server")
    print("=" * 60 + "\n")
    
    try:
        server = StressTestServer()
        server.run()
    except KeyboardInterrupt:
        print("\n\nServer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Server error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
