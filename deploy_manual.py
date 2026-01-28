#!/usr/bin/env python3
"""
Manual deployment script for Nginx Stress-Test DPK
Run this script in your terminal with proper permissions
"""
import os
import dtlpy as dl

# Configuration
PROJECT_ID = '2e3fcb12-c387-4865-9409-fa9ffb9c8997'
DPK_NAME = 'nginx-stress-test-flows-env2'

def main():
    print("=" * 60)
    print("Stress-Test DPK Deployment")
    print("=" * 60)

    # 1. Login
    print("\n1. Setting environment to env2 and logging in...")
    dl.setenv('env2')
    if dl.token_expired():
        dl.login()
    print("   ✓ Logged in")

    # 2. Get project
    print(f"\n2. Getting project {PROJECT_ID}...")
    project = dl.projects.get(project_id=PROJECT_ID)
    print(f"   ✓ Project: {project.name}")

    # 3. Get DPK directory
    dpk_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"\n3. DPK directory: {dpk_dir}")

    # 4. Publish DPK
    print(f"\n4. Publishing DPK '{DPK_NAME}'...")
    
    manifest_path = os.path.join(dpk_dir, 'dataloop.json')
    
    dpk = project.dpks.publish(
        manifest_filepath=manifest_path,
        local_path=dpk_dir
    )
    
    print(f"   ✓ DPK published: {dpk.name} v{dpk.version}")
    print(f"   ✓ DPK ID: {dpk.id}")

    # 5. Install/Update App
    print(f"\n5. Installing/updating app...")
    
    try:
        app = project.apps.get(app_name=DPK_NAME)
        print(f"   App exists, updating to v{dpk.version}...")
        app.dpk_version = dpk.version
        app = project.apps.update(app=app)
        print(f"   ✓ App updated")
    except dl.exceptions.NotFound:
        print(f"   Installing new app...")
        app = project.apps.install(dpk=dpk)
        print(f"   ✓ App installed: {app.name}")
    
    print(f"   ✓ App ID: {app.id}")
    print(f"   ✓ App status: {app.status}")

    print("\n" + "=" * 60)
    print("✅ DEPLOYMENT COMPLETE")
    print("=" * 60)
    print(f"\nDPK: {dpk.name} v{dpk.version}")
    print(f"App: {app.name}")
    print(f"Project: {project.name}")
    
    print("\n--- Access the Panel ---")
    print(f"The panel should be available in the project sidebar.")
    print(f"Look for 'Nginx Stress-Test' in the project menu.")
    
    print("\n--- Service Configuration ---")
    print(f"Make sure the stress-test-service is deployed in this project:")
    print(f"  Service Name: stress-test-service")
    print(f"  Project ID: {PROJECT_ID}")
    print(f"\nIf the service doesn't exist, deploy it first using:")
    print(f"  python stress-test-flows/service/deploy_service.py")


if __name__ == "__main__":
    main()
