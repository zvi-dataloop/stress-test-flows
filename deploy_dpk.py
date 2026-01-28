#!/usr/bin/env python3
"""
Deploy Nginx Stress-Test Flows DPK to Dataloop env2
Custom server with interactive HTML panel
"""

import os
import dtlpy as dl

# Configuration
# PROJECT_ID = '2e3fcb12-c387-4865-9409-fa9ffb9c8997'
PROJECT_ID = '80138ed5-169a-4be1-9603-b5e13832e55d'
DPK_NAME = 'nginx-stress4-test-flows-env2'
# DPK_VERSION = '2.0.8'


def main():
    print("=" * 60)
    print("Stress-Test DPK Deployment")
    print("=" * 60)

    # 1. Login
    print("\n1. Setting environment to env2 and logging in...")
    dl.setenv('env2')
    if dl.token_expired():
        dl.login()
    print("   Logged in")

    # 2. Get project
    print(f"\n2. Getting project {PROJECT_ID}...")
    project = dl.projects.get(project_id=PROJECT_ID)
    print(f"   Project: {project.name}")

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

    print("\n" + "=" * 60)
    print("DPK PUBLISH COMPLETE")
    print("=" * 60)
    print(f"\nDPK: {dpk.name} v{dpk.version}")
    print(f"Project: {project.name}")
    
    print("\n--- Next Steps ---")
    print(f"To install the app, run:")
    print(f"  app = project.apps.install(dpk=dpk)")
    print(f"\nOr update existing app:")
    print(f"  app = project.apps.get(app_name='{DPK_NAME}')")
    print(f"  app.dpk_version = dpk.version")
    print(f"  app = project.apps.update(app=app)")


if __name__ == "__main__":
    main()
