#!/usr/bin/env python3
"""
Simple script to check and list faasProxy drivers for a Dataloop project.

This script lists all faasProxy storage drivers available for a project,
filtered by project_id and driver type 'faasProxy'.

Usage:
    python check_faas_proxy_drivers.py --project-name "my-project"
    python check_faas_proxy_drivers.py --project-id "project-id-here"
    
Note: Either --project-name or --project-id is required.

Environment Variables:
- DTLPY_ENV: Dataloop environment (optional, defaults to 'prod')
  Example: export DTLPY_ENV=ford
"""

import dtlpy as dl
import argparse
import os
import sys


def list_faas_proxy_drivers(project_id: str = None, project_name: str = None):
    """
    List all faasProxy drivers for a project.
    
    Args:
        project_id: Project ID (optional, if not provided will ask)
        project_name: Project name (optional, if not provided will ask)
    
    Returns:
        List of faasProxy driver dictionaries
    """
    # Login
    env = os.environ.get('DTLPY_ENV', 'prod')
    dl.setenv(env)
    
    # Check if already logged in
    if dl.token_expired():
        print(f"Logging in to Dataloop (env: {env})...")
        try:
            dl.login()
            print("✓ Login successful\n")
        except Exception as e:
            print(f"✗ Login failed: {e}")
            sys.exit(1)
    else:
        print(f"✓ Already logged in to Dataloop (env: {env})\n")
    
    # Get project - require project_id or project_name
    if not project_id and not project_name:
        print("✗ Error: Project ID or project name is required")
        print("Usage: python check_faas_proxy_drivers.py --project-name 'project-name'")
        print("   or: python check_faas_proxy_drivers.py --project-id 'project-id'")
        sys.exit(1)
    
    try:
        if project_id:
            project = dl.projects.get(project_id=project_id)
        else:
            project = dl.projects.get(project_name=project_name)
        
        print(f"Project: {project.name} (ID: {project.id})\n")
    except Exception as e:
        print(f"✗ Failed to get project: {e}")
        sys.exit(1)
    
    # List drivers using SDK
    faas_proxy_drivers = []
    
    try:
        print("Listing drivers via SDK...")
        all_drivers = project.drivers.list()
        
        # Filter for faasProxy type
        for driver in all_drivers:
            driver_type = getattr(driver, 'type', None) or getattr(driver, 'driverType', None)
            if driver_type == 'faasProxy':
                driver_info = {
                    'id': driver.id,
                    'name': driver.name,
                    'type': driver_type,
                    'bucket': getattr(driver, 'bucket', None),
                    'path': getattr(driver, 'path', None),
                    'allow_external_delete': getattr(driver, 'allow_external_delete', None),
                    'integration_id': getattr(driver, 'integration_id', None)
                }
                faas_proxy_drivers.append(driver_info)
        
    except Exception as e:
        print(f"✗ Error listing drivers: {e}")
        sys.exit(1)
    
    # Display results
    print("=" * 70)
    print(f"faasProxy Drivers for Project: {project.name}")
    print("=" * 70)
    
    if faas_proxy_drivers:
        print(f"\nFound {len(faas_proxy_drivers)} faasProxy driver(s):\n")
        for idx, driver_info in enumerate(faas_proxy_drivers, 1):
            print(f"[{idx}] {driver_info['name']}")
            print(f"     ID: {driver_info['id']}")
            print(f"     Type: {driver_info['type']}")
            if driver_info.get('bucket'):
                print(f"     Bucket: {driver_info['bucket']}")
            if driver_info.get('path'):
                print(f"     Path: {driver_info['path']}")
            if driver_info.get('integration_id'):
                print(f"     Integration ID: {driver_info['integration_id']}")
            print()
    else:
        print("\nNo faasProxy drivers found in this project.")
        print("\nTo create a faasProxy driver, run:")
        print("  python create_faas_proxy_driver.py")
    
    return faas_proxy_drivers


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='List faasProxy storage drivers for a Dataloop project',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_faas_proxy_drivers.py --project-name "my-project"
  python check_faas_proxy_drivers.py --project-id "abc123-def456"
        """
    )
    parser.add_argument(
        '--project-id',
        type=str,
        help='Project ID (required if --project-name not provided)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        help='Project name (required if --project-id not provided)'
    )
    
    args = parser.parse_args()
    
    if not args.project_id and not args.project_name:
        parser.error("Either --project-id or --project-name is required")
    
    list_faas_proxy_drivers(
        project_id=args.project_id,
        project_name=args.project_name
    )


if __name__ == "__main__":
    main()
