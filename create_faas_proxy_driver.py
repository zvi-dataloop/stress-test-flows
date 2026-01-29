#!/usr/bin/env python3
"""
Script to create or check faasProxy storage drivers for a Dataloop project.

The faasProxy driver is built as a layer above the GCS driver and is needed
to store JSON files. This script helps you:
1. Create or verify a GCS integration
2. Create a faasProxy storage driver that uses the GCS integration
3. List existing faasProxy drivers in a project

Usage:
    # Create a new faasProxy driver (interactive)
    python create_faas_proxy_driver.py
    
    # Check existing faasProxy drivers (project name or ID required)
    python create_faas_proxy_driver.py --check --project-name "my-project"
    python create_faas_proxy_driver.py --check --project-id "project-id-here"

Prerequisites:
- Dataloop SDK installed (pip install dtlpy)
- GCS bucket created in Google Cloud Platform
- GCS service account credentials (JSON key file) or IAM setup for Cross-Project integration

Environment Variables:
- DTLPY_ENV: Dataloop environment (optional, defaults to 'prod')
  Example: export DTLPY_ENV=ford
"""

import dtlpy as dl
import json
import os
import sys
import argparse
from typing import Optional, Dict, Any


def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def print_step(step_num: int, text: str):
    """Print a formatted step."""
    print(f"\n[Step {step_num}] {text}")
    print("-" * 70)


def get_user_input(prompt: str, default: Optional[str] = None, required: bool = True) -> str:
    """Get user input with optional default value."""
    if default:
        prompt_text = f"{prompt} [{default}]: "
    else:
        prompt_text = f"{prompt}: "
    
    while True:
        value = input(prompt_text).strip()
        if value:
            return value
        elif default:
            return default
        elif not required:
            return ""
        else:
            print("  This field is required. Please enter a value.")


def list_gcs_integrations(organization: dl.Organization) -> list:
    """
    List all GCS integrations in the organization.
    
    Args:
        organization: Dataloop organization object
    
    Returns:
        List of GCS integration objects
    """
    gcs_integrations = []
    try:
        all_integrations = organization.integrations.list()
        print(f"  Checking {len(all_integrations)} integration(s) in organization...")
        
        for integration in all_integrations:
            # Check if it's a GCS integration
            # Integration can be either an object or a dict, so use getattr/get for both
            integration_type = None
            if hasattr(integration, 'type'):
                integration_type = integration.type
            elif hasattr(integration, 'integrations_type'):
                integration_type = integration.integrations_type
            elif isinstance(integration, dict):
                integration_type = integration.get('type')
            else:
                integration_type = getattr(integration, 'type', None)
            
            # Get name and ID - handle both object and dict
            if isinstance(integration, dict):
                integration_name = integration.get('name', 'Unknown')
                integration_id = integration.get('id', 'Unknown')
            else:
                integration_name = getattr(integration, 'name', 'Unknown')
                integration_id = getattr(integration, 'id', 'Unknown')
            
            # Check if it matches GCS type (could be string 'gcs', 'gcp-workload-identity-federation', or enum)
            is_gcs = False
            print(f"    Integration type: {integration}")
            if integration_type:
                # Convert to string for comparison
                type_str = str(integration_type).lower().strip()
                
                # Check against enum (dl.ExternalStorage.GCS)
                if integration_type == dl.ExternalStorage.GCS:
                    is_gcs = True
                # Check if it's exactly 'gcs' (most common case)
                elif type_str == 'gcs':
                    is_gcs = True
                # Check for gcp-workload-identity-federation
                elif type_str == 'gcp-workload-identity-federation':
                    is_gcs = True
                # Check for variations
                elif 'gcs' in type_str or 'google' in type_str or 'cloudstorage' in type_str.replace(' ', '') or 'workload-identity' in type_str:
                    is_gcs = True
            else:
                # Fallback: check if name contains GCS-related keywords
                name_lower = integration_name.lower()
                if 'gcs' in name_lower or 'google' in name_lower or 'cloud storage' in name_lower or 'workload-identity' in name_lower:
                    print(f"    Found potential GCS integration by name: {integration_name} (type attribute not found)")
                    is_gcs = True
            
            if is_gcs:
                gcs_integrations.append(integration)
                print(f"    ✓ GCS integration found: {integration_name} (ID: {integration_id}, type: {integration_type})")
            else:
                print(f"    - Skipped: {integration_name} (type: {integration_type})")
                
    except Exception as e:
        print(f"  Warning: Could not list integrations: {e}")
        import traceback
        print(f"  Error details: {traceback.format_exc()}")
    
    return gcs_integrations


def create_gcs_integration(organization: dl.Organization, integration_name: str, 
                          gcs_credentials_path: Optional[str] = None,
                          use_cross_project: bool = False) -> dl.Integration:
    """
    Create a GCS integration.
    
    Args:
        organization: Dataloop organization object
        integration_name: Name for the integration
        gcs_credentials_path: Path to GCS service account JSON key file
        use_cross_project: Whether to use cross-project integration (requires IAM setup)
    
    Returns:
        Created integration object
    """
    print(f"\n  Creating GCS integration: {integration_name}")
    
    # Check if integration already exists
    try:
        existing_integrations = organization.integrations.list()
        for integration in existing_integrations:
            if integration.get('name', 'Unknown') == integration_name:
                print(f"  ✓ Integration '{integration_name}' already exists (ID: {integration.get('id', 'Unknown') })")
                return integration
    except Exception as e:
        print(f"  Warning: Could not check existing integrations: {e}")
    
    if use_cross_project:
        # Cross-project integration - requires IAM setup
        print("\n  Using Cross-Project Integration (recommended for GCP)")
        print("  Note: This requires IAM role setup in GCP. See documentation:")
        print("  https://docs.dataloop.ai/docs/cross-project-integration")
        
        bucket_name = get_user_input("  Enter GCS bucket name", required=True)
        
        try:
            integration = organization.integrations.create(
                integrations_type=dl.ExternalStorage.GCS,
                name=integration_name,
                options={
                    'bucket': bucket_name,
                    'type': 'crossProject'
                }
            )
            print(f"  ✓ Created Cross-Project integration: {integration.get('id', 'Unknown')}")
            return integration
        except Exception as e:
            print(f"  ✗ Failed to create Cross-Project integration: {e}")
            raise
    else:
        # Private key integration
        if not gcs_credentials_path:
            gcs_credentials_path = get_user_input(
                "  Enter path to GCS service account JSON key file",
                required=True
            )
        
        if not os.path.exists(gcs_credentials_path):
            raise FileNotFoundError(f"GCS credentials file not found: {gcs_credentials_path}")
        
        print(f"  Reading GCS credentials from: {gcs_credentials_path}")
        with open(gcs_credentials_path, 'r') as f:
            gcs_json = json.load(f)
        
        gcs_to_string = json.dumps(gcs_json)
        
        try:
            integration = organization.integrations.create(
                integrations_type=dl.ExternalStorage.GCS,
                name=integration_name,
                options={
                    'key': '',
                    'secret': '',
                    'content': gcs_to_string
                }
            )
            print(f"  ✓ Created GCS integration: {integration.get('id', 'Unknown')}")
            return integration
        except Exception as e:
            print(f"  ✗ Failed to create GCS integration: {e}")
            raise


def list_faas_proxy_drivers(project: dl.Project) -> list:
    """
    List all faasProxy drivers available for the project.
    
    Args:
        project: Dataloop project object
    
    Returns:
        List of faasProxy driver dictionaries with id, name, type, bucket, etc.
    """
    print(f"\n  Listing faasProxy drivers for project: {project.name} (ID: {project.id})")
    
    faas_proxy_drivers = []
    
    try:
        # List all drivers for the project
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
        
        if faas_proxy_drivers:
            print(f"  ✓ Found {len(faas_proxy_drivers)} faasProxy driver(s):")
            for idx, driver_info in enumerate(faas_proxy_drivers, 1):
                print(f"    {idx}. {driver_info['name']} (ID: {driver_info['id']})")
                if driver_info.get('bucket'):
                    print(f"       Bucket: {driver_info['bucket']}")
        else:
            print(f"  ℹ No faasProxy drivers found in project")
        
        return faas_proxy_drivers
        
    except Exception as e:
        print(f"  ✗ Error listing drivers: {e}")
        return []


def create_faas_proxy_driver(project: dl.Project, driver_name: str, 
                             integration: dl.Integration, 
                             bucket_name: str,
                             path: Optional[str] = None,
                             allow_external_delete: bool = True) -> Dict[str, Any]:
    """
    Create a faasProxy storage driver.
    
    The faasProxy driver is built as a layer above the GCS driver and is needed
    to store JSON files. It uses the GCS integration but has type 'faasProxy'.
    
    Args:
        project: Dataloop project object
        driver_name: Name for the storage driver
        integration: GCS integration object
        bucket_name: GCS bucket name
        path: Optional path within the bucket (folder)
        allow_external_delete: Whether to allow deleting items from storage
    
    Returns:
        Created driver information as dict
    """
    print(f"\n  Creating faasProxy storage driver: {driver_name}")
    
    # Check if driver already exists by listing faasProxy drivers
    existing_faas_proxy_drivers = list_faas_proxy_drivers(project)
    
    # Check if a driver with the same name already exists
    for driver_info in existing_faas_proxy_drivers:
        if driver_info['name'] == driver_name:
            print(f"  ✓ Driver '{driver_name}' already exists (ID: {driver_info['id']})")
            return {
                'id': driver_info['id'],
                'name': driver_info['name'],
                'type': driver_info['type'],
                'exists': True
            }
    
    # Create faasProxy driver using SDK
    # The SDK supports faasProxy type by passing 'faasProxy' as driver_type
    try:
        print(f"  Creating driver using SDK with type 'faasProxy'...")
        
        # Build driver creation parameters
        # Get integration type and ID - handle both object and dict
        integration_type = None
        if isinstance(integration, dict):
            integration_type = integration.get('type')
            integration_id = integration.get('id')
        else:
            if hasattr(integration, 'type'):
                integration_type = integration.type
            elif hasattr(integration, 'integrations_type'):
                integration_type = integration.integrations_type
            elif hasattr(integration, 'integration_type'):
                integration_type = integration.integration_type
            integration_id = getattr(integration, 'id', None)
        
        # Default to GCS if we can't determine the type
        if integration_type is None:
            integration_type = dl.ExternalStorage.GCS
        
        # Convert integration_type to string if it's an enum
        if hasattr(integration_type, 'value'):
            integration_type_str = integration_type.value
        elif isinstance(integration_type, str):
            integration_type_str = integration_type
        else:
            integration_type_str = str(integration_type).lower()
        
        # Get organization ID from project
        org_id = None
        if hasattr(project, 'org') and hasattr(project.org, 'id'):
            org_id = project.org.id
        elif hasattr(project, 'organization'):
            org_id = project.organization
        elif hasattr(project, '_org_id'):
            org_id = project._org_id
        
        if org_id is None:
            # Try to get from project metadata or fetch organization
            try:
                # Get project details which should include org info
                project_dict = project.to_json() if hasattr(project, 'to_json') else {}
                org_id = project_dict.get('org') or project_dict.get('organization')
                if not org_id:
                    # Last resort: get organization from project's org attribute
                    if hasattr(project, 'org'):
                        org_obj = project.org
                        if hasattr(org_obj, 'id'):
                            org_id = org_obj.id
            except:
                pass
        
        if org_id is None:
            raise ValueError("Could not determine organization ID from project. Please ensure project has organization information.")
        
        # Build payload according to the API structure
        payload = {
            "integrationId": integration_id,
            "integrationType": integration_type_str,
            "name": driver_name,
            "metadata": {
                "system": {
                    "projectId": project.id
                }
            },
            "type": "faasProxy",
            "payload": {
                "bucket": bucket_name
            },
            "allowExternalDelete": allow_external_delete,
            "creator": project._client_api.info().get("user_email", ""),
        }
        
        if path:
            payload["payload"]["path"] = path
        
        print(f"  Driver payload: {json.dumps({k: v for k, v in payload.items() if k != 'creator'}, indent=2)}")
        
        # Create driver using direct API call
        url = f"/drivers"
        success, response = project._client_api.gen_request(
            req_type='post',
            path=url,
            json_req=payload
        )
        
        if not success:
            error_msg = response if isinstance(response, str) else str(response)
            raise Exception(f"Failed to create driver: {error_msg}")
        
        # Parse response - gen_request returns (success, response_data)
        # response_data is typically already a dict
        if isinstance(response, dict):
            driver_data = response
        elif hasattr(response, 'json'):
            driver_data = response.json()
        else:
            driver_data = response
        
        driver_id = driver_data.get('id') or driver_data.get('driverId')
        driver_name_result = driver_data.get('name', driver_name)
        
        if not driver_id:
            raise Exception(f"Driver created but no ID returned in response: {driver_data}")
        
        print(f"  ✓ Created faasProxy driver: {driver_id}")
        return {
            'id': driver_id,
            'name': driver_name_result,
            'type': 'faasProxy',
            'exists': False
        }
    except Exception as e:
        print(f"  ✗ Error creating faasProxy driver: {e}")
        print(f"  Error details: {type(e).__name__}")
        raise


def verify_driver(project: dl.Project, driver_id: str) -> bool:
    """Verify that the driver exists and is accessible."""
    print(f"\n  Verifying driver: {driver_id}")
    
    try:
        # Try to get the driver using SDK
        all_drivers = project.drivers.list()
        for driver in all_drivers:
            if driver.id == driver_id:
                print(f"  ✓ Driver verified successfully")
                print(f"    Name: {driver.name}")
                driver_type = getattr(driver, 'type', None) or getattr(driver, 'driverType', None)
                print(f"    Type: {driver_type}")
                bucket = getattr(driver, 'bucket', None)
                if bucket:
                    print(f"    Bucket: {bucket}")
                return True
        
        print(f"  ✗ Driver not found in project")
        return False
    except Exception as e:
        print(f"  ✗ Error verifying driver: {e}")
        return False


def check_drivers_only(project_id: Optional[str] = None, project_name: Optional[str] = None):
    """Check and list faasProxy drivers for a project."""
    print_header("Check faasProxy Drivers")
    
    if not project_id and not project_name:
        print("  ✗ Error: Project ID or project name is required")
        print("  Usage: python create_faas_proxy_driver.py --check --project-name 'project-name'")
        print("     or: python create_faas_proxy_driver.py --check --project-id 'project-id'")
        sys.exit(1)
    
    # Login
    env = get_user_input("  Enter Dataloop environment (e.g., 'prod', 'ford')", 
                        default=os.environ.get('DTLPY_ENV', 'prod'), 
                        required=False)
    if env:
        dl.setenv(env)
    
    # Check if already logged in
    if dl.token_expired():
        print("\n  Logging in to Dataloop...")
        try:
            dl.login()
            print("  ✓ Login successful")
        except Exception as e:
            print(f"  ✗ Login failed: {e}")
            sys.exit(1)
    else:
        print("\n  ✓ Already logged in to Dataloop")
    
    # Get project
    try:
        if project_id:
            project = dl.projects.get(project_id=project_id)
        else:
            project = dl.projects.get(project_name=project_name)
        
        print(f"  ✓ Found project: {project.name} (ID: {project.id})")
    except Exception as e:
        print(f"  ✗ Failed to get project: {e}")
        sys.exit(1)
    
    # List faasProxy drivers
    drivers = list_faas_proxy_drivers(project)
    
    if drivers:
        print(f"\n  Summary: Found {len(drivers)} faasProxy driver(s)")
        print("\n  Driver Details:")
        for idx, driver_info in enumerate(drivers, 1):
            print(f"\n  [{idx}] {driver_info['name']}")
            print(f"      ID: {driver_info['id']}")
            print(f"      Type: {driver_info['type']}")
            if driver_info.get('bucket'):
                print(f"      Bucket: {driver_info['bucket']}")
            if driver_info.get('path'):
                print(f"      Path: {driver_info['path']}")
            if driver_info.get('integration_id'):
                print(f"      Integration ID: {driver_info['integration_id']}")
    else:
        print(f"\n  No faasProxy drivers found in project '{project.name}'")
        print("  You can create one using: python create_faas_proxy_driver.py")
    
    return drivers


def main():
    """Main function to create faasProxy driver."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Create or check faasProxy storage drivers for Dataloop projects'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Only check/list existing faasProxy drivers (do not create)'
    )
    parser.add_argument(
        '--project-id',
        type=str,
        help='Project ID (required for --check mode)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        help='Project name (required for --check mode)'
    )
    
    args = parser.parse_args()
    
    # If --check flag is set, only list drivers
    if args.check:
        if not args.project_id and not args.project_name:
            parser.error("--check requires either --project-id or --project-name")
        check_drivers_only(project_id=args.project_id, project_name=args.project_name)
        return
    
    print_header("faasProxy Storage Driver Creation Script")
    
    print("""
This script will help you create a faasProxy storage driver for your Dataloop project.

The faasProxy driver is built as a layer above the GCS driver and is needed to
store JSON files. It requires:
1. A GCS integration (you can use an existing one or create a new one)
2. A faasProxy storage driver that uses the GCS integration

The script will:
- List existing GCS integrations and let you select one, or
- Help you create a new GCS integration if needed

GCS Integration Documentation:
- GCS Integration Guide: https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter
- Cross-Project Integration: https://docs.dataloop.ai/docs/cross-project-integration
- Private Key Integration: https://docs.dataloop.ai/docs/private-key-integration

Let's get started!
""")
    
    # Step 1: Login and get organization
    print_step(1, "Login to Dataloop")
    
    env = get_user_input("  Enter Dataloop environment (e.g., 'prod', 'ford')", 
                        default=os.environ.get('DTLPY_ENV', 'prod'), 
                        required=False)
    if env:
        dl.setenv(env)
        print(f"  Using environment: {env}")
    
    # Check if already logged in
    if dl.token_expired():
        print("\n  Logging in to Dataloop...")
        try:
            dl.login()
            print("  ✓ Login successful")
        except Exception as e:
            print(f"  ✗ Login failed: {e}")
            sys.exit(1)
    else:
        print("\n  ✓ Already logged in to Dataloop")
    
    # Get organization
    org_name = get_user_input("  Enter organization name", required=True)
    try:
        organization = dl.organizations.get(organization_name=org_name)
        print(f"  ✓ Found organization: {organization.name} (ID: {organization.id})")
    except Exception as e:
        print(f"  ✗ Failed to get organization: {e}")
        sys.exit(1)
    
    # Step 2: Get or create project
    print_step(2, "Select Project")
    
    project_name = get_user_input("  Enter project name", required=True)
    try:
        project = dl.projects.get(project_name=project_name)
        print(f"  ✓ Found project: {project.name} (ID: {project.id})")
    except dl.exceptions.NotFound:
        create = get_user_input("  Project not found. Create it? (y/n)", default="y")
        if create.lower() == 'y':
            project = dl.projects.create(project_name=project_name)
            print(f"  ✓ Created project: {project.name} (ID: {project.id})")
        else:
            print("  Exiting. Please create the project first.")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Failed to get/create project: {e}")
        sys.exit(1)
    
    # Step 3: Create or select GCS integration
    print_step(3, "GCS Integration Setup")
    
    print("\n  You can use an existing GCS integration or create a new one.")
    print("  If you need to create one, see the Dataloop documentation:")
    print("  - GCS Integration: https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter")
    print("  - Cross-Project Integration: https://docs.dataloop.ai/docs/cross-project-integration")
    print("  - Private Key Integration: https://docs.dataloop.ai/docs/private-key-integration")
    print()
    
    # List existing GCS integrations
    existing_integrations = list_gcs_integrations(organization)
    integration = None
    
    if existing_integrations:
        print(f"  Found {len(existing_integrations)} existing GCS integration(s):")
        for idx, existing_int in enumerate(existing_integrations, 1):
            print(f"    {idx}. {existing_int.get('name', 'Unknown')} (ID: {existing_int.get('id', 'Unknown')})")
        print()
        
        print("  Choose an option:")
        print("  1. Use an existing integration (select from list above)")
        print("  2. Create a new integration")
        print()
        
        choice = get_user_input(
            "  Enter choice (1 or 2)",
            default="1"
        )
        
        if choice == "1":
            # Use existing integration
            if len(existing_integrations) == 1:
                integration = existing_integrations[0]
                integration_name = integration.get('name', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'name', 'Unknown')
                integration_id = integration.get('id', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'id', 'Unknown')
                print(f"  ✓ Using existing integration: {integration_name} (ID: {integration_id})")
            else:   
                integration_choice = get_user_input(
                    f"  Enter integration number (1-{len(existing_integrations)})",
                    required=True
                )
                try:
                    idx = int(integration_choice) - 1
                    if 0 <= idx < len(existing_integrations):
                        integration = existing_integrations[idx]
                        integration_name = integration.get('name', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'name', 'Unknown')
                        integration_id = integration.get('id', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'id', 'Unknown')
                        print(f"  ✓ Selected integration: {integration_name} (ID: {integration_id})")
                    else:
                        print(f"  ✗ Invalid choice. Please run the script again.")
                        sys.exit(1)
                except ValueError:
                    print(f"  ✗ Invalid input. Please run the script again.")
                    sys.exit(1)
        elif choice == "2":
            # User wants to create a new integration
            print("\n  Creating a new GCS integration...")
        else:
            print(f"  ✗ Invalid choice. Please run the script again.")
            sys.exit(1)
    else:
        print("  No existing GCS integrations found.")
        print("  Creating a new GCS integration...")
    
    # If no existing integration selected, create a new one
    if integration is None:
        print("\n  Creating a new GCS integration...")
        print("  Choose integration method:")
        print("  1. Cross-Project Integration (recommended for GCP, requires IAM setup)")
        print("  2. Private Key Integration (requires service account JSON key file)")
        
        integration_method = get_user_input("  Enter choice (1 or 2)", default="1")
        
        integration_name = get_user_input(
            "  Enter integration name",
            default=f"gcs-integration-{project_name}",
            required=True
        )
        
        use_cross_project = (integration_method == "1")
        gcs_credentials_path = None
        
        if not use_cross_project:
            gcs_credentials_path = get_user_input(
                "  Enter path to GCS service account JSON key file",
                required=True
            )
        
        try:
            integration = create_gcs_integration(
                organization=organization,
                integration_name=integration_name,
                gcs_credentials_path=gcs_credentials_path,
                use_cross_project=use_cross_project
            )
        except Exception as e:
            print(f"\n  ✗ Failed to create integration: {e}")
            print("\n  Please refer to the Dataloop documentation for creating GCS integrations:")
            print("    - GCS Integration Guide: https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter")
            print("    - Cross-Project Integration: https://docs.dataloop.ai/docs/cross-project-integration")
            print("    - Private Key Integration: https://docs.dataloop.ai/docs/private-key-integration")
            sys.exit(1)
    
    # Step 4: Create faasProxy driver
    print_step(4, "Create faasProxy Storage Driver")
    
    driver_name = get_user_input(
        "  Enter driver name",
        default=f"faas-proxy-{project_name}",
        required=True
    )
    
    bucket_name = get_user_input("  Enter GCS bucket name", required=True)
    
    path = get_user_input(
        "  Enter optional path within bucket (folder, leave empty for root)",
        required=False
    )
    if not path:
        path = None
    
    allow_delete = get_user_input(
        "  Allow external delete? (y/n)",
        default="y"
    )
    allow_external_delete = (allow_delete.lower() == 'y')
    
    try:
        driver_info = create_faas_proxy_driver(
            project=project,
            driver_name=driver_name,
            integration=integration,
            bucket_name=bucket_name,
            path=path,
            allow_external_delete=allow_external_delete
        )
        
        driver_id = driver_info['id']
        
        # Verify the driver
        verify_driver(project, driver_id)
        
        # Summary
        print_header("Setup Complete!")
        
        integration_status = "used" if driver_info.get('exists') else "created"
        # Get integration name and ID - handle both object and dict
        integration_name = integration.get('name', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'name', 'Unknown')
        integration_id_val = integration.get('id', 'Unknown') if isinstance(integration, dict) else getattr(integration, 'id', 'Unknown')
        
        print(f"""
✓ GCS Integration {integration_status}: {integration_name} (ID: {integration_id_val})
✓ faasProxy Driver created: {driver_name} (ID: {driver_id})

Driver ID: {driver_id}
""")
        
        # Save configuration
        config_file = f"faas_proxy_driver_config_{project_name}.json"
        config = {
            'project_name': project_name,
            'project_id': project.id,
            'organization_name': organization.name,
            'organization_id': organization.id,
            'integration_name': integration_name,
            'integration_id': integration_id_val,
            'driver_name': driver_name,
            'driver_id': driver_id,
            'bucket_name': bucket_name,
            'path': path,
            'allow_external_delete': allow_external_delete
        }
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"\n  Configuration saved to: {config_file}")
        
    except Exception as e:
        print(f"\n  ✗ Failed to create faasProxy driver: {e}")
        print("\n  Troubleshooting:")
        print("    - Ensure the GCS integration is valid and has access to the bucket")
        print("    - Check that you have organization admin/owner permissions")
        print("    - Verify the bucket name is correct")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n  Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
