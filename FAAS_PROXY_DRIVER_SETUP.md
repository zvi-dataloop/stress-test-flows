# faasProxy Storage Driver Setup Guide

## Overview

The **faasProxy storage driver** is a specialized driver built as a layer above the GCS driver. It's required for storing JSON files and enabling datasets to reference files stored in FaaS Storage without copying them (using link items).

## Why faasProxy?

- **JSON File Storage**: Enables storing JSON metadata files in datasets
- **Link Items**: Allows datasets to reference external files without copying
- **FaaS Integration**: Works seamlessly with Dataloop FaaS (Function as a Service) infrastructure
- **Required for Stress Test**: The stress test workflow requires this driver type

## Quick Start

### Check Existing Drivers

Before creating a new driver, check if one already exists:

```bash
# With project name (required)
python check_faas_proxy_drivers.py --project-name "my-project"

# With project ID (required)
python check_faas_proxy_drivers.py --project-id "project-id-here"

# Or use the main script with --check flag
python create_faas_proxy_driver.py --check --project-name "my-project"
python create_faas_proxy_driver.py --check --project-id "project-id-here"
```

### Automated Setup (Recommended)

Run the interactive script:

```bash
python create_faas_proxy_driver.py
```

The script will:
1. ✅ Guide you through login and project selection
2. ✅ Check for existing faasProxy drivers
3. ✅ List existing GCS integrations and allow you to use one, or help create a new one
4. ✅ Create the faasProxy driver
5. ✅ Save configuration for future use

### What You'll Need

**Before running the script, prepare:**

1. **GCS Integration**: You can use an existing GCS integration or create a new one
   - The script will list existing GCS integrations and let you select one
   - To create a new one, see Dataloop documentation: [GCS Integration Guide](https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter)
   - For Cross-Project Integration (recommended): [Cross-Project Integration](https://docs.dataloop.ai/docs/cross-project-integration)
   - For Private Key Integration: [Private Key Integration](https://docs.dataloop.ai/docs/private-key-integration)

2. **Dataloop Access**:
   - Organization name
   - Project name (or create new)
   - Admin/Owner permissions for creating integrations and drivers (if creating new)

3. **Environment Configuration** (optional):
   - Set `DTLPY_ENV` environment variable to specify the Dataloop environment
   - Default: `prod` (if not set)
   - Examples: `prod`, `ford`, `env2`, etc.
   ```bash
   export DTLPY_ENV=ford  # For non-production environments
   python create_faas_proxy_driver.py
   ```

## Step-by-Step Manual Setup

### Environment Configuration

Before running the scripts, you can optionally set the Dataloop environment:

```bash
# Set the Dataloop environment (optional, defaults to 'prod')
export DTLPY_ENV=ford  # or 'prod', 'env2', etc.

# Then run the script
python create_faas_proxy_driver.py
```

**Note:** If `DTLPY_ENV` is not set, the scripts will default to `prod` and prompt you to confirm or change it during execution.

### Step 1: Create GCS Integration

First, create a GCS integration in your Dataloop organization. See the Dataloop documentation for detailed instructions:

- **GCS Integration Guide**: https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter
- **Cross-Project Integration** (recommended for GCP): https://docs.dataloop.ai/docs/cross-project-integration
- **Private Key Integration**: https://docs.dataloop.ai/docs/private-key-integration

### Step 2: Create faasProxy Driver

Create the faasProxy driver using the SDK by setting `driver_type='faasProxy'`:

```python
import dtlpy as dl

dl.login()
project = dl.projects.get(project_name='YOUR_PROJECT')

# Create faasProxy driver using SDK
driver = project.drivers.create(
    name='faas-proxy-driver',      # Your driver name
    driver_type='faasProxy',        # Important: use 'faasProxy' as the type
    integration_id=integration.id,
    bucket_name='your-bucket-name',
    allow_external_delete=True,     # Optional: allow deleting items
    path='optional/folder/path'     # Optional: subfolder in bucket
)

print(f"✓ Created faasProxy driver: {driver.id}")
print(f"  Name: {driver.name}")
print(f"  Type: {driver.type}")
```

**Note:** The SDK supports creating faasProxy drivers directly by passing `driver_type='faasProxy'`.

### Step 3: List/Check faasProxy Drivers

#### Using the Check Script

```bash
python check_faas_proxy_drivers.py --project-name "YOUR_PROJECT"
```

#### Using Python SDK

```python
import dtlpy as dl

dl.login()
project = dl.projects.get(project_name='YOUR_PROJECT')

# List all drivers and filter for faasProxy
all_drivers = project.drivers.list()
faas_proxy_drivers = []

for driver in all_drivers:
    driver_type = getattr(driver, 'type', None) or getattr(driver, 'driverType', None)
    if driver_type == 'faasProxy':
        faas_proxy_drivers.append({
            'id': driver.id,
            'name': driver.name,
            'type': driver_type,
            'bucket': getattr(driver, 'bucket', None)
        })

if faas_proxy_drivers:
    print(f"✓ Found {len(faas_proxy_drivers)} faasProxy driver(s):")
    for driver in faas_proxy_drivers:
        print(f"  - {driver['name']} (ID: {driver['id']})")
else:
    print("No faasProxy drivers found")
```

#### Using API

```python
import dtlpy as dl
import requests

dl.login()
org = dl.organizations.get(organization_name='YOUR_ORG')

headers = {'Authorization': f'Bearer {dl.token()}'}
base_url = dl.environment()

response = requests.get(
    f"{base_url}/organizations/{org.id}/storage-drivers",
    headers=headers
)

if response.status_code == 200:
    drivers = response.json()
    faas_proxy_drivers = [
        driver for driver in drivers
        if driver.get('type') == 'faasProxy' or driver.get('driverType') == 'faasProxy'
    ]
    
    if faas_proxy_drivers:
        print(f"✓ Found {len(faas_proxy_drivers)} faasProxy driver(s):")
        for driver in faas_proxy_drivers:
            print(f"  - {driver.get('name')} (ID: {driver.get('id')})")
```

## Troubleshooting

### "Integration not found"
- Verify the integration was created successfully
- Check you're using the correct organization
- Ensure integration ID is correct

### "Driver creation failed"
- Verify you have Organization Admin/Owner permissions
- Check the GCS integration has access to the bucket
- Ensure bucket name is correct
- Verify the integration is of type GCS

### "faasProxy driver not found"
- List all drivers using SDK: `project.drivers.list()`
- Filter for faasProxy type: check `driver.type == 'faasProxy'` or `driver.driverType == 'faasProxy'`
- Verify driver was created at organization level

## API Reference

### Create Storage Driver

**Endpoint:** `POST /organizations/{organization_id}/storage-drivers`

**Headers:**
```
Authorization: Bearer {token}
Content-Type: application/json
```

**Body:**
```json
{
  "name": "faas-proxy-driver",
  "type": "faasProxy",
  "integrationId": "integration-id",
  "bucket": "bucket-name",
  "allowExternalDelete": true,
  "path": "optional/folder/path"
}
```

**Response:**
```json
{
  "id": "driver-id",
  "name": "faas-proxy-driver",
  "type": "faasProxy",
  "integrationId": "integration-id",
  "bucket": "bucket-name",
  ...
}
```

### List Storage Drivers

**Endpoint:** `GET /organizations/{organization_id}/storage-drivers`

**Response:**
```json
[
  {
    "id": "driver-id",
    "name": "faas-proxy-driver",
    "type": "faasProxy",
    "driverType": "faasProxy",
    "bucket": "bucket-name",
    ...
  }
]
```

## Additional Resources

- [Dataloop Storage Drivers Overview](https://docs.dataloop.ai/docs/storage-drivers-overview)
- [GCS Integration Guide](https://developers.dataloop.ai/tutorials/data_management/external_storage_drivers/gcs/chapter)
- [Cross-Project Integration](https://docs.dataloop.ai/docs/cross-project-integration)
- [Private Key Integration](https://docs.dataloop.ai/docs/private-key-integration)

## Configuration File

After running the script, a configuration file is saved: `faas_proxy_driver_config_{project_name}.json`

Example:
```json
{
  "project_name": "my-project",
  "project_id": "project-id",
  "organization_name": "my-org",
  "organization_id": "org-id",
  "integration_name": "gcs-integration",
  "integration_id": "integration-id",
  "driver_name": "faas-proxy-driver",
  "driver_id": "driver-id",
  "bucket_name": "my-bucket",
  "path": null,
  "allow_external_delete": true
}
```

Use this file to reference your driver configuration in other scripts.
