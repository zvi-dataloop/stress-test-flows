# Nginx Stress-Test DPK

Dataloop Package (DPK) application with custom server panel for interactive stress-test workflow execution.

## Features

- **Custom Server Panel**: Serves interactive HTML documentation
- **API Endpoints**: Execute stress-test functions and stream logs
- **Interactive Workflow**: Run full workflow (Download → Link Items → Create Pipeline → Execute Batch) from the UI
- **Live Logs**: Real-time log streaming from service executions
- **Visual Progress**: Step indicators and progress bars

## Structure

```
dpk/
├── dataloop.json          # DPK manifest
├── main.py                 # Custom server with API endpoints
├── requirements.txt        # Python dependencies
├── deploy_dpk.py          # Deployment script
└── panels/
    └── stress-test-flows-panel/
        ├── index.html
        ├── step1-compute-setup.html
        ├── step2-project.html
        ├── step3-download-images.html
        ├── step4-link-items.html
        ├── step5-pipeline.html
        └── step6-execute.html  # Interactive execution page
```

## Prerequisites

1. **Stress-Test Service**: The DPK requires the `stress-test-service` to be deployed:
   - Service ID: `696fab2d7ef2cb6fd8d1ad64`
   - Service Name: `stress-test-service`
   - Project ID: `c7b25269-e003-46d8-81e3-02daa4826a05`

2. **Dataloop Login**: Must be logged in to env2:
   ```python
   import dtlpy as dl
   dl.setenv('env2')
   dl.login()
   ```

## Deployment

```bash
python stress-test-flows/dpk/deploy_dpk.py
```

This will:
1. Publish the DPK to the project
2. Install/update the app
3. Deploy the custom server service

## Usage

1. **Access the Panel**: 
   - Open the project in Dataloop UI
   - Look for "Nginx Stress-Test" in the project sidebar menu
   - Click to open the panel

2. **Navigate to Execute Tab**:
   - Click on "Step 6: Execute" in the navigation
   - Or directly open `step6-execute.html`

3. **Run Full Workflow**:
   - Configure parameters (max images, dataset ID, etc.)
   - Click "🚀 Run Full Workflow"
   - Watch live logs and progress

## API Endpoints

The custom server provides these endpoints:

### `GET /api/health`
Health check endpoint.

### `GET /api/project`
Get current project information.

### `GET /api/datasets`
List all datasets in the project.

### `GET /api/pipelines`
List all pipelines in the project.

### `POST /api/execute`
Execute a function on the stress-test-service.

**Request:**
```json
{
  "service_id": "696fab2d7ef2cb6fd8d1ad64",
  "project_id": "c7b25269-e003-46d8-81e3-02daa4826a05",
  "function_name": "download_images",
  "execution_input": {
    "max_images": 50000,
    "num_workers": 50
  }
}
```

**Response:**
```json
{
  "success": true,
  "execution_id": "...",
  "status": "running"
}
```

### `POST /api/run-full`
Run the complete workflow using `run_full_stress_test`.

**Request:**
```json
{
  "max_images": 50000,
  "dataset_id": "...",
  "pipeline_name": "stress-test-resnet-v11",
  "num_workers": 50
}
```

### `GET /api/logs/<execution_id>`
Get logs for an execution.

**Response:**
```json
{
  "logs": [
    {
      "timestamp": "...",
      "level": "INFO",
      "message": "...",
      "function_name": "..."
    }
  ],
  "status": "running"
}
```

### `GET /api/execution/<execution_id>`
Get execution status.

## Configuration

The DPK server uses these environment variables:

- `PORT`: Server port (default: 8080)
- `DL_PROJECT_ID`: Current project ID (set automatically by Dataloop)
- `STRESS_TEST_SERVICE_ID`: Service ID to call (default: `696fab2d7ef2cb6fd8d1ad64`)
- `STRESS_TEST_SERVICE_NAME`: Service name (default: `stress-test-service`)

## Troubleshooting

### Panel Not Showing

- Check that the app is installed: `project.apps.list()`
- Verify the service is running: `project.services.list()`
- Check browser console for errors

### API Calls Failing

- Verify the stress-test-service is deployed and running
- Check service logs: `service.logs.list()`
- Ensure service ID is correct in the DPK configuration

### Logs Not Appearing

- Logs are polled every second
- Check execution status: `GET /api/execution/<execution_id>`
- Verify execution is still running

## Development

### Local Testing

```python
import dtlpy as dl
dl.setenv('env2')
dl.login()

# Set environment variables
import os
os.environ['DL_PROJECT_ID'] = 'c7b25269-e003-46d8-81e3-02daa4826a05'
os.environ['STRESS_TEST_SERVICE_ID'] = '696fab2d7ef2cb6fd8d1ad64'

# Run server
from main import StressTestServer
server = StressTestServer()
server.run()
```

Then open `http://localhost:8080/step6-execute.html` in your browser.

### Updating HTML Files

1. Edit files in `stress-test-flows/`
2. Copy updated files to `dpk/panels/stress-test-flows-panel/`
3. Update DPK version in `dataloop.json`
4. Redeploy: `python deploy_dpk.py`
