# Local Testing Guide

Test the DPK custom server locally before publishing.

## Quick Start

```bash
cd stress-test-flows/dpk
python test_local.py
```

## Prerequisites

1. **Login to Dataloop**:
   ```python
   import dtlpy as dl
   dl.setenv('env2')
   dl.login()
   ```

2. **Verify static files exist**:
   ```bash
   ls -la panels/stress-test-flows-panel/
   ```
   
   Should see:
   - `index.html`
   - `step1-compute-setup.html`
   - `step2-project.html`
   - `step3-download-images.html`
   - `step4-link-items.html`
   - `step5-pipeline.html`
   - `step6-execute.html`
   - `architecture.html`

## Running the Test

1. **Start the local server**:
   ```bash
   python test_local.py
   ```

2. **Open in browser**:
   - Main page: http://localhost:3000/
   - Execute page: http://localhost:3000/step6-execute.html
   - Index: http://localhost:3000/index.html

3. **Test API endpoints**:
   ```bash
   # Health check
   curl http://localhost:3000/api/health
   
   # Get project info
   curl http://localhost:3000/api/project
   
   # List datasets
   curl http://localhost:3000/api/datasets
   
   # List pipelines
   curl http://localhost:3000/api/pipelines
   ```

## Testing the Interactive Workflow

1. Open http://localhost:8080/step6-execute.html in your browser

2. Configure the form:
   - Max Images: 100 (for testing)
   - Dataset ID: Your dataset ID
   - Pipeline Name: test-pipeline
   - Project ID: 2e3fcb12-c387-4865-9409-fa9ffb9c8997
   - Service ID: Your stress-test-service ID
   - Num Workers: 10

3. Click "🚀 Run Full Workflow"

4. Watch the logs appear in real-time

## Troubleshooting

### Port Already in Use

If port 3000 is already in use, change it in `test_local.py`:
```python
PORT = 3001  # or any other port
```

### Static Files Not Found

Make sure the HTML files are in `panels/stress-test-flows-panel/`:
```bash
# Copy files if needed
cp ../step*.html panels/stress-test-flows-panel/
cp ../index.html panels/stress-test-flows-panel/
cp ../architecture.html panels/stress-test-flows-panel/
```

### Service Not Found

If you get errors about the stress-test-service:
1. Make sure the service is deployed in the project
2. Update `STRESS_TEST_SERVICE_ID` in `test_local.py`
3. Or set it as an environment variable:
   ```bash
   export STRESS_TEST_SERVICE_ID=your-service-id
   python test_local.py
   ```

### Token Expired

If you see "Token expired" errors:
```python
import dtlpy as dl
dl.setenv('env2')
dl.login()
```

## Testing API Endpoints Manually

### Execute a Function

```bash
curl -X POST http://localhost:8080/api/execute \
  -H "Content-Type: application/json" \
  -d '{
    "service_id": "696fab2d7ef2cb6fd8d1ad64",
    "project_id": "2e3fcb12-c387-4865-9409-fa9ffb9c8997",
    "function_name": "download_images",
    "execution_input": {
      "max_images": 10,
      "num_workers": 5
    }
  }'
```

### Get Execution Logs

```bash
# Replace EXECUTION_ID with the ID from the execute response
curl http://localhost:8080/api/logs/EXECUTION_ID
```

### Run Full Workflow

```bash
curl -X POST http://localhost:8080/api/run-full \
  -H "Content-Type: application/json" \
  -d '{
    "max_images": 100,
    "dataset_id": "your-dataset-id",
    "pipeline_name": "test-pipeline",
    "num_workers": 10
  }'
```

## What to Check

Before publishing, verify:

- [ ] Server starts without errors
- [ ] Static HTML files are served correctly
- [ ] API endpoints respond correctly
- [ ] Health check returns OK
- [ ] Project info endpoint works
- [ ] Can list datasets and pipelines
- [ ] Can execute service functions
- [ ] Logs are polled and displayed
- [ ] Interactive workflow page loads
- [ ] No console errors in browser

## Next Steps

Once local testing passes:

1. **Publish the DPK**:
   ```bash
   python deploy_dpk.py
   ```

2. **Install the app** (if not done automatically):
   ```python
   import dtlpy as dl
   dl.setenv('env2')
   project = dl.projects.get(project_id='2e3fcb12-c387-4865-9409-fa9ffb9c8997')
   app = project.apps.install(dpk=dpk)
   ```

3. **Access the panel** in Dataloop UI
