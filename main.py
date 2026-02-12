"""
Nginx Stress-Test Benchmark Server
Custom server that serves HTML documentation and provides API for stress testing.
Combines UI server and stress test execution in one service.
"""

import os
import json
import logging
import secrets
import time
import threading
from threading import Thread
import zipfile
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import dtlpy as dl
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('stress-test-server')

# Static files directory
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'panels', 'stress-test-flows-panel')

# Default Configuration for stress test execution
DEFAULT_STORAGE_BASE_PATH = '/s/pd_datfs2/stress-test' 
DEFAULT_LINK_BASE_URL = 'http://34.140.193.179/s/pd_datfs2/stress-test'
DEFAULT_DATASET_ID = ''

# COCO dataset URLs
COCO_TRAIN2017_BASE_URL = "http://images.cocodataset.org/train2017/"
COCO_VAL2017_BASE_URL = "http://images.cocodataset.org/val2017/"

# Cache for COCO image list
_coco_image_cache = None

# Store execution logs and status
execution_logs = {}
execution_status = {}

# Store workflow progress for real-time updates
workflow_progress = {}  # workflow_id -> {status, current_step, progress_pct, logs, result, error}

# Prometheus URL for Nginx RPS metrics (same namespace: prometheus:9090)
PROMETHEUS_URL = os.environ.get('PROMETHEUS_URL', 'http://prometheus:3000').rstrip('/')

# Global reference to the server instance (set by StressTestServer.__init__)
_stress_test_server_instance = None


def get_coco_images(dataset: str = 'all', progress_callback=None):
    """
    Get COCO image URLs.
    
    Args:
        dataset: 'train2017' (118k), 'val2017' (5k), or 'all' (123k)
        progress_callback: optional (step, status, pct, message) to report e.g. "Downloading annotations..."
    
    Returns:
        list of image URLs
    """
    global _coco_image_cache
    
    if _coco_image_cache is not None:
        logger.info(f"Using cached image list: {len(_coco_image_cache)} images")
        return _coco_image_cache
    
    logger.info(f"Loading COCO image list (dataset={dataset})...")
    
    train_ids = []
    val_ids = []
    
    # Try bundled file: same dir as main.py first, then ../service/ (for deployed layout)
    _dir = os.path.dirname(os.path.abspath(__file__))
    bundled_candidates = [
        os.path.join(_dir, 'coco_all_ids.json'),
        os.path.join(_dir, '..', 'service', 'coco_all_ids.json'),
    ]
    bundled_file = None
    for p in bundled_candidates:
        if os.path.exists(p):
            bundled_file = p
            break
    if bundled_file:
        try:
            with open(bundled_file, 'r') as f:
                data = json.load(f)
            train_ids = data.get('train2017', [])
            val_ids = data.get('val2017', [])
            logger.info(f"Loaded from bundled file: {len(train_ids)} train, {len(val_ids)} val")
        except Exception as e:
            logger.error(f"Failed to load bundled file: {e}")
    
    # Fallback to old val-only file
    if not train_ids and not val_ids:
        old_bundled = os.path.join(_dir, 'coco_val2017_ids.json')
        if not os.path.exists(old_bundled):
            old_bundled = os.path.join(_dir, '..', 'service', 'coco_val2017_ids.json')
        if os.path.exists(old_bundled):
            try:
                with open(old_bundled, 'r') as f:
                    val_ids = json.load(f)
                logger.info(f"Loaded {len(val_ids)} val IDs from old bundled file")
            except:
                pass
    
    # Fallback: download annotations (~250MB, one-time; can take 5–10 min)
    if not train_ids and not val_ids:
        msg = "Downloading COCO annotations (~250MB, one-time)..."
        logger.info(f"Bundled files not found, {msg}")
        if progress_callback:
            try:
                progress_callback('download_images', 'running', 15, msg)
            except Exception:
                pass
        try:
            url = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
            response = requests.get(url, timeout=600)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                with z.open('annotations/instances_train2017.json') as f:
                    train_annotations = json.load(f)
                with z.open('annotations/instances_val2017.json') as f:
                    val_annotations = json.load(f)
            
            train_ids = [img['id'] for img in train_annotations.get('images', [])]
            val_ids = [img['id'] for img in val_annotations.get('images', [])]
            logger.info(f"Downloaded: {len(train_ids)} train, {len(val_ids)} val")
            
        except Exception as e:
            logger.error(f"Failed to download annotations: {e}")
    
    # Build URL list based on dataset selection
    urls = []
    if dataset in ['train2017', 'all']:
        urls.extend([f"{COCO_TRAIN2017_BASE_URL}{str(img_id).zfill(12)}.jpg" for img_id in train_ids])
    if dataset in ['val2017', 'all']:
        urls.extend([f"{COCO_VAL2017_BASE_URL}{str(img_id).zfill(12)}.jpg" for img_id in val_ids])
    
    if not urls:
        logger.warning("No images found, using minimal fallback")
        urls = [f"{COCO_VAL2017_BASE_URL}{str(i).zfill(12)}.jpg" for i in [397133, 37777, 252219, 87038, 174482]]
    
    logger.info(f"Total: {len(urls)} COCO image URLs available")
    _coco_image_cache = urls
    return urls


# Process-pool constants for download_images (after annotations / URL list is ready)
DOWNLOAD_CHUNK_SIZE = 1000
DOWNLOAD_THREADS_PER_PROCESS = 32
DOWNLOAD_MAX_PROCESSES = 32


def _path_inside_storage_from_link_base_url(link_base_url: str) -> str:
    """
    Extract path-inside-storage from Link Base URL. Link URL = serve-agent path + path inside storage.
    Strips leading 'serve-agent/' if present so storage path matches the path inside storage.
    E.g. https://my-domain.ai/serve-agent/root-folder/subfolder -> root-folder/subfolder
         http://34.140.193.179/root-folder/subfolder -> root-folder/subfolder
    """
    if not link_base_url:
        return ''
    parsed = urlparse(link_base_url)
    path = (parsed.path or '').strip('/')
    if not path:
        return ''
    if path.startswith('serve-agent/'):
        path = path[len('serve-agent/'):].lstrip('/')
    return path


def _remote_path_from_link_base_url(link_base_url: str) -> str:
    """
    Derive dataset remote path from Link Base URL (path inside storage; last segment as folder).
    E.g. .../serve-agent/root-folder/subfolder -> /subfolder; .../root-folder -> /root-folder
    Falls back to /stress-test if path cannot be derived.
    """
    path_inside = _path_inside_storage_from_link_base_url(link_base_url)
    if not path_inside:
        return '/stress-test'
    parts = [p for p in path_inside.split('/') if p]
    folder = parts[-1] if parts else 'stress-test'
    return f'/{folder}'


def _storage_path_from_link_base_url(link_base_url: str):
    """
    Derive filesystem storage path for download from Link Base URL.
    Link URL = serve-agent path + path inside storage; we use path inside storage (strip serve-agent/).
    E.g. https://my-domain.ai/serve-agent/root-folder/subfolder -> /root-folder/subfolder
         http://34.140.193.179/root-folder/subfolder -> /root-folder/subfolder
    Returns None if link_base_url is missing (caller should use default storage_path).
    """
    path_inside = _path_inside_storage_from_link_base_url(link_base_url)
    if not path_inside:
        return None
    return '/' + path_inside if not path_inside.startswith('/') else path_inside


def _upload_one_link_item_standalone(dataset, filename: str, link_base_url_full: str, overwrite: bool = True):
    """Create or update one link item in dataset. Used inside worker process (no self).
    overwrite=True ensures existing items get the correct link URL (create or replace).
    """
    ext = filename.lower().split('.')[-1]
    mimetype = 'image/jpeg' if ext in ['jpg', 'jpeg'] else f'image/{ext}'
    link_url = f"{link_base_url_full}/{filename}"
    link_item_content = {
        "type": "link",
        "shebang": "dataloop",
        "metadata": {
            "dltype": "link",
            "linkInfo": {
                "type": "url",
                "ref": link_url,
                "mimetype": mimetype
            }
        }
    }
    json_filename = f"{os.path.splitext(filename)[0]}.json"
    json_bytes = json.dumps(link_item_content).encode('utf-8')
    buffer = io.BytesIO(json_bytes)
    remote_path = _remote_path_from_link_base_url(link_base_url_full)
    dataset.items.upload(
        local_path=buffer,
        remote_path=remote_path,
        remote_name=json_filename,
        overwrite=overwrite
    )


def _download_chunk_worker(args):
    """
    Run in a separate process: download a chunk of URLs using N threads.
    Args: (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id, workflow_id, create_link, threads_per_process)
    Returns: (downloaded, failed, skipped, error_str or None) - 4-tuple so main process can log worker errors without losing partial results.
    """
    try:
        (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id,
         workflow_id, create_link, threads_per_process) = args
        log = logging.getLogger('stress-test-server')
        cancel_file = f"/tmp/stress_cancel_{workflow_id}" if workflow_id else None

        def _is_cancelled():
            return cancel_file and os.path.exists(cancel_file)

        link_dataset = None
        if create_link and dataset_id and link_base_url_full:
            try:
                pid = project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
                if pid:
                    project = dl.projects.get(project_id=pid)
                    link_dataset = project.datasets.get(dataset_id=dataset_id)
                else:
                    link_dataset = dl.datasets.get(dataset_id=dataset_id)
            except Exception as e:
                log.warning(f"Could not get dataset in worker: {e}")

        def download_one(url):
            try:
                if _is_cancelled():
                    return {'cancelled': True}
                filename = os.path.basename(url)
                filepath = os.path.join(storage_path, filename)
                file_existed = os.path.exists(filepath) and os.path.getsize(filepath) > 0
                if not file_existed:
                    response = requests.get(url, timeout=60)
                    response.raise_for_status()
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                # Ensure link item in dataset for every file (downloaded or already present) with correct URL (create or overwrite)
                if link_dataset is not None and link_base_url_full and not _is_cancelled():
                    try:
                        _upload_one_link_item_standalone(link_dataset, filename, link_base_url_full, overwrite=True)
                    except Exception as link_err:
                        log.warning(f"Ensure link item failed for {filename}: {link_err}")
                return {'success': True, 'filename': filename, 'path': str(filepath), 'skipped': file_existed}
            except Exception as e:
                return {'success': False, 'url': url, 'error': str(e)}

        downloaded, failed, skipped = [], [], []
        with ThreadPoolExecutor(max_workers=threads_per_process) as executor:
            futures = {executor.submit(download_one, url): url for url in chunk_urls}
            for future in as_completed(futures):
                if _is_cancelled():
                    break
                result = future.result()
                if result.get('cancelled'):
                    continue
                if result.get('success'):
                    if result.get('skipped'):
                        skipped.append(result)
                    else:
                        downloaded.append(result)
                else:
                    failed.append(result)
        return (downloaded, failed, skipped, None)
    except Exception as e:
        log = logging.getLogger('stress-test-server')
        log.exception("Worker chunk failed with exception")
        return ([], [], [], str(e))


class StressTestHandler(SimpleHTTPRequestHandler):
    """HTTP handler for stress test server"""
    
    def __init__(self, *args, **kwargs):
        # Don't use directory parameter - we'll handle paths manually
        super().__init__(*args, **kwargs)

    def log_message(self, format, *args):
        """Log requests at INFO so 200 responses are not captured as ERROR (base class logs to stderr and is often treated as ERROR by platforms)."""
        msg = format % args if args else format
        logger.info("%s - - [%s] %s", self.address_string(), self.log_date_time_string(), msg)

    def do_GET(self):
        """Handle GET requests. Wrapped in try/except so service never crashes (platform health checks keep working)."""
        path = ''
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            logger.info(f"GET request received: {path} (full path: {self.path})")
            # Handle /metrics first (before any path rewriting) so platform scrapers always get 200 and pod is not restarted
            if path == '/metrics' or path.rstrip('/') == '/metrics':
                self._serve_metrics()
                return
            original_path = path
            # Remove common prefixes that might be added by Dataloop/Tornado
            prefixes_to_remove = [
                '/api/v1/apps/nginx-stress-test-flows-env2/panels/stress-test-panel',
                '/api/v1/apps/nginx-stress-test-flows-env2/panels',
                '/panels/stress-test-panel',
                '/stress-test-panel'
            ]
            for prefix in prefixes_to_remove:
                if path.startswith(prefix + '/'):
                    path = path[len(prefix):]
                    logger.info(f"Removed prefix '{prefix}': {original_path} -> {path}")
                    break
                elif path == prefix:
                    path = '/'
                    logger.info(f"Matched exact prefix '{prefix}': {original_path} -> {path}")
                    break
            # API endpoints - check after prefix removal
            if path == '/api/health' or path.startswith('/api/health'):
                logger.info("Serving /api/health")
                self._send_json({'status': 'ok', 'service': 'stress-test-server'})
            elif path == '/api/project' or path.startswith('/api/project'):
                self._get_project_info()
            elif path == '/api/datasets' or path.startswith('/api/datasets'):
                self._list_datasets()
            elif path == '/api/pipelines' or path.startswith('/api/pipelines'):
                self._list_pipelines()
            elif path.startswith('/api/logs/'):
                execution_id = path.split('/')[-1]
                self._get_execution_logs(execution_id)
            elif path.startswith('/api/execution/'):
                execution_id = path.split('/')[-1]
                self._get_execution_status(execution_id)
            elif path.startswith('/api/workflow-progress/'):
                workflow_id = path.split('/api/workflow-progress/')[1].split('/')[0]
                self._get_workflow_progress(workflow_id)
            elif path == '/api/active-workflow' or path.startswith('/api/active-workflow'):
                self._get_active_workflow()
            elif path == '/api/gcs-integrations' or path.startswith('/api/gcs-integrations'):
                self._list_gcs_integrations()
            elif path == '/api/faas-proxy-drivers' or path.startswith('/api/faas-proxy-drivers'):
                self._list_faas_proxy_drivers()
            elif path == '/api/org-access' or path.startswith('/api/org-access'):
                self._check_org_access()
            elif path == '/api/nginx-rps' or path.startswith('/api/nginx-rps'):
                self._get_nginx_rps_metrics(self.path)
            elif path == '/api/pipeline-execution-stats' or path.startswith('/api/pipeline-execution-stats'):
                self._get_pipeline_execution_stats(self.path)
            else:
                if path == '/' or path == '':
                    path = '/index.html'
                    logger.info(f"Root path, serving index.html")
                logger.info(f"Serving static file: {path} (from original: {original_path})")
                self._serve_static_file(path)
        except Exception as e:
            logger.error(f"GET handler error: {e}", exc_info=True)
            if path == '/metrics' or (path and path.rstrip('/') == '/metrics'):
                self._serve_metrics()
            elif path == '/api/health' or (path and path.startswith('/api/health')):
                self._send_json({'status': 'error', 'service': 'stress-test-server'}, status=200)
            else:
                try:
                    self._send_error(500, str(e)[:200])
                except Exception:
                    pass
    
    def _serve_static_file(self, path):
        """Serve static files with proper path handling"""
        try:
            logger.info(f"_serve_static_file called with path: {path}")
            logger.info(f"STATIC_DIR: {STATIC_DIR}")
            logger.info(f"STATIC_DIR exists: {os.path.exists(STATIC_DIR)}")
            
            # Handle root path or index.html
            if path == '/' or path == '' or path == '/index.html':
                file_path = os.path.join(STATIC_DIR, 'index.html')
                logger.info(f"Root/index path, serving: {file_path}")
            else:
                # Remove leading slash
                clean_path = path.lstrip('/')
                
                # Remove any remaining prefixes (defensive - shouldn't be here after do_GET processing)
                prefixes_to_clean = ['stress-test-panel/', 'panels/stress-test-panel/']
                for prefix in prefixes_to_clean:
                    if clean_path.startswith(prefix):
                        clean_path = clean_path[len(prefix):]
                        logger.info(f"Removed prefix '{prefix}' from clean_path: {clean_path}")
                
                if clean_path == '':
                    clean_path = 'index.html'
                
                file_path = os.path.join(STATIC_DIR, clean_path)
                logger.info(f"Resolved file path: {path} -> {clean_path} -> {file_path}")
            
            # Normalize path to prevent directory traversal
            file_path = os.path.normpath(file_path)
            
            # Ensure it's within STATIC_DIR
            static_dir_norm = os.path.normpath(STATIC_DIR)
            if not file_path.startswith(static_dir_norm):
                logger.warning(f"Path traversal attempt: {path} -> {file_path}")
                self._send_error(403, 'Forbidden')
                return
            
            # Check if file exists
            if not os.path.exists(file_path):
                # Try with .html extension
                if not file_path.endswith('.html'):
                    html_path = file_path + '.html'
                    if os.path.exists(html_path):
                        file_path = html_path
                        logger.debug(f"Found file with .html extension: {html_path}")
                    else:
                        logger.warning(f"File not found: {path} (tried {file_path} and {html_path})")
                        self._send_error(404, f'File not found: {path}')
                        return
                else:
                    logger.warning(f"File not found: {path} -> {file_path}")
                    self._send_error(404, f'File not found: {path}')
                    return
            
            # Check if it's a directory, serve index.html
            if os.path.isdir(file_path):
                file_path = os.path.join(file_path, 'index.html')
                if not os.path.exists(file_path):
                    logger.warning(f"Directory index not found: {path} -> {file_path}")
                    self._send_error(404, f'Directory index not found: {path}')
                    return
            
            # Determine content type
            content_type = 'text/html'
            if file_path.endswith('.js'):
                content_type = 'application/javascript'
            elif file_path.endswith('.css'):
                content_type = 'text/css'
            elif file_path.endswith('.json'):
                content_type = 'application/json'
            elif file_path.endswith('.png'):
                content_type = 'image/png'
            elif file_path.endswith('.jpg') or file_path.endswith('.jpeg'):
                content_type = 'image/jpeg'
            elif file_path.endswith('.svg'):
                content_type = 'image/svg+xml'
            
            # Read and serve file
            logger.debug(f"Serving file: {file_path} (content-type: {content_type})")
            with open(file_path, 'rb') as f:
                content = f.read()
            
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(content)
            
        except Exception as e:
            logger.error(f"Error serving file {path}: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def do_POST(self):
        """Handle POST requests. Wrapped in try/except so service never crashes."""
        try:
            parsed = urlparse(self.path)
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self._send_error(400, 'Invalid JSON')
                return
            if parsed.path == '/api/execute':
                self._execute_service_function(data)
            elif parsed.path == '/api/run-full':
                self._run_full_workflow(data)
            elif parsed.path == '/api/create-faas-proxy-driver':
                self._create_faas_proxy_driver(data)
            elif parsed.path.startswith('/api/cancel-workflow/'):
                workflow_id = parsed.path.split('/api/cancel-workflow/')[1].split('/')[0]
                self._cancel_workflow(workflow_id)
            else:
                self._send_error(404, 'Not Found')
        except Exception as e:
            logger.error(f"POST handler error: {e}", exc_info=True)
            try:
                self._send_error(500, str(e)[:200])
            except Exception:
                pass
    
    def _send_json(self, data, status=200):
        """Send JSON response"""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _send_error(self, status, message):
        """Send error response"""
        self._send_json({'error': message}, status)
    
    def _get_project(self):
        """Get current project from environment"""
        # Try both PROJECT_ID and DL_PROJECT_ID (Dataloop uses PROJECT_ID)
        project_id = os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
        if not project_id:
            raise ValueError('PROJECT_ID or DL_PROJECT_ID not set')
        return dl.projects.get(project_id=project_id)
    
    def _get_project_info(self):
        """Get project information"""
        try:
            project = self._get_project()
            self._send_json({
                'id': project.id,
                'name': project.name,
                'org': project.org.get('id') if project.org else None
            })
        except Exception as e:
            self._send_error(500, str(e))
    
    def _list_datasets(self):
        """List datasets in project"""
        try:
            project = self._get_project()
            datasets = list(project.datasets.list().all())
            self._send_json({
                'datasets': [
                    {'id': ds.id, 'name': ds.name, 'items_count': ds.items_count}
                    for ds in datasets
                ]
            })
        except Exception as e:
            self._send_error(500, str(e))
    
    def _list_pipelines(self):
        """List pipelines in project"""
        try:
            project = self._get_project()
            pipelines = list(project.pipelines.list().all())
            self._send_json({
                'pipelines': [
                    {'id': p.id, 'name': p.name, 'status': p.status}
                    for p in pipelines
                ]
            })
        except Exception as e:
            self._send_error(500, str(e))
    
    def _list_gcs_integrations(self):
        """List all GCS integrations in the organization"""
        try:
            project = self._get_project()
            result = _stress_test_server_instance.list_gcs_integrations()
            self._send_json(result)
        except Exception as e:
            logger.error(f"Error listing GCS integrations: {e}", exc_info=True)
            self._send_json({'success': False, 'error': str(e), 'integrations': []})
    
    def _list_faas_proxy_drivers(self):
        """List all faasProxy drivers for the project"""
        try:
            project = self._get_project()
            result = _stress_test_server_instance.list_faas_proxy_drivers(project_id=project.id)
            self._send_json(result)
        except Exception as e:
            logger.error(f"Error listing faasProxy drivers: {e}", exc_info=True)
            self._send_json({'success': False, 'error': str(e), 'drivers': []})
    
    def _check_org_access(self):
        """Check if current identity has org-level permission"""
        try:
            result = _stress_test_server_instance.check_org_access()
            self._send_json(result)
        except Exception as e:
            logger.error(f"Error checking org access: {e}", exc_info=True)
            self._send_json({
                'success': False,
                'hasOrgPermission': False,
                'orgId': None,
                'orgName': None,
                'orgMembersUrl': None,
                'currentIdentity': None,
                'botUserName': None,
                'message': str(e)
            })
    
    def _get_nginx_rps_metrics(self, path):
        """Proxy Prometheus query_range for Nginx gateway RPS (2xx, 4xx, 5xx). GET /api/nginx-rps?start=&end=&step=15s (default: last 1h, step 15s)."""
        try:
            parsed = urlparse(path)
            qs = parse_qs(parsed.query or '')
            end_ts = int(time.time())
            range_sec = 3600  # 1 hour
            step_sec = 15
            if qs.get('range'):
                try:
                    range_sec = int(qs['range'][0])
                except (ValueError, IndexError):
                    pass
            if qs.get('step'):
                try:
                    step_sec = int(qs['step'][0].replace('s', '').replace('m', ''))
                    if 'm' in (qs['step'][0] or ''):
                        step_sec *= 60
                except (ValueError, IndexError, AttributeError):
                    pass
            start_ts = end_ts - range_sec
            step_str = f'{step_sec}s'
            queries = {
                '2xx': 'sum(rate(nginx_gateway_request_duration_seconds_count{status_class="2xx",job="kubernetes-service-endpoints"}[1m]))',
                '4xx': 'sum(rate(nginx_gateway_request_duration_seconds_count{status_class="4xx",job="kubernetes-service-endpoints"}[1m]))',
                '5xx': 'sum(rate(nginx_gateway_request_duration_seconds_count{status_class="5xx",job="kubernetes-service-endpoints"}[1m]))',
            }
            out = {'2xx': [], '4xx': [], '5xx': [], 'error': None}
            for status, expr in queries.items():
                try:
                    r = requests.get(
                        f'{PROMETHEUS_URL}/api/v1/query_range',
                        params={'query': expr, 'start': start_ts, 'end': end_ts, 'step': step_str},
                        timeout=10
                    )
                    r.raise_for_status()
                    data = r.json()
                    if data.get('status') != 'success' or data.get('data', {}).get('resultType') != 'matrix':
                        out[status] = []
                        continue
                    results = data.get('data', {}).get('result', [])
                    if not results:
                        out[status] = []
                        continue
                    values = results[0].get('values', [])
                    out[status] = [[int(t), float(v)] for t, v in values]
                except Exception as e:
                    logger.warning(f"Prometheus query for {status} failed: {e}")
                    out[status] = []
                    out['error'] = out['error'] or str(e)
            self._send_json(out)
        except Exception as e:
            logger.error(f"Nginx RPS metrics error: {e}", exc_info=True)
            self._send_json({'2xx': [], '4xx': [], '5xx': [], 'error': str(e)})
    
    def _get_pipeline_execution_stats(self, path):
        """GET /api/pipeline-execution-stats?pipeline_id=xxx OR ?pipeline_name=...&project_id=... - return execution counts (success, failed, in_progress), duration, and per-node stats."""
        def _send_err(msg):
            self._send_json({'error': msg, 'success': 0, 'failed': 0, 'in_progress': 0, 'nodes': [], 'duration_seconds': None})
        try:
            parsed = urlparse(path)
            qs = parse_qs(parsed.query or '')
            pipeline_id = (qs.get('pipeline_id') or [None])[0] if qs.get('pipeline_id') else None
            pipeline_name = (qs.get('pipeline_name') or [None])[0] if qs.get('pipeline_name') else None
            project_id = (qs.get('project_id') or [None])[0] if qs.get('project_id') else None
            project_name = (qs.get('project_name') or [None])[0] if qs.get('project_name') else None
            if pipeline_id:
                pipeline_id = pipeline_id.strip()
            if pipeline_name:
                pipeline_name = pipeline_name.strip()
            if project_id:
                project_id = project_id.strip()
            if project_name:
                project_name = project_name.strip()
            if not pipeline_id and (not pipeline_name or not (project_id or project_name or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID'))):
                _send_err('Provide pipeline_id or (pipeline_name and project_id or project_name, or set PROJECT_ID)')
                return
            if not pipeline_id and pipeline_name:
                try:
                    if project_id:
                        project = dl.projects.get(project_id=project_id)
                    elif project_name:
                        project = dl.projects.get(project_name=project_name)
                    else:
                        proj_id = os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
                        if not proj_id:
                            _send_err('project_id or project_name is required when using pipeline_name')
                            return
                        project = dl.projects.get(project_id=proj_id)
                    pipeline = project.pipelines.get(pipeline_name=pipeline_name)
                    pipeline_id = pipeline.id
                except Exception as e:
                    _send_err(f'Pipeline by name failed: {e}')
                    return
            headers = {'Authorization': f'Bearer {dl.token()}'}
            base_url = (dl.environment() or '').strip().rstrip('/')
            if not base_url:
                self._send_json({'error': 'Could not determine API base URL', 'success': 0, 'failed': 0, 'in_progress': 0, 'nodes': [], 'duration_seconds': None})
                return
            # 1) Get pipeline details for node list
            nodes_list = []
            try:
                pipe_response = requests.get(f"{base_url}/pipelines/{pipeline_id}", headers=headers, timeout=10)
                if pipe_response.status_code == 200:
                    pipe_data = pipe_response.json()
                    for n in pipe_data.get('nodes') or pipe_data.get('composition', {}).get('nodes') or []:
                        node_id = n.get('id') or n.get('nodeId') or ''
                        node_name = n.get('name') or n.get('displayName') or node_id or 'Unknown'
                        nodes_list.append({'node_id': node_id, 'node_name': node_name, 'success': 0, 'failed': 0, 'in_progress': 0})
            except Exception as e:
                logger.debug(f"Could not fetch pipeline nodes: {e}")
            # 2) Get pipeline statistics
            stats_response = requests.get(
                f"{base_url}/pipelines/{pipeline_id}/statistics",
                headers=headers,
                timeout=15
            )
            if stats_response.status_code != 200:
                err = stats_response.text[:200] if stats_response.text else f'HTTP {stats_response.status_code}'
                self._send_json({'error': err, 'success': 0, 'failed': 0, 'in_progress': 0, 'nodes': nodes_list, 'duration_seconds': None, 'pipeline_id': pipeline_id})
                return
            pipeline_stats = stats_response.json()
            execution_counters = pipeline_stats.get('pipelineExecutionCounters', [])
            success_count = 0
            failed_count = 0
            in_progress_count = 0
            for counter in execution_counters:
                status = (counter.get('status') or '').lower()
                count = int(counter.get('count', 0))
                if status == 'success':
                    success_count = count
                elif status == 'failed':
                    failed_count = count
                elif status in ('in-progress', 'inprogress', 'running', 'pending'):
                    in_progress_count = count
                # aborted/terminated are not lumped into failed; display shows only success, failed, in progress
            # Per-node counters: API uses nodeExecutionsCounters (with 's') and statusCount per node (see dtlpy PipelineStats/NodeCounters)
            node_counters = (
                pipeline_stats.get('nodeExecutionsCounters')
                or pipeline_stats.get('nodeExecutionCounters')
                or pipeline_stats.get('nodeCounters')
                or pipeline_stats.get('nodes')
                or []
            )
            if isinstance(node_counters, list) and node_counters:
                for nc in node_counters:
                    node_id = nc.get('nodeId') or nc.get('node_id') or nc.get('id') or ''
                    node_name = nc.get('nodeName') or nc.get('node_name') or nc.get('name') or node_id
                    ns, nf, np = 0, 0, 0
                    # SDK uses statusCount (list of {status, count}); also support counters/executionCounters
                    raw_counts = nc.get('statusCount') or nc.get('counters') or nc.get('executionCounters') or [nc]
                    for c in raw_counts:
                        st = (c.get('status') or '').lower()
                        cnt = int(c.get('count', 0))
                        if st == 'success':
                            ns = cnt
                        elif st == 'failed':
                            nf = cnt
                        elif st in ('in-progress', 'inprogress', 'running', 'pending'):
                            np += cnt
                        # aborted/terminated are not shown in Failed; only status "failed" is shown
                    # Match by node_id or by node_name (API may use different id format than GET /pipelines)
                    existing = next(
                        (n for n in nodes_list if (n.get('node_id') == node_id or (node_name and n.get('node_name') == node_name))),
                        None
                    )
                    if existing:
                        existing['success'] = ns
                        existing['failed'] = nf
                        existing['in_progress'] = np
                    else:
                        nodes_list.append({'node_id': node_id, 'node_name': node_name, 'success': ns, 'failed': nf, 'in_progress': np})
            completed = success_count + failed_count
            total = completed + in_progress_count
            # If API didn't return per-node counters, fill each node with pipeline-level totals so the UI can show counters; set per_node_available=False so the UI can label it as pipeline total.
            per_node_available = any((n.get('success') or n.get('failed') or n.get('in_progress')) for n in nodes_list)
            if nodes_list and not per_node_available:
                for n in nodes_list:
                    n['success'] = success_count
                    n['failed'] = failed_count
                    n['in_progress'] = in_progress_count
            # 3) Duration: first execution to last (use raw API so we get JSON dates reliably)
            duration_seconds = None
            def _parse_ts(v):
                if v is None:
                    return None
                if hasattr(v, 'timestamp'):
                    return v.timestamp()
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, str):
                    try:
                        import datetime
                        s = v.replace('Z', '+00:00').replace(' ', 'T')
                        dt = datetime.datetime.fromisoformat(s)
                        return dt.timestamp()
                    except Exception:
                        pass
                return None
            def _get_ts(obj, *keys):
                for k in keys:
                    v = getattr(obj, k, None) if hasattr(obj, k) else (obj.get(k) if isinstance(obj, dict) else None)
                    t = _parse_ts(v)
                    if t is not None:
                        return t
                return None
            project_id = os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
            if not project_id and pipe_response and pipe_response.status_code == 200:
                project_id = pipe_data.get('projectId') or pipe_data.get('project_id')
            if project_id:
                t0_raw, t1_raw = None, None
                # Pipeline runs are listed via POST /pipelines/query (pipeline executions), not project executions
                try:
                    query_url = f"{base_url}/pipelines/query"
                    body_first = {
                        "resource": "pipelineState",
                        "filter": {"$and": [{"pipelineId": pipeline_id}]},
                        "sort": {"createdAt": "ascending"},
                        "page": 0,
                        "pageSize": 1,
                    }
                    r_first = requests.post(query_url, headers=headers, json=body_first, timeout=15)
                    if r_first.status_code == 200:
                        data = r_first.json()
                        items = data.get("items") or data.get("data", {}).get("items") or []
                        if items and isinstance(items[0], dict):
                            t0_raw = _get_ts(items[0], "createdAt", "created_at")
                    body_last = {
                        "resource": "pipelineState",
                        "filter": {"$and": [{"pipelineId": pipeline_id}]},
                        "sort": {"createdAt": "descending"},
                        "page": 0,
                        "pageSize": 1,
                    }
                    r_last = requests.post(query_url, headers=headers, json=body_last, timeout=15)
                    if r_last.status_code == 200:
                        data = r_last.json()
                        items = data.get("items") or data.get("data", {}).get("items") or []
                        if items and isinstance(items[0], dict):
                            t1_raw = _get_ts(items[0], "updatedAt", "updated_at", "createdAt", "created_at")
                    if t0_raw is not None and t1_raw is not None:
                        duration_seconds = max(0, int(t1_raw - t0_raw))
                        logger.info(f"Pipeline {pipeline_id} duration: {duration_seconds}s (from pipelines/query)")
                except Exception as e:
                    logger.debug(f"Duration from pipelines/query: {e}")
                if duration_seconds is None:
                    try:
                        project = dl.projects.get(project_id=project_id)
                        pipeline = project.pipelines.get(pipeline_id=pipeline_id)
                        filters_asc = dl.Filters(resource=dl.FiltersResource.PIPELINE_EXECUTION)
                        filters_asc.add(field="pipelineId", values=pipeline_id)
                        filters_asc.sort_by(field="createdAt", value=dl.FiltersOrderByDirection.ASCENDING)
                        first_page = pipeline.pipeline_executions.list(filters=filters_asc, page_size=1)
                        filters_desc = dl.Filters(resource=dl.FiltersResource.PIPELINE_EXECUTION)
                        filters_desc.add(field="pipelineId", values=pipeline_id)
                        filters_desc.sort_by(field="createdAt", value=dl.FiltersOrderByDirection.DESCENDING)
                        last_page = pipeline.pipeline_executions.list(filters=filters_desc, page_size=1)
                        first_exec = first_page.items[0] if first_page.items else None
                        last_exec = last_page.items[0] if last_page.items else None
                        if first_exec and last_exec:
                            first_json = first_exec.to_json() if hasattr(first_exec, "to_json") else (getattr(first_exec, "_json", None) or first_exec)
                            last_json = last_exec.to_json() if hasattr(last_exec, "to_json") else (getattr(last_exec, "_json", None) or last_exec)
                            t0 = _get_ts(first_json or first_exec, "createdAt", "created_at")
                            t1 = _get_ts(last_json or last_exec, "updatedAt", "updated_at", "createdAt", "created_at")
                            if t0 is not None and t1 is not None:
                                duration_seconds = max(0, int(t1 - t0))
                                logger.info(f"Pipeline {pipeline_id} duration: {duration_seconds}s (from SDK pipeline_executions)")
                    except Exception as e:
                        logger.debug(f"Duration from SDK pipeline_executions: {e}")
            self._send_json({
                'pipeline_id': pipeline_id,
                'success': success_count,
                'failed': failed_count,
                'in_progress': in_progress_count,
                'completed': completed,
                'total': total if total > 0 else (success_count + failed_count + in_progress_count),
                'duration_seconds': duration_seconds,
                'nodes': nodes_list,
                'per_node_available': per_node_available,
                'error': None
            })
        except Exception as e:
            logger.error(f"Pipeline execution stats error: {e}", exc_info=True)
            self._send_json({
                'error': str(e),
                'success': 0,
                'failed': 0,
                'in_progress': 0,
                'completed': 0,
                'total': 0,
                'duration_seconds': None,
                'nodes': [],
                'pipeline_id': None
            })

    def _serve_metrics(self):
        """Serve GET /metrics with minimal Prometheus exposition format. Always return 200 so platform does not restart the pod."""
        try:
            body = "# HELP stress_test_server_up Server is up (1 = up).\n# TYPE stress_test_server_up gauge\nstress_test_server_up 1\n"
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Content-Length', str(len(body.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(body.encode('utf-8'))
        except Exception as e:
            logger.debug(f"Metrics endpoint error: {e}")
            try:
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(b"stress_test_server_up 1\n")
            except Exception:
                pass

    def _create_faas_proxy_driver(self, data):
        """Create a faasProxy storage driver"""
        try:
            project = self._get_project()
            project_id = project.id
            
            driver_name = data.get('driverName') or data.get('driver_name')
            integration_id = data.get('integrationId') or data.get('integration_id')
            integration_type = data.get('integrationType') or data.get('integration_type')
            bucket_name = data.get('bucketName') or data.get('bucket_name')
            path = data.get('path')
            allow_external_delete = data.get('allowExternalDelete', data.get('allow_external_delete', True))
            
            if not driver_name or not integration_id or not bucket_name:
                self._send_error(400, 'driverName, integrationId, and bucketName are required')
                return
            
            result = _stress_test_server_instance.create_faas_proxy_driver(
                project_id=project_id,
                driver_name=driver_name,
                integration_id=integration_id,
                integration_type=integration_type,
                bucket_name=bucket_name,
                path=path,
                allow_external_delete=allow_external_delete
            )
            self._send_json(result)
        except Exception as e:
            logger.error(f"Error creating faasProxy driver: {e}", exc_info=True)
            self._send_json({'success': False, 'error': str(e)})
    
    def _get_stress_test_service(self, project_id=None):
        """Get the stress-test-service and its project"""
        try:
            # Always need a project to get a service
            # If project_id not provided, try multiple sources
            if not project_id:
                # Try environment variables (Dataloop uses PROJECT_ID, not DL_PROJECT_ID)
                project_id = os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
                
                # Log all environment variables that might contain project ID
                env_vars = {k: v for k, v in os.environ.items() if 'PROJECT' in k or 'project' in k.lower()}
                logger.info(f"Environment variables with 'project': {env_vars}")
            
            if not project_id:
                logger.warning("Project ID not found in environment. Need project_id parameter.")
                raise ValueError("Project ID is required. Please provide project_id parameter or ensure PROJECT_ID/DL_PROJECT_ID environment variable is set.")
            
            project = dl.projects.get(project_id=project_id)
            logger.info(f"Getting service from project {project.id} ({project.name})")
            logger.info(f"Looking for service with ID '{STRESS_TEST_SERVICE_ID}' or name '{STRESS_TEST_SERVICE_NAME}'")
            
            # Try by ID first
            try:
                service = project.services.get(service_id=STRESS_TEST_SERVICE_ID)
                logger.info(f"Found service by ID: {service.id} ({service.name})")
                # Check package type - Dpk vs Package
                if hasattr(service, 'package'):
                    pkg_name = service.package.name if hasattr(service.package, 'name') else 'N/A'
                    pkg_revision = getattr(service.package, 'revision', getattr(service.package, 'version', 'N/A'))
                    logger.info(f"Service package: {pkg_name} (revision: {pkg_revision})")
                else:
                    logger.info(f"Service package: N/A")
                return service, project
            except Exception as e:
                logger.debug(f"Service not found by ID {STRESS_TEST_SERVICE_ID}: {e}")
            
            # Try by name - but filter out DPK services (they use the DPK package)
            # List all services with the name and find the one that's NOT the DPK service
            try:
                # Get all services with this name (there might be multiple)
                all_services = project.services.list()
                matching_services = [s for s in all_services if s.name == STRESS_TEST_SERVICE_NAME]
                
                if not matching_services:
                    raise Exception(f"No service found with name '{STRESS_TEST_SERVICE_NAME}'")
                
                logger.info(f"Found {len(matching_services)} service(s) with name '{STRESS_TEST_SERVICE_NAME}'")
                
                # Find the service that's NOT the DPK service
                service = None
                for s in matching_services:
                    if hasattr(s, 'package'):
                        pkg_name = s.package.name if hasattr(s.package, 'name') else 'N/A'
                        logger.info(f"  - Service {s.id} ({s.name}): package '{pkg_name}'")
                        
                        # Skip DPK services - they use the DPK package name or have the DPK service name
                        if pkg_name == DPK_PACKAGE_NAME or s.name == DPK_SERVICE_NAME:
                            logger.info(f"    Skipping DPK service {s.id} (uses DPK package or is DPK service)")
                            continue
                        
                        # This is the actual stress-test-service
                        service = s
                        break
                
                if not service:
                    logger.error(f"All services with name '{STRESS_TEST_SERVICE_NAME}' are DPK services!")
                    logger.error(f"Please deploy the separate 'stress-test-service' package (from stress-test-flows/service/)")
                    raise ValueError(f"Service '{STRESS_TEST_SERVICE_NAME}' not found. All services with this name are DPK services. Please deploy the separate 'stress-test-service' package.")
                
                logger.info(f"Found service by name: {service.id} ({service.name})")
                # Check package type - Dpk vs Package
                if hasattr(service, 'package'):
                    pkg_name = service.package.name if hasattr(service.package, 'name') else 'N/A'
                    pkg_revision = getattr(service.package, 'revision', getattr(service.package, 'version', 'N/A'))
                    logger.info(f"Service package: {pkg_name} (revision: {pkg_revision})")
                else:
                    logger.info(f"Service package: N/A")
                return service, project
            except ValueError:
                # Re-raise ValueError as-is (our custom error)
                raise
            except Exception as e:
                logger.error(f"Could not find stress-test-service in project {project_id}: {e}")
                # List all services in the project to help debug
                try:
                    all_services = project.services.list()
                    logger.info(f"Available services in project: {[s.name for s in all_services]}")
                except Exception as list_error:
                    logger.debug(f"Could not list services: {list_error}")
                raise ValueError(f"Stress-test-service '{STRESS_TEST_SERVICE_NAME}' not found in project {project_id}. Please deploy it first.")
        except ValueError:
            # Re-raise ValueError as-is
            raise
        except Exception as e:
            logger.error(f"Failed to get service: {e}", exc_info=True)
            raise ValueError(f"Failed to get stress-test-service: {str(e)}")
    
    def _execute_service_function(self, data):
        """
        Execute a function on the stress-test-service.
        
        Request body:
        {
            "service_id": "...",  # Optional, auto-detected if not provided
            "project_id": "...",  # Optional, uses DL_PROJECT_ID
            "function_name": "download_images",
            "execution_input": {...}
        }
        
        Note: Service ID is automatically detected - no need to provide it.
        """
        try:
            # Service and project are automatically detected
            # Project ID comes from request or environment variable
            project_id = data.get('project_id') or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
            function_name = data.get('function_name')
            execution_input = data.get('execution_input', {})
            
            if not function_name:
                self._send_error(400, 'function_name is required')
                return
            
            # Get service (requires project_id - will use environment variable if not provided)
            service, project = self._get_stress_test_service(project_id=project_id)
            logger.info(f"Executing {function_name} on service {service.id} ({service.name}) in project {project.id}")
            
            # Execute function
            execution = service.execute(
                function_name=function_name,
                execution_input=execution_input,
                project_id=project_id
            )
            
            logger.info(f"Execution started: {execution.id}")
            
            # Initialize log storage
            execution_logs[execution.id] = []
            execution_status[execution.id] = {
                'status': execution.status,
                'started_at': time.time()
            }
            
            # Start log polling in background
            Thread(target=self._poll_execution_logs, args=(execution.id, service, project_id), daemon=True).start()
            
            self._send_json({
                'success': True,
                'execution_id': execution.id,
                'status': execution.status,
                'message': f'Execution {execution.id} started'
            })
            
        except Exception as e:
            logger.error(f"Execute error: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def _run_full_workflow(self, data):
        """
        Run the full workflow using run_full_stress_test function.
        
        Request body (supports both camelCase and snake_case):
        {
            "maxImages": 50000,  # or "max_images"
            "datasetId": "...",  # or "dataset_id" (optional)
            "projectId": "...",  # or "project_id" (optional, auto-detected from service if not provided)
            "datasetName": "...",  # or "dataset_name" (optional, used if createDataset=true)
            "driverId": "...",  # or "driver_id" (mandatory)
            "pipelineName": "...",  # or "pipeline_name" (optional)
            "numWorkers": 50,  # or "num_workers"
            "pipelineConcurrency": 30,  # or "pipeline_concurrency" - default for both nodes when per-node not set
            "pipelineMaxReplicas": 12,  # or "pipeline_max_replicas" - default for both nodes when per-node not set
            "streamImageConcurrency": 30,  # or "stream_image_concurrency" - code node (stream-image)
            "streamImageMaxReplicas": 12,  # or "stream_image_max_replicas"
            "resnetConcurrency": 10,  # or "resnet_concurrency" - ResNet model node (default 10)
            "resnetMaxReplicas": 12,  # or "resnet_max_replicas"
            "cocoDataset": "all",  # or "coco_dataset" - "train2017", "val2017", or "all"
            "createDataset": false,  # or "create_dataset" - Create dataset if it doesn't exist
            "skipDownload": false,  # or "skip_download"
            "skipLinkItems": false,  # or "skip_link_items"
            "skipPipeline": false,  # or "skip_pipeline"
            "skipExecute": false,  # or "skip_execute"
            "linkBaseUrl": "...",  # or "link_base_url" (optional, base URL for link items)
        }
        
        Note: Service ID and Project ID are automatically detected - no need to provide them.
        """
        try:
            # Project ID comes from request or environment variable
            project_id = data.get('projectId') or data.get('project_id') or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID') or self.project_id
            
            logger.info(f"Running full stress test workflow (project_id: {project_id})")
            
            # Parse parameters - support both camelCase (from HTML) and snake_case (from API)
            max_images = int(data.get('maxImages') or data.get('max_images') or 50000)
            dataset_id_param = data.get('datasetId') or data.get('dataset_id')
            project_id_param = project_id
            dataset_name = data.get('datasetName') or data.get('dataset_name')
            driver_id = (data.get('driverId') or data.get('driver_id') or '').strip()
            if not driver_id:
                self._send_error(400, 'driverId is mandatory')
                return
            pipeline_name = data.get('pipelineName') or data.get('pipeline_name', 'stress-test-resnet-v11')
            num_workers = data.get('numWorkers') or data.get('num_workers', 50)
            pipeline_concurrency = data.get('pipelineConcurrency') or data.get('pipeline_concurrency', 30)
            pipeline_max_replicas = data.get('pipelineMaxReplicas') or data.get('pipeline_max_replicas', 12)
            stream_image_concurrency = data.get('streamImageConcurrency') or data.get('stream_image_concurrency') or pipeline_concurrency
            stream_image_max_replicas = data.get('streamImageMaxReplicas') or data.get('stream_image_max_replicas') or pipeline_max_replicas
            resnet_concurrency = data.get('resnetConcurrency') or data.get('resnet_concurrency') or 10
            resnet_max_replicas = data.get('resnetMaxReplicas') or data.get('resnet_max_replicas') or pipeline_max_replicas
            coco_dataset = data.get('cocoDataset') or data.get('coco_dataset', 'all')
            # When dataset field is empty, auto-create a dataset so we can create link items and run the pipeline
            create_dataset_param = data.get('createDataset') or data.get('create_dataset', False)
            if not dataset_id_param or (isinstance(dataset_id_param, str) and dataset_id_param.strip() == ''):
                create_dataset = True
                logger.info("Dataset ID is empty: will create a new dataset and upload link items to it")
            else:
                create_dataset = create_dataset_param
            skip_download = data.get('skipDownload') or data.get('skip_download', False)
            skip_link_items = data.get('skipLinkItems') or data.get('skip_link_items', False)
            skip_pipeline = data.get('skipPipeline') or data.get('skip_pipeline', False)
            skip_execute = data.get('skipExecute') or data.get('skip_execute', False)
            # Use Link Base URL from request only when provided; default only when missing or empty
            link_base_url = data.get('linkBaseUrl') or data.get('link_base_url')
            if isinstance(link_base_url, str):
                link_base_url = link_base_url.strip()
            if link_base_url is None or link_base_url == '':
                link_base_url = DEFAULT_LINK_BASE_URL
            logger.info(f"Workflow params: dataset_id={dataset_id_param!r}, create_dataset={create_dataset}, link_base_url={link_base_url!r}")
            
            # Generate unique workflow ID
            import uuid
            workflow_id = str(uuid.uuid4())
            
            # Initialize progress tracking
            workflow_progress[workflow_id] = {
                'status': 'starting',
                'current_step': None,
                'progress_pct': 0,
                'logs': [],
                'result': None,
                'error': None,
                'started_at': time.time()
            }
            
            # Run workflow in background thread for real-time progress
            logger.info(f"Starting workflow in background thread (workflow_id: {workflow_id})")
            Thread(target=self._run_workflow_async, args=(
                workflow_id,
                max_images,
                dataset_id_param,
                project_id_param,
                dataset_name,
                driver_id,
                pipeline_name,
                num_workers,
                coco_dataset,
                create_dataset,
                skip_download,
                skip_link_items,
                skip_pipeline,
                skip_execute,
                link_base_url,
                pipeline_concurrency,
                pipeline_max_replicas,
                stream_image_concurrency,
                stream_image_max_replicas,
                resnet_concurrency,
                resnet_max_replicas
            ), daemon=True).start()
            
            # Return immediately with workflow_id for polling
            self._send_json({
                'success': True,
                'workflow_id': workflow_id,
                'message': 'Workflow started. Use /api/workflow-progress/{workflow_id} to poll progress.'
            })
            
        except Exception as e:
            logger.error(f"Run full error: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def _get_workflow_progress(self, workflow_id):
        """Get progress for a workflow"""
        try:
            if workflow_id not in workflow_progress:
                self._send_error(404, f'Workflow {workflow_id} not found')
                return

            progress = workflow_progress[workflow_id].copy()
            # Calculate elapsed time
            if 'started_at' in progress:
                progress['elapsed_time'] = time.time() - progress['started_at']

            self._send_json(progress)
        except Exception as e:
            logger.error(f"Get workflow progress error: {e}", exc_info=True)
            self._send_error(500, str(e))

    def _get_active_workflow(self):
        """Return whether a test is currently running and its workflow_id so other users can attach and see logs."""
        try:
            global workflow_progress, _stress_test_server_instance
            # Prefer current workflow from server instance (the one actually running)
            wf_id = getattr(_stress_test_server_instance, '_current_workflow_id', None) if _stress_test_server_instance else None
            if wf_id and wf_id in workflow_progress:
                status = workflow_progress[wf_id].get('status', '')
                if status not in ('completed', 'failed', 'cancelled'):
                    self._send_json({'running': True, 'workflow_id': wf_id})
                    return
            # Else scan for any running/starting/cancelling workflow
            for wid, prog in workflow_progress.items():
                status = prog.get('status', '')
                if status in ('running', 'starting', 'cancelling'):
                    self._send_json({'running': True, 'workflow_id': wid})
                    return
            self._send_json({'running': False})
        except Exception as e:
            logger.error(f"Get active workflow error: {e}", exc_info=True)
            self._send_json({'running': False})

    def _run_workflow_async(self, workflow_id, max_images, dataset_id_param, project_id_param,
                           dataset_name, driver_id, pipeline_name, num_workers, coco_dataset,
                           create_dataset, skip_download, skip_link_items, skip_pipeline, skip_execute,
                           link_base_url, pipeline_concurrency, pipeline_max_replicas,
                           stream_image_concurrency, stream_image_max_replicas, resnet_concurrency, resnet_max_replicas):
        """Run workflow in background thread with progress updates"""
        try:
            def update_progress(step, status, progress_pct, message, result=None, error=None):
                """Helper to update progress"""
                if workflow_id not in workflow_progress:
                    workflow_progress[workflow_id] = {
                        'status': 'unknown',
                        'current_step': None,
                        'progress_pct': 0,
                        'logs': [],
                        'result': None,
                        'error': None,
                        'started_at': time.time()
                    }
                
                progress = workflow_progress[workflow_id]
                progress['current_step'] = step
                progress['status'] = status
                progress['progress_pct'] = progress_pct
                
                # Always append new log entries to ensure UI sees all updates
                # For batch execution, we'll append but limit to last 50 entries to avoid spam
                progress['logs'].append({
                    'timestamp': time.time(),
                    'step': step,
                    'level': 'INFO' if not error else 'ERROR',
                    'message': message
                })
                
                # Limit logs to last 1800 entries (~1–1.5 MB); frontend resyncs when trimmed
                if len(progress['logs']) > 1800:
                    progress['logs'] = progress['logs'][-1800:]
                
                if result:
                    progress['result'] = result
                if error:
                    progress['error'] = error
                    progress['status'] = 'failed'
                
                logger.info(f"[Workflow {workflow_id}] {step}: {status} ({progress_pct}%) - {message}")
            
            update_progress('initializing', 'running', 0, 'Starting workflow...')
            
            # Get server instance (handler doesn't have direct access)
            global _stress_test_server_instance
            if _stress_test_server_instance is None:
                raise RuntimeError("StressTestServer instance not available")
            
            server = _stress_test_server_instance
            
            # Store workflow_id in server instance for pipeline_id tracking
            server._current_workflow_id = workflow_id
            
            # Call run_full_stress_test with progress callback
            result = server.run_full_stress_test(
                max_images=max_images,
                dataset_id=dataset_id_param,
                project_id=project_id_param,
                link_base_url=link_base_url,
                dataset_name=dataset_name,
                driver_id=driver_id,
                pipeline_name=pipeline_name,
                num_workers=num_workers,
                pipeline_concurrency=pipeline_concurrency,
                pipeline_max_replicas=pipeline_max_replicas,
                stream_image_concurrency=stream_image_concurrency,
                stream_image_max_replicas=stream_image_max_replicas,
                resnet_concurrency=resnet_concurrency,
                resnet_max_replicas=resnet_max_replicas,
                coco_dataset=coco_dataset,
                create_dataset=create_dataset,
                skip_download=skip_download,
                skip_link_items=skip_link_items,
                skip_pipeline=skip_pipeline,
                skip_execute=skip_execute,
                progress_callback=update_progress
            )
            
            # Clear workflow_id after completion
            server._current_workflow_id = None
            
            if result.get('success'):
                update_progress('complete', 'completed', 100, 'Workflow completed successfully', result=result)
            else:
                update_progress('complete', 'failed', 100, f"Workflow failed: {result.get('error', 'Unknown error')}", error=result.get('error'))
                
        except Exception as e:
            logger.error(f"Workflow async error: {e}", exc_info=True)
            # Clear workflow_id on error
            if _stress_test_server_instance:
                _stress_test_server_instance._current_workflow_id = None
            if workflow_id in workflow_progress:
                workflow_progress[workflow_id]['status'] = 'failed'
                workflow_progress[workflow_id]['error'] = str(e)
                workflow_progress[workflow_id]['logs'].append({
                    'timestamp': time.time(),
                    'step': 'error',
                    'level': 'ERROR',
                    'message': f'Workflow failed: {str(e)}'
                })
    
    def _get_workflow_progress(self, workflow_id):
        """Get progress for a workflow"""
        try:
            if workflow_id not in workflow_progress:
                self._send_error(404, f'Workflow {workflow_id} not found')
                return
            
            progress = workflow_progress[workflow_id].copy()
            # Calculate elapsed time
            if 'started_at' in progress:
                progress['elapsed_time'] = time.time() - progress['started_at']
            
            self._send_json(progress)
        except Exception as e:
            logger.error(f"Get workflow progress error: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def _cancel_workflow(self, workflow_id):
        """Cancel a running workflow by uninstalling and deleting the pipeline"""
        try:
            if workflow_id not in workflow_progress:
                self._send_error(404, f'Workflow {workflow_id} not found')
                return
            
            progress = workflow_progress[workflow_id]
            
            # Check if workflow is already completed or failed
            if progress.get('status') in ['completed', 'failed', 'cancelled']:
                self._send_json({
                    'success': False,
                    'message': f'Workflow is already {progress.get("status")} and cannot be cancelled'
                })
                return
            
            # Mark as cancelling immediately
            progress['status'] = 'cancelling'
            progress['current_step'] = 'cancel'
            progress['logs'].append({
                'timestamp': time.time(),
                'step': 'cancel',
                'level': 'INFO',
                'message': 'Cancellation requested - uninstalling and deleting pipeline...'
            })
            logger.info(f"Workflow {workflow_id} marked as cancelling")
            # Signal worker processes (they check this file; no shared memory)
            try:
                open(f"/tmp/stress_cancel_{workflow_id}", "w").close()
            except Exception:
                pass
            
            # Get pipeline_id from result
            pipeline_id = None
            result = progress.get('result')
            if result and isinstance(result, dict):
                # Try to get from result directly
                pipeline_id = result.get('pipeline_id')
                if not pipeline_id:
                    # Try to get from steps
                    steps = result.get('steps', [])
                    if steps and isinstance(steps, list):
                        for step in steps:
                            if step and isinstance(step, dict) and step.get('step') == 'create_pipeline':
                                step_result = step.get('result', {})
                                if step_result and isinstance(step_result, dict):
                                    pipeline_id = step_result.get('pipeline_id')
                                    break
            
            if not pipeline_id:
                # Try to get from current step result
                if progress.get('current_step') == 'create_pipeline':
                    # Pipeline might be in progress, try to get from logs or wait
                    logger.warning(f"Pipeline ID not found in workflow {workflow_id} result")
                    progress['status'] = 'cancelled'
                    progress['logs'].append({
                        'timestamp': time.time(),
                        'step': 'cancel',
                        'level': 'WARNING',
                        'message': 'Pipeline ID not found - workflow cancelled but pipeline may need manual cleanup'
                    })
                    self._send_json({
                        'success': True,
                        'message': 'Workflow cancelled (pipeline ID not found - may need manual cleanup)'
                    })
                    return
            
            logger.info(f"Cancelling workflow {workflow_id} - uninstalling and deleting pipeline {pipeline_id}")
            
            # Uninstall and delete pipeline
            try:
                # Get project first (needed for proper pipeline access)
                result = progress.get('result')
                project_id = None
                if result and isinstance(result, dict):
                    project_id = result.get('project_id')
                if not project_id:
                    project_id = os.environ.get('DL_PROJECT_ID') or os.environ.get('PROJECT_ID')
                if not project_id:
                    # Try to get pipeline to extract project
                    try:
                        pipeline = dl.pipelines.get(pipeline_id=pipeline_id)
                        project = pipeline.project
                        project_id = project.id
                    except:
                        # Fallback: try to get from workflow progress
                        logger.warning(f"Could not get project from pipeline {pipeline_id}, trying direct delete")
                        project = None
                else:
                    project = dl.projects.get(project_id=project_id)
                
                # Get pipeline from project context (more reliable)
                if project:
                    try:
                        pipeline = project.pipelines.get(pipeline_id=pipeline_id)
                    except dl.exceptions.NotFound:
                        # Fallback to global get
                        pipeline = dl.pipelines.get(pipeline_id=pipeline_id)
                else:
                    pipeline = dl.pipelines.get(pipeline_id=pipeline_id)
                
                # Try to uninstall pipeline first (optional - skip if it fails)
                uninstall_success = False
                try:
                    logger.info(f"Uninstalling pipeline {pipeline_id}...")
                    # Try uninstall with project context if available
                    if hasattr(pipeline, 'uninstall'):
                        try:
                            pipeline.uninstall()
                            uninstall_success = True
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'INFO',
                                'message': f'Pipeline {pipeline_id} uninstalled successfully'
                            })
                        except Exception as uninstall_err:
                            # If uninstall fails with identifier error, it's likely not installed or doesn't support uninstall
                            error_str = str(uninstall_err)
                            if 'identifier' in error_str.lower() or 'inputs' in error_str.lower():
                                logger.info(f"Pipeline {pipeline_id} may not be installed or uninstall not supported, skipping uninstall")
                                progress['logs'].append({
                                    'timestamp': time.time(),
                                    'step': 'cancel',
                                    'level': 'INFO',
                                    'message': f'Skipping uninstall (pipeline may not be installed)'
                                })
                            else:
                                raise
                except Exception as uninstall_error:
                    logger.warning(f"Failed to uninstall pipeline {pipeline_id}: {uninstall_error}")
                    progress['logs'].append({
                        'timestamp': time.time(),
                        'step': 'cancel',
                        'level': 'WARNING',
                        'message': f'Uninstall failed (non-critical): {str(uninstall_error)}'
                    })
                    # Continue with delete even if uninstall fails
                
                # Pause the pipeline itself before deleting
                try:
                    logger.info(f"Pausing pipeline {pipeline_id}...")
                    progress['logs'].append({
                        'timestamp': time.time(),
                        'step': 'cancel',
                        'level': 'INFO',
                        'message': f'Pausing pipeline...'
                    })
                    
                    # Pause the pipeline
                    if hasattr(pipeline, 'pause'):
                        try:
                            pipeline.pause()
                            logger.info(f"Pipeline {pipeline_id} paused successfully")
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'INFO',
                                'message': f'Pipeline paused successfully'
                            })
                            # Wait a moment for pipeline to pause
                            time.sleep(2)  # Give pipeline time to pause
                        except Exception as pause_err:
                            logger.warning(f"Pipeline pause() method failed: {pause_err}")
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'WARNING',
                                'message': f'Could not pause pipeline (will try to delete anyway): {str(pause_err)}'
                            })
                    else:
                        logger.warning(f"Pipeline {pipeline_id} does not have pause() method")
                        progress['logs'].append({
                            'timestamp': time.time(),
                            'step': 'cancel',
                            'level': 'WARNING',
                            'message': 'Pipeline does not support pause (will try to delete anyway)'
                        })
                        
                except Exception as pause_error:
                    logger.warning(f"Failed to pause pipeline {pipeline_id}: {pause_error}")
                    progress['logs'].append({
                        'timestamp': time.time(),
                        'step': 'cancel',
                        'level': 'WARNING',
                        'message': f'Could not pause pipeline (will try to delete anyway): {str(pause_error)}'
                    })
                    # Continue with delete even if pause fails
                    # Continue with delete attempt anyway
                
                # Delete pipeline (always try this, even if uninstall or stop failed)
                try:
                    logger.info(f"Deleting pipeline {pipeline_id}...")
                    pipeline.delete()
                    progress['logs'].append({
                        'timestamp': time.time(),
                        'step': 'cancel',
                        'level': 'INFO',
                        'message': f'Pipeline {pipeline_id} deleted successfully'
                    })
                except Exception as delete_error:
                    error_str = str(delete_error)
                    # Check if it's the "unfinished execution" error
                    if 'unfinished execution' in error_str.lower() or 'still running' in error_str.lower():
                        logger.warning(f"Pipeline still has running executions, retrying after longer wait...")
                        progress['logs'].append({
                            'timestamp': time.time(),
                            'step': 'cancel',
                            'level': 'WARNING',
                            'message': 'Pipeline still has running executions, waiting 5 seconds...'
                        })
                        time.sleep(5)  # Wait longer for executions to finish
                        # Try delete again
                        try:
                            pipeline.delete()
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'INFO',
                                'message': f'Pipeline {pipeline_id} deleted successfully (after retry)'
                            })
                        except Exception as retry_error:
                            logger.error(f"Failed to delete pipeline after retry: {retry_error}")
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'ERROR',
                                'message': f'Failed to delete pipeline: {str(retry_error)}. You may need to manually stop executions and delete the pipeline.'
                            })
                            raise
                    else:
                        error_str = str(delete_error)
                        # Check if error is about package deletion (non-critical if pipeline is deleted)
                        if 'package' in error_str.lower() or 'Composition failed' in error_str or 'upsert packages' in error_str.lower():
                            logger.warning(f"Pipeline {pipeline_id} deleted but package cleanup failed (non-critical): {delete_error}")
                            progress['logs'].append({
                                'timestamp': time.time(),
                                'step': 'cancel',
                                'level': 'WARNING',
                                'message': f'Pipeline deleted successfully, but package cleanup failed (non-critical): {error_str[:150]}'
                            })
                            # Don't raise - pipeline is deleted, package cleanup failure is not critical
                            # Verify pipeline was actually deleted
                            try:
                                dl.pipelines.get(pipeline_id=pipeline_id)
                                # Pipeline still exists - this might be a real error, but log as warning anyway
                                logger.warning(f"Pipeline {pipeline_id} may still exist despite package error")
                            except dl.exceptions.NotFound:
                                # Pipeline was deleted - package cleanup error is non-critical
                                logger.info(f"Pipeline {pipeline_id} confirmed deleted (package cleanup error is non-critical)")
                        else:
                            # Real deletion error - check if pipeline still exists
                            try:
                                # Try to verify if pipeline was actually deleted
                                dl.pipelines.get(pipeline_id=pipeline_id)
                                # Pipeline still exists - this is a real error
                                logger.error(f"Failed to delete pipeline {pipeline_id}: {delete_error}")
                                progress['logs'].append({
                                    'timestamp': time.time(),
                                    'step': 'cancel',
                                    'level': 'ERROR',
                                    'message': f'Failed to delete pipeline: {str(delete_error)}'
                                })
                                raise
                            except dl.exceptions.NotFound:
                                # Pipeline was actually deleted despite the error - likely cleanup issue
                                logger.warning(f"Pipeline {pipeline_id} was deleted but error occurred during cleanup: {delete_error}")
                                progress['logs'].append({
                                    'timestamp': time.time(),
                                    'step': 'cancel',
                                    'level': 'WARNING',
                                    'message': f'Pipeline deleted successfully, but cleanup error occurred: {error_str[:150]}'
                                })
                                # Don't raise - pipeline is deleted
                            except Exception as verify_error:
                                # Can't verify - treat as real error
                                logger.error(f"Failed to delete pipeline {pipeline_id}: {delete_error}")
                                progress['logs'].append({
                                    'timestamp': time.time(),
                                    'step': 'cancel',
                                    'level': 'ERROR',
                                    'message': f'Failed to delete pipeline: {str(delete_error)}'
                                })
                                raise
            
            except dl.exceptions.NotFound:
                logger.warning(f"Pipeline {pipeline_id} not found - may have been already deleted")
                progress['logs'].append({
                    'timestamp': time.time(),
                    'step': 'cancel',
                    'level': 'INFO',
                    'message': f'Pipeline {pipeline_id} not found (may have been already deleted)'
                })
            except Exception as e:
                logger.error(f"Error cancelling pipeline {pipeline_id}: {e}", exc_info=True)
                progress['logs'].append({
                    'timestamp': time.time(),
                    'step': 'cancel',
                    'level': 'ERROR',
                    'message': f'Error during cancellation: {str(e)}'
                })
            
            # Mark workflow as cancelled
            progress['status'] = 'cancelled'
            progress['progress_pct'] = 0
            progress['current_step'] = 'cancelled'
            progress['logs'].append({
                'timestamp': time.time(),
                'step': 'cancel',
                'level': 'INFO',
                'message': 'Workflow cancelled successfully - pipeline uninstalled and deleted'
            })
            
            logger.info(f"Workflow {workflow_id} cancelled successfully")
            
            self._send_json({
                'success': True,
                'message': 'Workflow cancelled successfully - pipeline uninstalled and deleted',
                'pipeline_id': pipeline_id if pipeline_id else None
            })
            
        except Exception as e:
            logger.error(f"Cancel workflow error: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def _get_execution_logs(self, execution_id):
        """Get logs for an execution"""
        try:
            if execution_id not in execution_logs:
                self._send_json({'logs': [], 'status': 'unknown'})
                return
            
            logs = execution_logs[execution_id]
            status_info = execution_status.get(execution_id, {'status': 'unknown'})
            
            self._send_json({
                'logs': logs,
                'status': status_info.get('status', 'unknown')
            })
            
        except Exception as e:
            logger.error(f"Get logs error: {e}", exc_info=True)
            self._send_error(500, str(e))
    
    def _get_execution_status(self, execution_id):
        """Get execution status"""
        try:
            status_info = execution_status.get(execution_id, {'status': 'unknown'})
            self._send_json(status_info)
        except Exception as e:
            self._send_error(500, str(e))
    
    def _poll_execution_logs(self, execution_id, service, project_id):
        """
        Poll execution logs in background thread.
        """
        try:
            logger.info(f"Starting log polling for execution {execution_id}")
            
            max_polls = 600  # Poll for up to 10 minutes (600 * 1 second)
            poll_count = 0
            
            while poll_count < max_polls:
                try:
                    # Get execution status
                    execution = service.executions.get(execution_id=execution_id)
                    current_status = execution.status.lower()
                    
                    # Update status
                    if execution_id in execution_status:
                        execution_status[execution_id]['status'] = current_status
                    
                    if current_status in ['completed', 'failed', 'success']:
                        logger.info(f"Execution {execution_id} finished with status: {current_status}")
                        break
                    
                    # Get logs from execution
                    try:
                        logs = service.logs.list(execution_id=execution_id)
                        if logs:
                            for log_entry in logs:
                                log_data = {
                                    'timestamp': log_entry.get('timestamp', time.time()),
                                    'level': log_entry.get('level', 'INFO'),
                                    'message': log_entry.get('message', ''),
                                    'function_name': log_entry.get('function_name', '')
                                }
                                
                                # Avoid duplicates
                                if log_data not in execution_logs[execution_id]:
                                    execution_logs[execution_id].append(log_data)
                    except Exception as log_error:
                        logger.debug(f"Could not fetch logs yet: {log_error}")
                    
                    time.sleep(1)  # Poll every second
                    poll_count += 1
                    
                except Exception as e:
                    logger.warning(f"Poll error: {e}")
                    time.sleep(2)
                    poll_count += 1
            
            if poll_count >= max_polls:
                logger.warning(f"Log polling timeout for execution {execution_id}")
                
        except Exception as e:
            logger.error(f"Log polling error: {e}", exc_info=True)


class StressTestServer(dl.BaseServiceRunner):
    """Dataloop service runner for stress test server - combines UI and execution"""
    
    def __init__(self, 
                 storage_base_path: str = None,
                 link_base_url: str = None,
                 dataset_id: str = None):
        super().__init__()
        self.port = int(os.environ.get('PORT', 3000))
        self.server = None
        self.server_thread = None
        
        # Get link_base_url from parameter or default
        link_base_url_provided = link_base_url or DEFAULT_LINK_BASE_URL
        
        # Check if LINK_ITEM_URL_OVERRIDE env var exists and derive storage_base_path
        link_override = os.environ.get('LINK_ITEM_URL_OVERRIDE')
        if link_base_url_provided and link_override:
            try:
                # Parse LINK_ITEM_URL_OVERRIDE: "http://34.140.193.179/s,file://s"
                # Format: "source_url,target_url"
                parts = link_override.split(',')
                if len(parts) == 2:
                    source_url = parts[0].strip()  # "http://34.140.193.179/s"
                    target_url = parts[1].strip()  # "file://s"
                    
                    # Replace the entire source_url in link_base_url with target_url to derive storage path
                    # Example: "http://34.140.193.179/s/pd_datfs2/stress-test" -> "file://s/pd_datfs2/stress-test"
                    if source_url in link_base_url_provided:
                        storage_path = link_base_url_provided.replace(source_url, target_url)
                        # Remove "file:/" (one slash, not "file://") to get the base path
                        # "file://s/pd_datfs2/stress-test" -> "s/pd_datfs2/stress-test"
                        storage_path = storage_path.replace('file:/', '')
                        # Ensure it starts with /
                        if not storage_path.startswith('/'):
                            storage_path = '/' + storage_path
                        # Use this as storage_base_path
                        if storage_base_path is None:
                            storage_base_path = storage_path
                            logger.info(f"Derived storage_base_path from LINK_ITEM_URL_OVERRIDE: {storage_base_path}")
                            logger.info(f"  Link Base URL (preserved): {link_base_url_provided}")
                            logger.info(f"  LINK_ITEM_URL_OVERRIDE: {link_override}")
                            logger.info(f"  Source URL: {source_url}")
                            logger.info(f"  Target URL: {target_url}")
                            
                            # IMPORTANT: link_base_url_provided should keep /s (it's the HTTP URL)
                            # Only storage_path is derived. Verify link_base_url_provided has /s
                            if '/s' not in link_base_url_provided:
                                logger.warning(f"  WARNING: link_base_url_provided missing '/s': {link_base_url_provided}")
                                # If storage path has /s, ensure link URL also has it
                                if storage_path.startswith('/s/'):
                                    # Extract path after /s from storage_path
                                    path_after_s = storage_path[3:]  # Remove '/s/'
                                    # Reconstruct link_base_url with source_url (which includes /s)
                                    link_base_url_provided = f"{source_url}/{path_after_s}"
                                    logger.info(f"  Reconstructed link_base_url to include /s: {link_base_url_provided}")
                            else:
                                logger.info(f"  Verified link_base_url_provided includes /s: {link_base_url_provided}")
            except Exception as e:
                logger.warning(f"Failed to parse LINK_ITEM_URL_OVERRIDE: {e}, using default storage_base_path", exc_info=True)
        
        # Stress test service properties
        self.storage_base_path = storage_base_path or DEFAULT_STORAGE_BASE_PATH
        self.link_base_url_base = link_base_url_provided
        self.default_dataset_id = dataset_id or DEFAULT_DATASET_ID
        
        # No date in storage/link paths: reuse existing downloads from any day
        self.date_str = datetime.now().strftime('%Y-%m-%d')
        self.storage_path = self.storage_base_path
        self.link_base_url = self.link_base_url_base
        
        # Get project_id from environment variable (set by Dataloop)
        self.project_id = os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
        if self.project_id:
            logger.info(f"Project ID from environment: {self.project_id}")
            # Set it in both environment variables for handlers to use (for consistency)
            os.environ['PROJECT_ID'] = self.project_id
            os.environ['DL_PROJECT_ID'] = self.project_id
        else:
            logger.warning("Project ID not found in environment variables")
            logger.warning(f"Available env vars with 'project': {[k for k in os.environ.keys() if 'project' in k.lower()]}")
        
        logger.info(f"StressTestServer initialized")
        logger.info(f"Storage base path: {self.storage_base_path}")
        logger.info(f"Storage path: {self.storage_path}")
        logger.info(f"Link base URL: {self.link_base_url}")
        logger.info(f"Default dataset ID: {self.default_dataset_id}")
        
        # Store global reference for handler to access
        global _stress_test_server_instance
        _stress_test_server_instance = self
        
        # Check if this is actually a custom server
        is_custom_server = os.environ.get('IS_CUSTOM_SERVER', 'false').lower() == 'true'
        logger.info(f"IS_CUSTOM_SERVER environment variable: {os.environ.get('IS_CUSTOM_SERVER', 'not set')}")
        logger.info(f"is_custom_server flag: {is_custom_server}")
        
        # Only start server if IS_CUSTOM_SERVER is true
        if is_custom_server:
            logger.info("IS_CUSTOM_SERVER is true - starting server in __init__")
            self._start_server()
        else:
            logger.warning("IS_CUSTOM_SERVER is false - NOT starting server")
            logger.warning("This means the agent runner will start its own external server")
            logger.warning("Check dataloop.json config - isCustomServer should be in module.config")
    
    def _start_server(self):
        """Start the HTTP server in a background thread"""
        logger.info(f"Starting stress test server on port {self.port}")
        logger.info(f"Static files directory: {STATIC_DIR}")
        logger.info(f"Static directory exists: {os.path.exists(STATIC_DIR)}")
        
        if os.path.exists(STATIC_DIR):
            files = os.listdir(STATIC_DIR)
            logger.info(f"Files in static directory: {files}")
        else:
            logger.warning(f"Static directory does not exist! Creating it...")
            os.makedirs(STATIC_DIR, exist_ok=True)
        
        
        try:
            self.server = HTTPServer(('0.0.0.0', self.port), StressTestHandler)
            logger.info(f"HTTPServer created successfully on port {self.port}")
        except OSError as e:
            if e.errno == 98:  # Address already in use
                logger.error(f"Port {self.port} is already in use - cannot start server")
                logger.error("This usually means IS_CUSTOM_SERVER is false and agent runner is using the port")
                logger.error("Check dataloop.json - isCustomServer should be in module.config")
                logger.error("Service will continue but server will not be available")
                # Don't raise - let the service start even if server can't bind
                # The service can still handle function calls
                self.server = None
                return
            else:
                logger.error(f"Failed to create HTTPServer: {e}", exc_info=True)
                self.server = None
                return  # Don't raise, let service continue
        except Exception as e:
            logger.error(f"Failed to create HTTPServer: {e}", exc_info=True)
            self.server = None
            return  # Don't raise, let service continue
        
        logger.info(f"Server started at http://0.0.0.0:{self.port}")
        logger.info(f"Server is listening on port {self.port}")
        logger.info("Available endpoints:")
        logger.info("  GET  /                        - Serve HTML documentation")
        logger.info("  GET  /api/health              - Health check")
        logger.info("  GET  /api/project             - Get project info")
        logger.info("  GET  /api/datasets            - List datasets")
        logger.info("  GET  /api/pipelines           - List pipelines")
        logger.info("  POST /api/execute             - Execute service function")
        logger.info("  POST /api/run-full            - Run full workflow (returns workflow_id)")
        logger.info("  GET  /api/workflow-progress/<workflow_id> - Get workflow progress (real-time)")
        logger.info("  GET  /api/nginx-rps - Nginx RPS metrics from Prometheus (2xx, 4xx, 5xx)")
        logger.info("  GET  /api/pipeline-execution-stats?pipeline_id= - Pipeline execution counts (success, failed, in progress)")
        logger.info("  GET  /api/logs/<execution_id> - Get execution logs")
        logger.info("  GET  /api/execution/<execution_id> - Get execution status")
        
        # Verify server is ready
        logger.info("=" * 60)
        logger.info("SERVER READY - Listening for requests on port %s", self.port)
        logger.info("=" * 60)
        
        # Test if port is available (just for logging, don't fail if in use)
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('0.0.0.0', self.port))
            sock.close()
            logger.info(f"Port {self.port} is available")
        except OSError as e:
            logger.warning(f"Port {self.port} appears to be in use: {e}")
            logger.warning("This might be because agent-app is using port 3000")
            logger.warning("Will attempt to start server anyway - if it fails, check IS_CUSTOM_SERVER")
            # Don't raise - let the actual server startup fail if port is truly in use
        
        # Only start server thread if server was created successfully
        if self.server is None:
            logger.error("Server was not created - cannot start server thread")
            return
        
        # Start server in background thread so it doesn't block
        # For custom servers, we need to start immediately
        def run_server():
            try:
                logger.info("Calling server.serve_forever() - this will block...")
                self.server.serve_forever()
            except Exception as e:
                logger.error(f"Server crashed: {e}", exc_info=True)
                # Don't raise in thread - just log the error
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        logger.info("Server thread started - server is now running in background")
    
    # ========== Stress Test Execution Methods ==========
    
    def create_dataset(self, project_id: str = None, dataset_name: str = None, driver_id: str = None) -> dict:
        """
        Create a dataset for stress testing if it doesn't exist.
        
        Args:
            project_id: Project ID (if None, uses project from default_dataset_id)
            dataset_name: Dataset name (default: 'stress-test-dataset-{date}')
            driver_id: Driver ID for the dataset (mandatory)
        
        Returns:
            dict with dataset info
        """
        if dataset_name is None:
            dataset_name = f'stress-test-dataset-{self.date_str}'
        
        if not driver_id or (isinstance(driver_id, str) and not driver_id.strip()):
            return {'error': 'driver_id is mandatory'}
        
        driver_id = driver_id.strip() if isinstance(driver_id, str) else driver_id
        logger.info(f"Creating dataset: {dataset_name} with driver_id: {driver_id}")
        
        # Get project
        if project_id is None:
            # Try to get project from default dataset if it exists
            try:
                default_dataset = dl.datasets.get(dataset_id=self.default_dataset_id)
                project = default_dataset.project
                project_id = project.id
                logger.info(f"Using project from default dataset: {project.name} (ID: {project_id})")
            except:
                # If default dataset doesn't exist, we need project_id
                logger.error("Cannot determine project_id. Please provide project_id parameter.")
                return {'error': 'project_id is required if default_dataset_id does not exist'}
        else:
            project = dl.projects.get(project_id=project_id)
            logger.info(f"Using provided project: {project.name} (ID: {project_id})")
        
        # Check if dataset already exists
        # Note: We only reuse existing datasets if explicitly requested (when dataset_id was provided)
        # When creating a new dataset (dataset_id was empty), we always create fresh
        try:
            existing_dataset = project.datasets.get(dataset_name=dataset_name)
            logger.info(f"Dataset already exists: {existing_dataset.id}")
            # Check if dataset is empty - if not, we should create a new one with unique name
            try:
                filters = dl.Filters(resource=dl.FiltersResource.ITEM)
                filters.add(field='hidden', values=False)
                filters.add(field='type', values='file')
                existing_items_count = existing_dataset.items.list(filters=filters).items_count
                if existing_items_count > 0:
                    # Dataset has items, create a new one with unique name instead
                    logger.info(f"Existing dataset has {existing_items_count} items - creating new dataset with unique name")
                    import time as time_module
                    unique_suffix = time_module.strftime('%Y%m%d-%H%M%S')
                    dataset_name = f"{dataset_name}-{unique_suffix}"
                    logger.info(f"Using unique dataset name: {dataset_name}")
                else:
                    # Dataset is empty, can reuse it
                    logger.info(f"Existing dataset is empty - reusing it")
                    return {
                        'dataset_id': existing_dataset.id,
                        'dataset_name': existing_dataset.name,
                        'project_id': project_id,
                        'driver_id': driver_id,
                        'created': False,
                        'message': 'Dataset already exists (empty)'
                    }
            except Exception as check_error:
                logger.warning(f"Could not check dataset items: {check_error}, will create new dataset")
        except dl.exceptions.NotFound:
            logger.info(f"Dataset does not exist, creating: {dataset_name}")
        
        # Create dataset with driver_id
        try:
            # Try using SDK with driver_id parameter
            # If that doesn't work, fall back to direct API call
            try:
                dataset = project.datasets.create(
                    dataset_name=dataset_name,
                    driver_id=driver_id
                )
            except (TypeError, AttributeError) as e:
                # SDK might not support driver_id directly, use API
                logger.info(f"SDK doesn't support driver_id parameter, using API directly: {e}")
                headers = {'Authorization': f'Bearer {dl.token()}'}
                base_url = dl.environment()
                
                payload = {
                    'name': dataset_name,
                    'projects': [project_id],
                    'driverId': driver_id,
                    'createDefaultRecipe': True
                }
                
                response = requests.post(
                    f"{base_url}/datasets",
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                dataset_data = response.json()
                
                # Get the dataset object from SDK
                dataset = project.datasets.get(dataset_id=dataset_data['id'])
            logger.info(f"Created dataset: {dataset.id} with driver_id: {driver_id}")
            return {
                'dataset_id': dataset.id,
                'dataset_name': dataset.name,
                'project_id': project_id,
                'driver_id': driver_id,
                'created': True,
                'message': 'Dataset created successfully'
            }
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            return {
                'error': f'Failed to create dataset: {str(e)}',
                'project_id': project_id,
                'dataset_name': dataset_name,
                'driver_id': driver_id
            }
    
    def check_org_access(self) -> dict:
        """
        Check if the current service/user has organization-level permission
        (e.g. can list integrations). Used by the UI to show full driver form
        only when the bot/user has org access.
        
        Returns:
            dict with xhasOrgPermission, orgId, orgName, currentIdentity, botUserName, message
        """
        try:
            project = dl.projects.get(project_id=self.project_id)
            # Get org_id and org_name from project ONLY (no org API call yet - we may not have org access)
            org_id = None
            org_name = None
            if isinstance(project.org, dict):
                org_id = project.org.get('id')
                org_name = (project.org.get('name') or project.org.get('title') or '') if org_id else ''
            else:
                try:
                    org_id = getattr(project.org, 'id', None)
                    org_name = getattr(project.org, 'name', None) or getattr(project.org, 'title', None) or ''
                except Exception:
                    pass
            if not org_id and hasattr(project, 'to_json'):
                try:
                    j = project.to_json() or {}
                    org_id = j.get('orgId') or (j.get('org') or {}).get('id') if isinstance(j.get('org'), dict) else j.get('org')
                    if isinstance(org_id, dict):
                        org_id = org_id.get('id')
                except Exception:
                    pass
            if not org_id:
                return {
                    'success': True,
                    'hasOrgPermission': False,
                    'orgId': None,
                    'orgName': None,
                    'orgMembersUrl': None,
                    'currentIdentity': None,
                    'botUserName': None,
                    'message': 'Could not get organization ID from project'
                }
            if not org_name:
                org_name = org_id

            # Build org members URL for "add bot to org" link — use gate url from client_api with /api/v1 stripped
            try:
                env_url = (dl.client_api.environments[dl.client_api.environment].get('url') or '').strip()
            except (KeyError, TypeError):
                env_url = ''
            base_url = env_url.replace('/api/v1', '').rstrip('/') if env_url else ''
            org_members_url = f'{base_url or "https://console.dataloop.ai"}/iam/{org_id}/members'

            # Current identity (bot or user email) - project-level only
            current_identity = None
            try:
                current_identity = project._client_api.info().get('user_email') or project._client_api.info().get('email') or project._client_api.info().get('username')
            except Exception:
                pass

            # Service bot username (for "add this bot to org" message) - project-level only, no org access needed
            # How we get it: 1) SERVICE_ID env -> get service -> read botUserName from entity/JSON
            #                2) Fallback: list services, find by package name "nginx-stress-test"
            #                3) From entity: attributes bot_user_name, botUserName, bot_username; or JSON keys; or nested bot.username
            #                4) Direct API GET /services/{id} and scan raw JSON for any bot-related key
            bot_user_name = None
            service_obj = None
            try:
                service_id = (
                    os.environ.get('SERVICE_ID') or
                    os.environ.get('DL_SERVICE_ID') or
                    os.environ.get('DTLPY_SERVICE_ID')
                )
                logger.info(f"botUserName lookup: SERVICE_ID env={service_id!r}, PROJECT_ID={self.project_id!r}")
                if service_id:
                    try:
                        service_obj = project.services.get(service_id=service_id)
                        logger.info(f"botUserName lookup: got service by id {service_id}")
                    except Exception as e:
                        logger.warning(f"botUserName lookup: project.services.get(service_id) failed: {e}")
                if not service_obj:
                    # Match "this" DPK app service: app.dpkName is "nginx-stress-test", or moduleName is "stress-test-server"
                    dpk_name = os.environ.get('PACKAGE_NAME') or 'nginx-stress-test'
                    module_name = 'stress-test-server'
                    logger.info(f"botUserName lookup: fallback listing services for dpk_name={dpk_name!r} or moduleName={module_name!r}")
                    try:
                        for svc in project.services.list():
                            j = None
                            if hasattr(svc, 'to_json'):
                                try:
                                    j = svc.to_json() or {}
                                except Exception:
                                    pass
                            if not isinstance(j, dict) and hasattr(svc, '_json'):
                                j = getattr(svc, '_json', None) or {}
                            if not isinstance(j, dict):
                                continue
                            # API returns app.dpkName and moduleName (not package.name)
                            app_dpk = (j.get('app') or {}) if isinstance(j.get('app'), dict) else {}
                            if app_dpk.get('dpkName') == dpk_name or j.get('moduleName') == module_name:
                                service_obj = svc
                                logger.info(f"botUserName lookup: found service by app.dpkName/moduleName, id={j.get('id') or getattr(svc, 'id', None)}")
                                break
                            # Legacy: package.name
                            pkg_name = None
                            if hasattr(svc, 'package') and svc.package:
                                pkg_name = getattr(svc.package, 'name', None)
                            if not pkg_name and isinstance(j, dict):
                                pkg = j.get('package') or j.get('packageId')
                                if isinstance(pkg, dict):
                                    pkg_name = pkg.get('name')
                            if pkg_name == dpk_name:
                                service_obj = svc
                                logger.info(f"botUserName lookup: found service by package name, id={getattr(svc, 'id', None)}")
                                break
                    except Exception as e:
                        logger.warning(f"botUserName lookup: list services failed: {e}")
                if service_obj:
                    def _from_json(j):
                        if not isinstance(j, dict):
                            return None
                        v = j.get('botUserName') or j.get('bot_user_name') or j.get('bot_username')
                        if not v and isinstance(j.get('bot'), dict):
                            v = j['bot'].get('username') or j['bot'].get('userName') or j['bot'].get('email')
                        return v
                    bot_user_name = (
                        getattr(service_obj, 'bot_user_name', None) or
                        getattr(service_obj, 'botUserName', None) or
                        getattr(service_obj, 'bot_username', None)
                    )
                    if not bot_user_name:
                        if isinstance(service_obj, dict):
                            bot_user_name = _from_json(service_obj)
                        elif hasattr(service_obj, 'to_json'):
                            try:
                                bot_user_name = _from_json(service_obj.to_json() or {})
                            except Exception:
                                pass
                        if not bot_user_name and hasattr(service_obj, '_json'):
                            try:
                                bot_user_name = _from_json(getattr(service_obj, '_json', {}) or {})
                            except Exception:
                                pass
                    if bot_user_name:
                        logger.info(f"Resolved service botUserName: {bot_user_name}")
                    else:
                        _j = getattr(service_obj, '_json', None) if service_obj else None
                        _keys = list(_j.keys()) if isinstance(_j, dict) else 'N/A'
                        logger.warning(f"Service entity has no botUserName. Service id={getattr(service_obj, 'id', None)}, keys={_keys}")
                # Direct API: GET service and scan raw JSON for any bot-related key (in case SDK strips it)
                if not bot_user_name and (service_id or (service_obj and getattr(service_obj, 'id', None))):
                    sid = service_id or getattr(service_obj, 'id', None)
                    try:
                        success, raw = project._client_api.gen_request(req_type='get', path=f'/services/{sid}')
                        if success and isinstance(raw, dict):
                            for key in ('botUserName', 'bot_user_name', 'bot_username', 'botUsername'):
                                if raw.get(key):
                                    bot_user_name = raw.get(key)
                                    logger.info(f"Resolved service botUserName from API key {key!r}: {bot_user_name}")
                                    break
                            if not bot_user_name and isinstance(raw.get('bot'), dict):
                                b = raw['bot']
                                bot_user_name = b.get('username') or b.get('userName') or b.get('email')
                                if bot_user_name:
                                    logger.info(f"Resolved service botUserName from API bot.*: {bot_user_name}")
                            if not bot_user_name:
                                logger.warning(f"API service response keys (no bot field found): {list(raw.keys())}")
                    except Exception as e:
                        logger.warning(f"Direct API GET /services/... failed: {e}")
            except Exception as e:
                logger.warning(f"Could not get service botUserName: {e}", exc_info=True)

            # Only now try to access organization (may fail if no org permission)
            try:
                organization = dl.organizations.get(organization_id=org_id)
                organization.integrations.list()
                return {
                    'success': True,
                    'hasOrgPermission': True,
                    'orgId': org_id,
                    'orgName': org_name or org_id,
                    'orgMembersUrl': org_members_url,
                    'currentIdentity': current_identity,
                    'botUserName': bot_user_name,
                    'message': None
                }
            except Exception as e:
                logger.info(f"Org access failed (no org permission): {e}")
                return {
                    'success': True,
                    'hasOrgPermission': False,
                    'orgId': org_id,
                    'orgName': org_name or org_id,
                    'orgMembersUrl': org_members_url,
                    'currentIdentity': current_identity,
                    'botUserName': bot_user_name,
                    'message': str(e)
                }
        except Exception as e:
            logger.error(f"Error checking org access: {e}", exc_info=True)
            return {
                'success': False,
                'hasOrgPermission': False,
                'orgId': None,
                'orgName': None,
                'orgMembersUrl': None,
                'currentIdentity': None,
                'botUserName': None,
                'message': str(e)
            }
    
    def list_gcs_integrations(self, organization_id: str = None) -> dict:
        """
        List all GCS integrations in the organization.
        
        Args:
            organization_id: Organization ID (if None, uses project's organization)
        
        Returns:
            dict with list of GCS integrations
        """
        try:
            if organization_id is None:
                project = dl.projects.get(project_id=self.project_id)
                # Handle both dict and SDK object for project.org
                if isinstance(project.org, dict):
                    org_id = project.org.get('id')
                    if not org_id:
                        raise ValueError("Could not get organization ID from project")
                    organization = dl.organizations.get(organization_id=org_id)
                else:
                    organization = project.org
            else:
                organization = dl.organizations.get(organization_id=organization_id)
            
            gcs_integrations = []
            all_integrations = organization.integrations.list()
            
            for integration in all_integrations:
                # Check if it's a GCS integration
                integration_type = None
                if hasattr(integration, 'type'):
                    integration_type = integration.type
                elif hasattr(integration, 'integrations_type'):
                    integration_type = integration.integrations_type
                elif isinstance(integration, dict):
                    integration_type = integration.get('type')
                else:
                    integration_type = getattr(integration, 'type', None)
                
                # Get name and ID
                if isinstance(integration, dict):
                    integration_name = integration.get('name', 'Unknown')
                    integration_id = integration.get('id', 'Unknown')
                else:
                    integration_name = getattr(integration, 'name', 'Unknown')
                    integration_id = getattr(integration, 'id', 'Unknown')
                
                # Check if it matches GCS type (including gcp-workload-identity-federation)
                is_gcs = False
                if integration_type:
                    type_str = str(integration_type).lower().strip()
                    if (integration_type == dl.ExternalStorage.GCS or 
                        type_str == 'gcs' or
                        type_str == 'gcp-workload-identity-federation' or
                        'gcs' in type_str or 'google' in type_str or 'workload-identity' in type_str):
                        is_gcs = True
                else:
                    # Fallback: check name
                    name_lower = integration_name.lower()
                    if 'gcs' in name_lower or 'google' in name_lower or 'workload-identity' in name_lower:
                        is_gcs = True
                
                if is_gcs:
                    gcs_integrations.append({
                        'id': integration_id,
                        'name': integration_name,
                        'type': str(integration_type) if integration_type else 'gcs'
                    })
            
            return {
                'success': True,
                'integrations': gcs_integrations,
                'count': len(gcs_integrations)
            }
        except Exception as e:
            logger.error(f"Error listing GCS integrations: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'integrations': []
            }
    
    def list_faas_proxy_drivers(self, project_id: str = None) -> dict:
        """
        List all faasProxy drivers for a project.
        
        Args:
            project_id: Project ID (if None, uses self.project_id)
        
        Returns:
            dict with list of faasProxy drivers
        """
        try:
            if project_id is None:
                project_id = self.project_id
            project = dl.projects.get(project_id=project_id)
            
            faas_proxy_drivers = []
            all_drivers = project.drivers.list()
            
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
            
            return {
                'success': True,
                'drivers': faas_proxy_drivers,
                'count': len(faas_proxy_drivers)
            }
        except Exception as e:
            logger.error(f"Error listing faasProxy drivers: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'drivers': []
            }
    
    def create_faas_proxy_driver(self, project_id: str = None, driver_name: str = None,
                                 integration_id: str = None, integration_type: str = None,
                                 bucket_name: str = None, path: str = None, 
                                 allow_external_delete: bool = True) -> dict:
        """
        Create a faasProxy storage driver.
        
        Args:
            project_id: Project ID (if None, uses self.project_id)
            driver_name: Driver name
            integration_id: GCS integration ID
            bucket_name: GCS bucket name
            path: Optional path within bucket
            allow_external_delete: Allow external delete
        
        Returns:
            dict with driver info
        """
        try:
            if project_id is None:
                project_id = self.project_id
            project = dl.projects.get(project_id=project_id)
            
            # Get integration - handle both dict and SDK object for project.org
            if isinstance(project.org, dict):
                org_id = project.org.get('id')
                if not org_id:
                    return {
                        'success': False,
                        'error': 'Could not get organization ID from project'
                    }
                organization = dl.organizations.get(organization_id=org_id)
            else:
                organization = project.org
            
            # If integration_type is provided (manual mode), use it directly
            # Otherwise, try to look up the integration to get its type
            integration_id_val = integration_id
            integration_type_obj = None
            
            if integration_type:
                # Use provided integration type (from manual input)
                integration_type_obj = integration_type
            else:
                # Try to look up the integration to get its type
                try:
                    all_integrations = organization.integrations.list()
                    integration = None
                    
                    for int_obj in all_integrations:
                        int_id = int_obj.get('id') if isinstance(int_obj, dict) else getattr(int_obj, 'id', None)
                        if int_id == integration_id:
                            integration = int_obj
                            break
                    
                    if integration:
                        # Get integration type from the integration object
                        if isinstance(integration, dict):
                            integration_type_obj = integration.get('type')
                        else:
                            if hasattr(integration, 'type'):
                                integration_type_obj = integration.type
                            elif hasattr(integration, 'integrations_type'):
                                integration_type_obj = integration.integrations_type
                except Exception as e:
                    logger.warning(f"Could not look up integration type: {e}, using default 'gcs'")
            
            # Default to 'gcs' if we couldn't determine the type
            if integration_type_obj is None:
                integration_type_obj = 'gcs'
            
            # Check if driver already exists
            existing_drivers = self.list_faas_proxy_drivers(project_id=project_id)
            if existing_drivers.get('success'):
                for driver_info in existing_drivers.get('drivers', []):
                    if driver_info['name'] == driver_name:
                        return {
                            'success': True,
                            'driver': driver_info,
                            'exists': True,
                            'message': 'Driver already exists'
                        }
            
            # Convert integration_type to string
            if isinstance(integration_type_obj, str):
                integration_type_str = integration_type_obj
            elif hasattr(integration_type_obj, 'value'):
                integration_type_str = integration_type_obj.value
            elif integration_type_obj == dl.ExternalStorage.GCS:
                integration_type_str = 'gcs'
            else:
                integration_type_str = str(integration_type_obj).lower()
            
            # Get organization ID - handle both dict and SDK object
            if isinstance(organization, dict):
                org_id = organization.get('id')
            else:
                org_id = organization.id
            
            if not org_id:
                return {
                    'success': False,
                    'error': 'Could not determine organization ID'
                }
            
            # Build payload
            payload = {
                "integrationId": integration_id_val,
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
            
            # Create driver using direct API call (requests.post so we always get response body on error)
            base_url = (dl.environment() or '').strip().rstrip('/')
            if not base_url:
                try:
                    base_url = dl.client_api.environment
                except (KeyError, TypeError):
                    pass
            api_url = f"{base_url}/drivers" if base_url else None
            if not api_url or not api_url.startswith('http'):
                return {'success': False, 'error': 'Could not determine API base URL', 'message': 'Could not determine API base URL'}
            headers = {'Authorization': f'Bearer {dl.token()}', 'Content-Type': 'application/json'}
            resp = requests.post(api_url, json=payload, headers=headers, timeout=60)
            if not resp.ok:
                # Always have response body from requests
                resp_dict = {}
                try:
                    resp_dict = resp.json()
                except Exception:
                    if resp.text:
                        resp_dict = {'message': resp.text}
                err_list = resp_dict.get('errors')
                if isinstance(err_list, list) and err_list:
                    parts = []
                    for e in err_list:
                        if isinstance(e, str):
                            parts.append(e)
                        elif isinstance(e, dict):
                            parts.append(e.get('message') or e.get('error') or str(e))
                        else:
                            parts.append(str(e))
                    error_str = ', '.join(parts)
                else:
                    error_str = (
                        resp_dict.get('error') or
                        resp_dict.get('message') or
                        (str(err_list) if err_list is not None else None) or
                        resp.text or f'HTTP {resp.status_code}'
                    )
                error_str = error_str if isinstance(error_str, str) else str(error_str)
                return {
                    'success': False,
                    'error': error_str,
                    'message': error_str
                }
            try:
                driver_data = resp.json()
            except Exception:
                driver_data = {}
            
            driver_id = driver_data.get('id') or driver_data.get('driverId')
            driver_name_result = driver_data.get('name', driver_name)
            
            if not driver_id:
                return {
                    'success': False,
                    'error': f'Driver created but no ID returned: {driver_data}'
                }
            
            return {
                'success': True,
                'driver': {
                    'id': driver_id,
                    'name': driver_name_result,
                    'type': 'faasProxy',
                    'bucket': bucket_name,
                    'path': path
                },
                'exists': False,
                'message': 'Driver created successfully'
            }
        except Exception as e:
            logger.error(f"Error creating faasProxy driver: {e}", exc_info=True)
            err_msg = str(e)
            # Try to extract API error from exception (response body, args, or str(e))
            try:
                if hasattr(e, 'response') and getattr(e, 'response', None) is not None:
                    r = e.response
                    if hasattr(r, 'json') and callable(getattr(r, 'json')):
                        body = r.json()
                        if isinstance(body, dict):
                            err_msg = body.get('error') or body.get('message') or err_msg
                    if err_msg == str(e) and hasattr(r, 'text') and r.text:
                        try:
                            body = json.loads(r.text)
                            if isinstance(body, dict):
                                err_msg = body.get('error') or body.get('message') or err_msg
                        except Exception:
                            pass
                if err_msg == str(e) and getattr(e, 'args', None) and len(e.args) > 0:
                    first = e.args[0]
                    if isinstance(first, dict):
                        err_msg = first.get('error') or first.get('message') or err_msg
                    elif isinstance(first, str) and first.strip().startswith('{'):
                        try:
                            parsed = json.loads(first)
                            if isinstance(parsed, dict):
                                err_msg = parsed.get('error') or parsed.get('message') or err_msg
                        except Exception:
                            pass
                # Dataloop SDK embeds API response in exception: [Response <404>][Reason: Not Found][Text: {"message":"...",...}]
                if err_msg == str(e):
                    s = str(e)
                    # Find JSON after "Text: " (may be object or array with one object)
                    text_prefix = 'Text: '
                    idx = s.find(text_prefix)
                    if idx >= 0:
                        rest = s[idx + len(text_prefix):].strip()
                        start = rest.find('{')
                        if start >= 0:
                            depth = 0
                            end = -1
                            for i, c in enumerate(rest[start:], start=start):
                                if c == '{':
                                    depth += 1
                                elif c == '}':
                                    depth -= 1
                                    if depth == 0:
                                        end = i
                                        break
                            if end >= 0:
                                try:
                                    parsed = json.loads(rest[start:end + 1])
                                    if isinstance(parsed, dict):
                                        err_msg = parsed.get('error') or parsed.get('message') or err_msg
                                    elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                                        err_msg = parsed[0].get('error') or parsed[0].get('message') or err_msg
                                except Exception:
                                    pass
                # Fallback: extract "message":"...\" value from exception string (handles nested JSON)
                if err_msg == str(e):
                    s = str(e)
                    key = '"message":"'
                    idx = s.find(key)
                    if idx >= 0:
                        start = idx + len(key)
                        i = start
                        end = start
                        while i < len(s):
                            if s[i] == '\\' and i + 1 < len(s):
                                i += 2
                                continue
                            if s[i] == '"':
                                end = i
                                break
                            i += 1
                        if end > start:
                            extracted = s[start:end].replace('\\"', '"').replace('\\n', '\n').strip()
                            if extracted:
                                err_msg = extracted
                if err_msg == str(e) and str(e).strip().startswith('{'):
                    try:
                        parsed = json.loads(str(e))
                        if isinstance(parsed, dict):
                            err_msg = parsed.get('error') or parsed.get('message') or err_msg
                    except Exception:
                        pass
            except Exception:
                pass
            return {
                'success': False,
                'error': err_msg,
                'message': err_msg
            }
    
    def download_images(self, image_urls: list = None, max_images: int = 50000, num_workers: int = 50, dataset: str = 'all', progress_callback=None,
                        create_link_on_download: bool = True, dataset_id: str = None, link_base_url: str = None, project_id: str = None, workflow_id: str = None) -> dict:
        """
        Download images from COCO to NFS storage.
        
        Args:
            image_urls: List of image URLs to download (default: COCO train+val)
            max_images: Maximum number of images to download (default: 50000)
            num_workers: Number of parallel download workers (default: 50)
            dataset: 'train2017' (118k), 'val2017' (5k), or 'all' (123k)
            create_link_on_download: If True (default), create link item in dataset as soon as each image is downloaded (faster). Set False to only download; pass dataset_id and link_base_url when True.
            dataset_id: Required when create_link_on_download=True; dataset to add link items to.
            link_base_url: Required when create_link_on_download=True; base URL for link items.
            project_id: Required when create_link_on_download=True for dataset access.
            workflow_id: If set, download stops when workflow is cancelled (status 'cancelling'); no new link items are created after cancel.
        
        Returns:
            dict with download results
        """
        # Get COCO image URLs if not provided (may take several minutes if annotations must be downloaded)
        if image_urls is None:
            if progress_callback:
                try:
                    progress_callback('download_images', 'running', 15, 'Preparing COCO image list (first run may download ~250MB annotations)...')
                except Exception:
                    pass
            logger.info(f"Fetching COCO image list (dataset={dataset}, max={max_images})...")
            all_urls = get_coco_images(dataset=dataset, progress_callback=progress_callback)
            requested = int(max_images)
            available = len(all_urls)
            if available < requested:
                logger.warning(f"Requested {requested} images but only {available} COCO URLs available - will use {available}")
            max_images = min(requested, available)
            image_urls = all_urls[:max_images]
            logger.info(f"Will download {len(image_urls)} images (out of {len(all_urls)} available)")
        
        total_images = len(image_urls)
        # Use storage path from Link Base URL when provided (no date dir: reuse existing downloads)
        link_base_for_storage = link_base_url or (self.link_base_url if (create_link_on_download and dataset_id) else None)
        storage_path = _storage_path_from_link_base_url(link_base_for_storage) if link_base_for_storage else None
        if storage_path is None:
            storage_path = self.storage_path
        logger.info(f"Starting download of {total_images} images to {storage_path}")
        # Resolve link_base_url for workers (each process will get dataset by id; no shared memory)
        link_base_url_full = None
        if create_link_on_download and dataset_id and link_base_url:
            link_base_url_full = (link_base_url or self.link_base_url or DEFAULT_LINK_BASE_URL).strip().rstrip('/')
        create_link = bool(create_link_on_download and dataset_id and link_base_url_full)
        pid = project_id or self.project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
        num_threads = num_workers if num_workers and num_workers > 0 else DOWNLOAD_THREADS_PER_PROCESS

        # Multiprocessing requires a picklable worker from an importable module (download_worker.py).
        # If that module is not available (e.g. not in deployed package), fall back to threads.
        try:
            from download_worker import run_download_chunk as _run_download_chunk
            _use_process_pool = True
        except ModuleNotFoundError:
            _run_download_chunk = None
            _use_process_pool = False
            logger.info("download_worker not found: using threads only (commit download_worker.py for multiprocessing)")

        if _use_process_pool:
            logger.info(f"Using process pool: chunks of {DOWNLOAD_CHUNK_SIZE}, {num_threads} threads per process, max {DOWNLOAD_MAX_PROCESSES} processes")
            if create_link:
                logger.info("Create-link-on-download enabled: each process will create link items after download")
        else:
            logger.info(f"Using {num_threads} threads (single process)")
            if create_link:
                logger.info("Create-link-on-download enabled: will create link item after each download")

        # Create directory
        os.makedirs(storage_path, exist_ok=True)

        downloaded = []
        failed = []
        skipped = []
        cancelled_count = 0

        def _is_cancelled():
            return workflow_id and workflow_progress.get(workflow_id, {}).get('status') == 'cancelling'

        if _use_process_pool and _run_download_chunk:
            # Process pool: chunks of 1000, each process runs N threads (picklable worker from download_worker)
            chunks = [image_urls[i:i + DOWNLOAD_CHUNK_SIZE] for i in range(0, len(image_urls), DOWNLOAD_CHUNK_SIZE)]
            max_processes = min(DOWNLOAD_MAX_PROCESSES, len(chunks))
            # Use a manager queue so workers can report per-item progress (picklable with spawn)
            manager = mp.Manager()
            progress_queue = manager.Queue()
            worker_args = [
                (chunk, storage_path, link_base_url_full, dataset_id, pid, workflow_id, create_link, num_threads, progress_queue)
                for chunk in chunks
            ]
            try:
                process_context = mp.get_context('fork')
            except (ValueError, AttributeError):
                process_context = mp.get_context('spawn')
            executor = ProcessPoolExecutor(max_workers=max_processes, mp_context=process_context)
            completed = 0
            progress_done = threading.Event()

            def _progress_reader():
                nonlocal completed
                count = 0
                while True:
                    try:
                        n = progress_queue.get()
                    except Exception:
                        break
                    if n is None:
                        break
                    count += n
                    if count % 50 == 0 or count >= total_images:
                        progress_pct = (count / total_images) * 100 if total_images else 0
                        progress_msg = f"Progress: {count}/{total_images} ({progress_pct:.1f}%)"
                        logger.info(progress_msg)
                        if progress_callback:
                            overall_progress = 15 + (progress_pct / 100) * 25
                            progress_callback('download_images', 'running', overall_progress, progress_msg)
                progress_done.set()

            reader_thread = Thread(target=_progress_reader, daemon=False)
            reader_thread.start()
            try:
                future_to_chunk = {executor.submit(_run_download_chunk, a): a for a in worker_args}
                for future in as_completed(future_to_chunk):
                    if _is_cancelled():
                        cancelled_count = total_images - completed
                        logger.info(f"Workflow cancelled - stopping download after {completed}/{total_images}")
                        break
                    try:
                        result = future.result()
                        d, f, s, chunk_error = result[0], result[1], result[2], (result[3] if len(result) > 3 else None)
                        if chunk_error:
                            logger.error(f"Chunk failed (partial results merged): {chunk_error}")
                        downloaded.extend(d)
                        failed.extend(f)
                        skipped.extend(s)
                        completed += len(d) + len(f) + len(s)
                    except Exception as e:
                        logger.exception(f"Chunk failed: {e}")
                        completed += len(future_to_chunk[future][0])
                    # Chunk-level update with exact counts (reader thread handles smooth per-item progress)
                    progress_pct = (completed / total_images) * 100 if total_images else 0
                    progress_msg = f"Progress: {completed}/{total_images} ({progress_pct:.1f}%) - New: {len(downloaded)}, Skipped: {len(skipped)}, Failed: {len(failed)}"
                    logger.info(progress_msg)
                    if progress_callback:
                        overall_progress = 15 + (progress_pct / 100) * 25
                        progress_callback('download_images', 'running', overall_progress, progress_msg)
            finally:
                try:
                    progress_queue.put(None)
                except Exception:
                    pass
                progress_done.wait(timeout=10)
                reader_thread.join(timeout=5)
                executor.shutdown(wait=not _is_cancelled())
        else:
            # Thread pool only (no pickle; works when download_worker is not in the package)
            link_dataset = None
            if create_link and dataset_id and link_base_url_full:
                try:
                    if pid:
                        project = dl.projects.get(project_id=pid)
                        link_dataset = project.datasets.get(dataset_id=dataset_id)
                    else:
                        link_dataset = dl.datasets.get(dataset_id=dataset_id)
                except Exception as e:
                    logger.warning(f"Could not get dataset for create_link_on_download: {e}")

            def download_one(url):
                try:
                    if _is_cancelled():
                        return {'cancelled': True}
                    filename = os.path.basename(url)
                    filepath = os.path.join(storage_path, filename)
                    file_existed = os.path.exists(filepath) and os.path.getsize(filepath) > 0
                    if not file_existed:
                        response = requests.get(url, timeout=60)
                        response.raise_for_status()
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                    if link_dataset is not None and link_base_url_full and not _is_cancelled():
                        try:
                            _upload_one_link_item_standalone(link_dataset, filename, link_base_url_full, overwrite=True)
                        except Exception as link_err:
                            logger.warning(f"Ensure link item failed for {filename}: {link_err}")
                    return {'success': True, 'filename': filename, 'path': filepath, 'skipped': file_existed}
                except Exception as e:
                    return {'success': False, 'url': url, 'error': str(e)}

            completed = 0
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {executor.submit(download_one, url): url for url in image_urls}
                for future in as_completed(futures):
                    if _is_cancelled():
                        cancelled_count = total_images - completed
                        logger.info(f"Workflow cancelled - stopping download after {completed}/{total_images}")
                        break
                    result = future.result()
                    completed += 1
                    if result.get('cancelled'):
                        cancelled_count += 1
                        continue
                    if result.get('success'):
                        if result.get('skipped'):
                            skipped.append(result)
                        else:
                            downloaded.append(result)
                    else:
                        failed.append(result)
                    if completed % 50 == 0 or completed == total_images:
                        progress_pct = (completed / total_images) * 100 if total_images else 0
                        progress_msg = f"Progress: {completed}/{total_images} ({progress_pct:.1f}%) - New: {len(downloaded)}, Skipped: {len(skipped)}, Failed: {len(failed)}"
                        logger.info(progress_msg)
                        if progress_callback:
                            overall_progress = 15 + (progress_pct / 100) * 25
                            progress_callback('download_images', 'running', overall_progress, progress_msg)
        logger.info(f"Download complete: {len(downloaded)} new, {len(skipped)} skipped, {len(failed)} failed" + (f", {cancelled_count} cancelled" if cancelled_count else ""))

        
        # List actual files in directory
        actual_files = []
        if os.path.exists(storage_path):
            actual_files = [f for f in os.listdir(storage_path) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        out = {
            'storage_path': storage_path,
            'total_requested': total_images,
            'downloaded': len(downloaded),
            'skipped': len(skipped),
            'failed': len(failed),
            'actual_files_on_disk': len(actual_files),
            'files': actual_files
        }
        if cancelled_count:
            out['cancelled'] = cancelled_count
        return out

    def _upload_one_link_item(self, dataset, filename: str, link_base_url_full: str):
        """
        Create and upload one link item (JSON) to the dataset from an in-memory buffer.
        Used by create_link_items and by download_images when create_link_on_download=True.
        """
        ext = filename.lower().split('.')[-1]
        mimetype = 'image/jpeg' if ext in ['jpg', 'jpeg'] else f'image/{ext}'
        link_url = f"{link_base_url_full}/{filename}"
        link_item_content = {
            "type": "link",
            "shebang": "dataloop",
            "metadata": {
                "dltype": "link",
                "linkInfo": {
                    "type": "url",
                    "ref": link_url,
                    "mimetype": mimetype
                }
            }
        }
        json_filename = f"{os.path.splitext(filename)[0]}.json"
        json_bytes = json.dumps(link_item_content).encode('utf-8')
        buffer = io.BytesIO(json_bytes)
        remote_path = _remote_path_from_link_base_url(link_base_url_full)
        item = dataset.items.upload(
            local_path=buffer,
            remote_path=remote_path,
            remote_name=json_filename,
            overwrite=False
        )
        return item
    
    def create_link_items(self, dataset_id: str = None, filenames: list = None, num_workers: int = 20, progress_callback=None, link_base_url: str = None, project_id: str = None, workflow_id: str = None) -> dict:
        """
        Create link items in dataset for downloaded images (parallel upload).
        
        Args:
            dataset_id: Dataset ID (default: configured dataset_id)
            filenames: List of filenames to create links for
            num_workers: Number of parallel upload workers (default: 20)
            progress_callback: Optional callback for progress updates
            link_base_url: Base URL for link items (overrides instance default)
            project_id: Project ID (required if dataset access needs project context)
            workflow_id: If set, upload stops when workflow is cancelled (status 'cancelling').
        
        Returns:
            dict with creation results
        """
        if dataset_id is None:
            dataset_id = self.default_dataset_id
        
        logger.info(f"Creating link items in dataset {dataset_id}")
        
        # Get dataset - use project to avoid 403 errors
        try:
            # Try to get project_id if not provided
            if project_id is None:
                project_id = self.project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
            
            if project_id:
                project = dl.projects.get(project_id=project_id)
                dataset = project.datasets.get(dataset_id=dataset_id)
            else:
                # Fallback to direct access (may fail with 403)
                dataset = dl.datasets.get(dataset_id=dataset_id)
        except Exception as e:
            logger.error(f"Failed to get dataset: {e}")
            raise
        
        # Override link_base_url if provided (no date: reuse existing storage)
        if link_base_url:
            link_base_url_full = link_base_url.strip().rstrip('/')
            logger.info(f"[create_link_items] Using provided link_base_url: {link_base_url_full}")
        else:
            link_base_url_full = self.link_base_url
            logger.info(f"[create_link_items] Using instance link_base_url: {link_base_url_full}")
        
        # Log the final link_base_url_full to verify /s is present
        if '/s/' not in link_base_url_full and '/s' not in link_base_url_full:
            logger.warning(f"[create_link_items] WARNING: link_base_url_full does not contain '/s': {link_base_url_full}")
        
        # If no filenames provided, scan the storage path
        if filenames is None:
            if os.path.exists(self.storage_path):
                filenames = [f for f in os.listdir(self.storage_path) 
                            if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
            else:
                filenames = []
        
        if not filenames:
            return {'error': 'No images found in storage path', 'storage_path': self.storage_path, 'created': 0}
        
        total_files = len(filenames)
        logger.info(f"Creating {total_files} link items with {num_workers} workers...")
        
        # Get existing items to skip (includes link items created on-the-fly during download)
        logger.info("Fetching existing items to skip duplicates...")
        existing_items = set()
        try:
            remote_path = _remote_path_from_link_base_url(link_base_url_full)
            filters = dl.Filters()
            filters.add(field='dir', values=remote_path)
            pages = dataset.items.list(filters=filters)
            for item in pages.all():
                existing_items.add(item.name)
            logger.info(f"Found {len(existing_items)} existing items in {remote_path}")
        except Exception as e:
            logger.warning(f"Could not fetch existing items: {e}")
        
        # Filter out already uploaded files
        filenames_to_upload = []
        for filename in filenames:
            json_filename = f"{os.path.splitext(filename)[0]}.json"
            if json_filename not in existing_items:
                filenames_to_upload.append(filename)
        
        skipped_count = len(filenames) - len(filenames_to_upload)
        logger.info(f"Skipping {skipped_count} already uploaded items, uploading {len(filenames_to_upload)} new items")
        
        if not filenames_to_upload:
            return {
                'dataset_id': dataset_id,
                'total_requested': total_files,
                'created': 0,
                'skipped': skipped_count,
                'failed': 0,
                'message': 'All items already exist'
            }
        
        created = []
        failed = []

        def _is_cancelled():
            return workflow_id and workflow_progress.get(workflow_id, {}).get('status') == 'cancelling'

        def upload_one(filename):
            try:
                if _is_cancelled():
                    return {'cancelled': True}
                if '/s/' not in link_base_url_full and link_base_url_full.count('/s') == 0:
                    logger.warning(f"[create_link_items] WARNING: Link URL missing '/s': {link_base_url_full}")
                logger.debug(f"Created link URL for {filename}: {link_base_url_full}/{filename}")
                item = self._upload_one_link_item(dataset, filename, link_base_url_full)
                json_filename = f"{os.path.splitext(filename)[0]}.json"
                return {'success': True, 'filename': json_filename, 'item_id': item.id, 'link_url': f"{link_base_url_full}/{filename}"}
            except Exception as e:
                return {'success': False, 'filename': filename, 'error': str(e)}

        # Upload in parallel with progress logging (stops when workflow is cancelled if workflow_id set)
        completed = 0
        cancelled_count = 0
        total_to_upload = len(filenames_to_upload)
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(upload_one, fn): fn for fn in filenames_to_upload}
            for future in as_completed(futures):
                if _is_cancelled():
                    cancelled_count = total_to_upload - completed
                    logger.info(f"Workflow cancelled - stopping link item creation after {completed}/{total_to_upload}")
                    break
                result = future.result()
                completed += 1
                if result.get('cancelled'):
                    cancelled_count += 1
                    continue
                if result.get('success'):
                    created.append(result)
                else:
                    failed.append(result)
                if completed % 100 == 0 or completed == total_to_upload:
                    progress_pct = (completed / total_to_upload) * 100 if total_to_upload > 0 else 0
                    progress_msg = f"Progress: {completed}/{total_to_upload} ({progress_pct:.1f}%) - Created: {len(created)}, Failed: {len(failed)}"
                    logger.info(progress_msg)
                    if progress_callback:
                        overall_progress = 45 + (progress_pct / 100) * 15
                        progress_callback('create_link_items', 'running', overall_progress, progress_msg)
        logger.info(f"Link items complete: {len(created)} created, {skipped_count} skipped, {len(failed)} failed" + (f", {cancelled_count} cancelled" if cancelled_count else ""))
        
        out = {
            'dataset_id': dataset_id,
            'storage_path': self.storage_path,
            'link_base_url': link_base_url_full,
            'total_requested': total_files,
            'created': len(created),
            'skipped': skipped_count,
            'failed': len(failed),
            'failed_items': failed[:10] if failed else [],
            'sample_items': created[:5] if created else []
        }
        if cancelled_count:
            out['cancelled'] = cancelled_count
        return out

    def create_pipeline(self, project_id: str = None, pipeline_name: str = None, dpk_name: str = None,
                       pipeline_concurrency: int = 30, pipeline_max_replicas: int = 12,
                       stream_image_concurrency: int = None, stream_image_max_replicas: int = None,
                       resnet_concurrency: int = None, resnet_max_replicas: int = None,
                       progress_callback=None) -> dict:
        """
        Create stress test pipeline with ResNet model node.
        Installs the ResNet DPK if not already installed.
        
        Args:
            project_id: Project ID
            pipeline_name: Pipeline name
            dpk_name: DPK name to install (default: 'resnet')
            pipeline_concurrency: Default concurrency when per-node not set
            pipeline_max_replicas: Default max replicas when per-node not set
            stream_image_concurrency: Code node (stream-image) concurrency
            stream_image_max_replicas: Code node max replicas
            resnet_concurrency: ResNet node concurrency (used in DPK computeConfigs)
            resnet_max_replicas: ResNet node max replicas (used in DPK computeConfigs)
            progress_callback: Optional (step, status, pct, message) for progress during install
        
        Returns:
            dict with pipeline info
        """
        _stream_concurrency = stream_image_concurrency if stream_image_concurrency is not None else pipeline_concurrency
        _stream_max_replicas = stream_image_max_replicas if stream_image_max_replicas is not None else pipeline_max_replicas
        _resnet_concurrency = resnet_concurrency if resnet_concurrency is not None else 10
        _resnet_max_replicas = resnet_max_replicas if resnet_max_replicas is not None else pipeline_max_replicas

        # Set defaults for None values (SDK passes None explicitly)
        if dpk_name is None:
            dpk_name = 'resnet'
        if pipeline_name is None:
            pipeline_name = f'stress-test-pipeline-{self.date_str}'
        
        logger.info(f"Creating pipeline: {pipeline_name}")
        
        # Get project - use provided project_id or get from environment
        if project_id is None:
            project_id = self.project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
            if project_id is None:
                # Try to get from default dataset if available
                try:
                    dataset = dl.datasets.get(dataset_id=self.default_dataset_id)
                    project_id = dataset.project.id
                except Exception as e:
                    logger.warning(f"Could not get project from dataset: {e}")
                    raise ValueError("Project ID is required. Please provide project_id or ensure PROJECT_ID environment variable is set.")
        
        project = dl.projects.get(project_id=project_id)
        logger.info(f"Project: {project.name} (ID: {project.id})")
        
        # Step 1: Install ResNet DPK if not already installed
        # Check for custom DPK name first: resnet-{project.id}
        custom_dpk_name = f'resnet-{project.id}'
        logger.info(f"Step 1: Checking if custom DPK '{custom_dpk_name}' is installed...")
        app = None
        installed_dpk_name = dpk_name  # Default to original dpk_name, will be updated if custom DPK is used
        try:
            # Check if app is already installed (look for custom DPK name)
            apps = list(project.apps.list().all())
            logger.info(f"Found {len(apps)} apps in project")
            for a in apps:
                logger.info(f"  - App: {a.name}, dpk_name: {getattr(a, 'dpk_name', 'N/A')}")
                # Check for custom DPK name first
                if getattr(a, 'dpk_name', '') == custom_dpk_name:
                    app = a
                    installed_dpk_name = custom_dpk_name
                    logger.info(f"Found existing custom DPK app: {app.id} (dpk_name: {custom_dpk_name})")
                    break
                # Fallback: also check for original resnet DPK
                elif getattr(a, 'dpk_name', '') == dpk_name:
                    logger.info(f"Found original {dpk_name} app (will try to create custom version)")
                    # Use original if custom creation fails
                    if app is None:
                        app = a
                        installed_dpk_name = dpk_name
                        logger.info(f"Will use original DPK app: {app.name} (dpk_name: {dpk_name})")
                    # Don't break - we want to try creating custom version first
        except Exception as e:
            logger.warning(f"Could not list apps: {e}")
        
        dpk_to_install = None  # Initialize
        
        if app is None:
            # Check if custom resnet DPK already exists for this project
            custom_dpk_name = f'resnet-{project.id}'
            logger.info(f"Checking for custom DPK: {custom_dpk_name}")
            
            try:
                # Try to get the custom DPK (project-scoped, should be accessible)
                custom_dpk = project.dpks.get(dpk_name=custom_dpk_name)
                logger.info(f"Found existing custom DPK: {custom_dpk.name} v{custom_dpk.version}")
                dpk_to_install = custom_dpk
            except dl.exceptions.NotFound:
                logger.info(f"Custom DPK not found, creating it from {dpk_name}...")
                try:
                    # Try to get the original ResNet DPK from marketplace
                    # Services may not have permission to access marketplace DPKs directly
                    try:
                        original_dpk = dl.dpks.get(dpk_name=dpk_name)
                        logger.info(f"Found original DPK from marketplace: {original_dpk.name} v{original_dpk.version}")
                    except Exception as marketplace_error:
                        error_msg = str(marketplace_error)
                        if '403' in error_msg or 'Forbidden' in error_msg or 'not authorized' in error_msg.lower():
                            logger.warning(f"Service doesn't have permission to access marketplace DPKs")
                            logger.warning(f"Falling back to using original DPK name without custom version")
                            # Skip custom DPK creation, use original
                            raise Exception(f"Cannot access marketplace DPK. Will use original DPK if available.")
                        else:
                            raise
                    
                    # Clone the DPK and modify service versions
                    logger.info(f"Cloning DPK and modifying service versions to dtlpy 1.118.15...")
                    
                    # Get DPK data via API
                    headers = {'Authorization': f'Bearer {dl.token()}'}
                    base_url = dl.environment()
                    
                    # Get DPK details - correct endpoint: /app-registry/{id}
                    dpk_response = requests.get(
                        f"{base_url}/app-registry/{original_dpk.id}",
                        headers=headers
                    )
                    dpk_response.raise_for_status()
                    dpk_data = dpk_response.json()
                    
                    logger.info(f"Got DPK data, modifying services and computeConfigs...")
                    
                    # Modify services to have dtlpy version 1.118.15
                    if 'components' in dpk_data and 'services' in dpk_data['components']:
                        for service in dpk_data['components']['services']:
                            if 'versions' not in service:
                                service['versions'] = {}
                            service['versions']['dtlpy'] = '1.118.15'
                            logger.info(f"Updated service {service.get('name', 'unknown')} with dtlpy version 1.118.15")
                    
                    # Modify computeConfigs to have dtlpy version 1.118.15 (especially resnet-deploy)
                    if 'components' in dpk_data and 'computeConfigs' in dpk_data['components']:
                        for compute_config in dpk_data['components']['computeConfigs']:
                            if 'versions' not in compute_config:
                                compute_config['versions'] = {}
                            compute_config['versions']['dtlpy'] = '1.118.15'
                            
                            # Merge runtime config with existing runtime if it exists (ResNet node uses these)
                            new_runtime = {
                                "podType": "regular-m",
                                'concurrency': _resnet_concurrency,
                                'autoscaler': {
                                    'type': 'rabbitmq',
                                    'minReplicas': 1,
                                    'maxReplicas': _resnet_max_replicas
                                }
                            }
                            if 'runtime' in compute_config and isinstance(compute_config['runtime'], dict):
                                # Merge existing runtime with new runtime (new values take precedence)
                                compute_config['runtime'] = {**compute_config['runtime'], **new_runtime}
                                # Also merge autoscaler if it exists
                                if 'autoscaler' in compute_config['runtime'] and isinstance(compute_config['runtime']['autoscaler'], dict):
                                    compute_config['runtime']['autoscaler'] = {**compute_config['runtime']['autoscaler'], **new_runtime['autoscaler']}
                            else:
                                compute_config['runtime'] = new_runtime
                            compute_config['onReset'] = 'rerun'
                            compute_config['executionTimeout'] = 40
                            config_name = compute_config.get('name', 'unknown')
                            logger.info(f"Updated computeConfig '{config_name}' with dtlpy version 1.118.15")
                    
                    # Update DPK name and metadata
                    dpk_data['name'] = custom_dpk_name
                    dpk_data['displayName'] = f'ResNet (dtlpy 1.118.15) - {project.id[:8]}'
                    dpk_data['version'] = '1.0.0'  # Start fresh version
                    dpk_data['scope'] = 'project'  # Project-scoped
                    dpk_data['context'] = {'project': project.id}  # Set project context
                    
                    # Remove fields that shouldn't be in publish
                    dpk_data.pop('_id', None)
                    dpk_data.pop('id', None)
                    dpk_data.pop('createdAt', None)
                    dpk_data.pop('updatedAt', None)
                    dpk_data.pop('creator', None)
                    dpk_data.pop('latest', None)
                    dpk_data.pop('baseId', None)
                    
                    logger.info(f"Publishing custom DPK: {custom_dpk_name}")
                    
                    # Try to publish via SDK first (more reliable for permissions)
                    try:
                        # Create a temporary DPK file and publish via SDK
                        import tempfile
                        import shutil
                        
                        # Create temp directory for DPK
                        temp_dir = tempfile.mkdtemp()
                        dpk_json_path = os.path.join(temp_dir, 'dataloop.json')
                        
                        # Write modified DPK data to file
                        with open(dpk_json_path, 'w') as f:
                            json.dump(dpk_data, f, indent=2)
                        
                        logger.info(f"Created temporary DPK file: {dpk_json_path}")
                        
                        # Publish using SDK (which handles authentication better)
                        custom_dpk = project.dpks.publish(
                            manifest_filepath=dpk_json_path,
                            local_path=temp_dir
                        )
                        logger.info(f"DPK published via SDK: {custom_dpk.name} v{custom_dpk.version}")
                        dpk_to_install = custom_dpk
                        
                        # Clean up temp directory
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        
                    except Exception as sdk_error:
                        error_msg = str(sdk_error)
                        logger.warning(f"SDK publish failed: {sdk_error}")
                        
                        # Check if it's a permission error
                        if '403' in error_msg or 'Forbidden' in error_msg or 'not authorized' in error_msg.lower():
                            logger.error(f"403 Forbidden - Service doesn't have permission to publish DPKs")
                            logger.error(f"This is expected - services running in pods have limited permissions.")
                            logger.error(f"Falling back to original ResNet DPK: {dpk_name}")
                            
                            # Fallback: try to use the original ResNet DPK instead
                            try:
                                # Try marketplace first
                                try:
                                    original_dpk = dl.dpks.get(dpk_name=dpk_name)
                                    logger.info(f"Using original DPK from marketplace: {original_dpk.name} v{original_dpk.version}")
                                    dpk_to_install = original_dpk
                                    installed_dpk_name = dpk_name  # Use original name
                                except Exception as get_error:
                                    error_msg_get = str(get_error)
                                    if '403' in error_msg_get or 'Forbidden' in error_msg_get:
                                        logger.warning(f"Service cannot access marketplace DPKs. Checking if app is already installed...")
                                        # Check if app is already installed (we might have found it earlier)
                                        if app is None:
                                            raise Exception(f"Service cannot access marketplace DPKs and no app is installed. Please install ResNet DPK manually.")
                                        else:
                                            logger.info(f"Using existing installed app: {app.name}")
                                            # App is already installed, skip DPK installation
                                            dpk_to_install = None
                                    else:
                                        raise
                            except Exception as fallback_error:
                                logger.error(f"Failed to get original DPK: {fallback_error}")
                                # If we have an app already, we can use it
                                if app is None:
                                    raise Exception(f"Permission denied: Service cannot publish or access DPKs. Please install ResNet DPK manually. Error: {error_msg}")
                                else:
                                    logger.info(f"Using existing installed app: {app.name}")
                                    dpk_to_install = None
                        else:
                            # Other error - try API as fallback
                            logger.warning(f"Trying API as fallback...")
                            publish_response = requests.post(
                                f"{base_url}/app-registry",
                                json=dpk_data,
                                headers=headers
                            )
                            if publish_response.status_code == 403:
                                logger.error(f"403 Forbidden - Service doesn't have permission to publish DPKs")
                                logger.error(f"Response: {publish_response.text}")
                                raise Exception(f"Permission denied: Service doesn't have permission to publish DPKs. Please publish manually or grant permissions.")
                            publish_response.raise_for_status()
                            published_dpk_data = publish_response.json()
                            logger.info(f"DPK published via API: {published_dpk_data.get('name')} v{published_dpk_data.get('version')}")
                            
                            # Get the published DPK object from SDK
                            custom_dpk = project.dpks.get(dpk_name=custom_dpk_name)
                            logger.info(f"Retrieved custom DPK: {custom_dpk.name} v{custom_dpk.version}")
                            dpk_to_install = custom_dpk
                    
                except Exception as e:
                    logger.error(f"Failed to clone and modify DPK: {e}", exc_info=True)
                    return {'error': f'Failed to create custom {dpk_name} DPK: {str(e)}'}
            
            # Install the DPK (if we have one to install)
            if dpk_to_install is not None:
                try:
                    logger.info(f"Installing DPK: {dpk_to_install.name} v{dpk_to_install.version}")
                    logger.info("DPK installation may take several minutes - please wait...")
                    logger.info("This step installs the ResNet DPK which includes services and models...")
                    
                    # Install DPK (this can take a while)
                    app = project.apps.install(dpk=dpk_to_install)
                    logger.info(f"✓ DPK installation completed: {app.name} (ID: {app.id})")
                    # Update installed_dpk_name to match what was actually installed
                    installed_dpk_name = dpk_to_install.name
                    logger.info(f"Using DPK name: {installed_dpk_name} for pipeline node")
                    
                    # Give DPK a moment to start creating the model (Step 2 will poll for up to 3 min)
                    logger.info("Waiting 5 seconds for DPK to start model creation...")
                    time.sleep(5)
                        
                except Exception as e:
                    logger.error(f"Failed to install DPK: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return {'error': f'Failed to install DPK: {str(e)}'}
            else:
                logger.info(f"DPK installation skipped - using existing app: {app.name if app else 'N/A'}")
                if app:
                    # Get dpk_name from the existing app
                    installed_dpk_name = getattr(app, 'dpk_name', dpk_name)
                    logger.info(f"Using existing app DPK name: {installed_dpk_name}")
        
        # Verify app exists before proceeding
        if app is None:
            logger.error("No ResNet app found or installed!")
            return {
                'error': 'No ResNet app found. Please install the ResNet DPK first.',
                'dpk_name': dpk_name,
                'custom_dpk_name': custom_dpk_name
            }
        
        logger.info(f"Using app: {app.name} (ID: {app.id}, dpk_name: {installed_dpk_name})")
        
        # Step 2: Get the model from the installed app (DPK creates it asynchronously; poll with retries)
        logger.info("Step 2: Getting model from project...")
        model = None
        models = []
        try:
            max_wait_sec = 180  # 3 minutes total
            poll_interval = 10
            attempts = max(1, max_wait_sec // poll_interval)
            for attempt in range(attempts):
                logger.info("Listing models in project (this may take a moment)...")
                models = list(project.models.list().all())
                logger.info(f"Found {len(models)} models in project")
                for m in models:
                    logger.info(f"  - Model: {m.name} (ID: {m.id})")
                    if 'resnet' in m.name.lower():
                        model = m
                        logger.info(f"Using model: {model.name} (ID: {model.id})")
                        break
                if model is not None:
                    break
                if attempt < attempts - 1:
                    logger.info(f"ResNet model not found yet. Waiting {poll_interval}s before retry ({attempt + 1}/{attempts})...")
                    time.sleep(poll_interval)

            if model is None:
                logger.error("No ResNet model found in project after waiting.")
                return {
                    'error': 'No ResNet model found. DPK may not have installed correctly, or the model is still being created. Install the ResNet DPK from the marketplace first and wait a few minutes, then retry.',
                    'models_found': [m.name for m in models],
                    'app_id': app.id if app else None
                }
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            return {'error': f'Failed to list models: {str(e)}'}
        
        # Step 3: If pipeline with same name exists, delete it so we create a new one (no reuse - fresh pipeline every time)
        logger.info(f"Step 3: Checking for existing pipeline...")
        try:
            existing = project.pipelines.get(pipeline_name=pipeline_name)
            logger.info(f"Found existing pipeline: {existing.id}. Deleting so we can create a new one.")
            existing.delete()
            logger.info("Deleted existing pipeline")
        except dl.exceptions.NotFound:
            logger.info("No existing pipeline found")
        except Exception as e:
            logger.warning(f"Could not get or delete pipeline by name: {e}")
        
        # Step 4: Create new pipeline with 2 nodes
        logger.info(f"Step 4: Creating new pipeline with 2 nodes...")
        # Check if stream-image package/DPK already exists and rename it if needed
        try:
            existing_package = project.packages.get(package_name='stream-image')
            if existing_package:
                logger.info(f"Found existing stream-image package: {existing_package.id}")
                import time as time_module
                unique_suffix = time_module.strftime('%Y%m%d%H%M%S')
                new_package_name = f"stream-image-{unique_suffix}"
                logger.info(f"Renaming existing package from 'stream-image' to '{new_package_name}' to avoid conflict")
                try:
                    existing_package.name = new_package_name
                    existing_package.update()
                    logger.info(f"Successfully renamed package to: {new_package_name}")
                except Exception as rename_error:
                    logger.warning(f"Could not rename package: {rename_error}")
                    try:
                        logger.info(f"Trying to delete existing package instead...")
                        existing_package.delete()
                        logger.info(f"Successfully deleted existing package")
                    except Exception as delete_error:
                        logger.warning(f"Could not delete package either: {delete_error}")
        except dl.exceptions.NotFound:
            logger.info("No existing stream-image package found - will create new one")
        except Exception as e:
            logger.warning(f"Could not check for existing stream-image package: {e}")
        logger.info("Ensuring stream-image package name is available for pipeline creation...")
        pipeline = project.pipelines.create(name=pipeline_name)
        logger.info(f"Created pipeline: {pipeline.id}")
        
        # Add 2 nodes connected: stream-image (code) -> resnet (ml)
        try:
            code_node_id = f"stream-image-{self.date_str}"
            resnet_node_id = f"resnet-{self.date_str}"
            headers = {'Authorization': f'Bearer {dl.token()}'}
            base_url = dl.environment()
            
            logger.info(f"Step 4d: Adding 2 nodes to pipeline...")
            
            # Code node for streaming/downloading image
            # Root node receives items from batch execution
            # Package config is always included (existing package was renamed if it existed)
            logger.info("Creating code node configuration...")
            _stream_service_name = f"stream-image-{secrets.token_hex(4)}"
            code_node = {
                "id": code_node_id,
                "name": "stream-image",
                "displayName": "Stream Image",
                "inputs": [
                    {"portId": "item", "type": "Item", "name": "item", "displayName": "item", "io": "input"}
                ],
                "outputs": [
                    {"portId": "item-out", "type": "Item", "name": "item", "displayName": "item", "io": "output"}
                ],
                "metadata": {
                    "serviceConfig": {
                        "runtime": {
                            "concurrency": _stream_concurrency,
                            "autoscaler": {
                                "type": "rabbitmq",
                                "minReplicas": 1,
                                "maxReplicas": _stream_max_replicas,
                                "queueLength": 10
                            }
                        }
                    },
                    "position": {"x": 100, "y": 200, "z": 0},
                    "componentGroupName": "automation",
                    "codeApplicationName": "stream-image",
                    "repeatable": True
                },
                "type": "code",
                "namespace": {
                    "functionName": "stream_image",
                    "projectName": project.name,
                    "serviceName": _stream_service_name,
                    "moduleName": "code_module"
                    # Note: packageName is not included - package will be created from config.package
                },
                "projectId": project.id,
                "config": {
                    "package": {
                        "code": '''import dtlpy as dl
import io
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from PIL import Image

class ServiceRunner:

    def stream_image(self, item):
        # Same as dtlpy: Session + Retry + HTTPAdapter, timeout=120, stream + iter_content(8192)
        # See api_client.send_session and downloader.get_url_stream / __thread_download
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=1,
            status_forcelist=(500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        prepared = requests.Request(
            method="GET",
            url=item.stream,
            headers={
                "Authorization": "Bearer " + dl.token(),
                "x-dl-sanitize": "0",
                "Connection": "keep-alive",
            },
        ).prepare()
        response = session.send(request=prepared, stream=True, timeout=120)
        response.raise_for_status()
        image_data = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                image_data.write(chunk)
        image_data.seek(0)
        with Image.open(image_data) as img:
            img.verify()
            print("Success: Valid {} image downloaded.".format(img.format))
        return item
''',
                        "name": "run",
                        "type": "code",
                        "codebase": {"type": "Item"}
                    }
                }
            }
            
            # ResNet model node (distinct name/displayName and position so graph shows two different nodes)
            resnet_node = {
                "id": resnet_node_id,
                "name": "resnet",
                "displayName": f"ResNet ({model.name})",
                "inputs": [
                    {"portId": "item", "type": "Item", "name": "item", "displayName": "item", "io": "input"}
                ],
                "outputs": [
                    {"portId": "item", "type": "Item", "name": "item", "displayName": "item", "io": "output"},
                    {"portId": "annotations", "type": "Annotation[]", "name": "annotations", "displayName": "annotations", "io": "output"}
                ],
                "metadata": {
                    "position": {"x": 500, "y": 200, "z": 0},
                    "modelName": model.name,
                    "modelId": model.id,
                    "componentGroupName": "models",
                    "repeatable": True
                },
                "type": "ml",
                "namespace": {
                    "functionName": "predict",
                    "projectName": project.name,
                    "serviceName": "",
                    "moduleName": "default_module",
                    "packageName": dpk_name
                },
                "projectId": project.id,
                "appName": app.name if app else installed_dpk_name,
                "dpkName": installed_dpk_name
            }
            
            # Connection from code node output to resnet node input
            connection = {
                "src": {"nodeId": code_node_id, "portId": "item-out"},
                "tgt": {"nodeId": resnet_node_id, "portId": "item"},
                "condition": "{}"
            }
            
            pipeline_update = {
                "nodes": [code_node, resnet_node],
                "connections": [connection],
                "startNodes": [{
                    "nodeId": code_node_id,
                    "type": "root",
                    "id": f"start-{code_node_id}"
                }]
            }
            
            logger.info(f"Step 4e: Updating pipeline with nodes via API...")
            node_response = requests.patch(
                f"{base_url}/pipelines/{pipeline.id}",
                json=pipeline_update,
                headers=headers
            )
            
            if node_response.status_code != 200:
                logger.error(f"Pipeline API response: {node_response.status_code} - {node_response.text}")
                node_response.raise_for_status()
            
            logger.info(f"✓ Updated pipeline with nodes: {node_response.status_code}")
            
            # Log the response to verify
            result = node_response.json()
            logger.info(f"Pipeline nodes: {len(result.get('nodes', []))}")
            logger.info(f"Pipeline variables: {result.get('variables', [])}")
            
            # Refresh pipeline
            logger.info("Refreshing pipeline object...")
            pipeline = project.pipelines.get(pipeline_id=pipeline.id)
            logger.info(f"✓ Pipeline refreshed: {pipeline.id}")
            
        except Exception as e:
            logger.error(f"Failed to update pipeline: {e}")
            return {
                'pipeline_id': pipeline.id,
                'pipeline_name': pipeline.name,
                'error': f'Pipeline created but failed to add node: {str(e)}',
                'model_id': model.id,
                'app_id': app.id if app else None
            }
        
        # Install the pipeline (with retry for package conflicts)
        logger.info(f"Step 4f: Installing pipeline (this may take a few minutes)...")
        max_install_retries = 3
        install_success = False
        for retry in range(max_install_retries):
            try:
                pipeline = project.pipelines.get(pipeline_id=pipeline.id)
                if getattr(pipeline, 'status', None) == 'Installed':
                    logger.info(f"Step 4f: Pipeline already Installed, skipping install.")
                    install_success = True
                    break
            except Exception:
                pass
            install_exception = None
            install_error_holder = [None]
            import time as time_module

            def _do_install():
                try:
                    pipeline.install()
                except Exception as e:
                    install_error_holder[0] = e

            logger.info(f"Installing pipeline (attempt {retry + 1}/{max_install_retries})...")
            install_thread = Thread(target=_do_install)
            install_start = time_module.time()
            install_thread.start()
            heartbeat_interval = 15
            while install_thread.is_alive():
                time_module.sleep(1)
                elapsed = int(time_module.time() - install_start)
                if progress_callback and elapsed >= 1:
                    try:
                        progress_callback('create_pipeline', 'running', 70, f'Installing pipeline... ({elapsed // 60}m {elapsed % 60}s)')
                    except Exception:
                        pass
            install_thread.join()
            if install_error_holder[0] is not None:
                install_exception = install_error_holder[0]
            else:
                logger.info("Pipeline install() call completed, refreshing pipeline status...")
                pipeline = project.pipelines.get(pipeline_id=pipeline.id)
                logger.info(f"Pipeline status after install: {pipeline.status}")
                if pipeline.status in ['Installed']:
                    logger.info("Pipeline installed successfully")
                    install_success = True
                    break
                else:
                    logger.warning(f"Pipeline status after install: {pipeline.status}")
            if install_exception is not None:
                error_str = str(install_exception)
                logger.warning(f"Pipeline install raised exception: {error_str}")
                should_check_error = False
                error_message = None
                if install_exception:
                    error_str = str(install_exception)
                    error_message = error_str
                    should_check_error = True
                    logger.info(f"Checking exception message for package conflict: {error_str[:200]}")
                    if isinstance(install_exception, tuple) and len(install_exception) >= 2:
                        error_message = str(install_exception[1]) if install_exception[1] else error_str
                        logger.info(f"Extracted error from tuple: {error_message[:200]}")
                try:
                    pipeline = project.pipelines.get(pipeline_id=pipeline.id)
                    if pipeline.status == "Failure":
                        should_check_error = True
                        logger.warning(f"Pipeline installation failed with status: {pipeline.status}")
                        try:
                            composition_id = getattr(pipeline, 'composition_id', None) or (pipeline.composition.get('id') if isinstance(getattr(pipeline, 'composition', None), dict) else None)
                            if composition_id:
                                composition = project.compositions.get(composition_id=composition_id)
                                error_text = composition.get('errorText') if isinstance(composition, dict) else getattr(composition, 'errorText', None)
                                if error_text:
                                    error_message = error_text.get('message', '') if isinstance(error_text, dict) else str(error_text)
                        except Exception as comp_error:
                            logger.warning(f"Could not fetch composition: {comp_error}")
                except Exception as refresh_error:
                    logger.warning(f"Could not refresh pipeline: {refresh_error}")
                package_conflict_detected = False
                if should_check_error and error_message:
                    error_lower = error_message.lower()
                    if ('package with the name stream-image already exist' in error_lower or
                        'package stream-image already exist' in error_lower or
                        'package name stream-image already exist' in error_lower or
                        ('stream-image' in error_lower and 'already exist' in error_lower)):
                        package_conflict_detected = True
                        logger.info(f"Package conflict detected in error message: {error_message[:200]}")
                if package_conflict_detected:
                    logger.warning("Package name conflict detected - updating code node to use unique package name")
                    import time as time_module
                    unique_suffix = time_module.strftime('%Y%m%d%H%M%S')
                    new_package_name = f"stream-image-{unique_suffix}"
                    logger.info(f"Updating package name from 'stream-image' to '{new_package_name}'")
                    try:
                        pipeline = project.pipelines.get(pipeline_id=pipeline.id)
                        current_nodes = pipeline.nodes
                        if not isinstance(current_nodes, list):
                            if current_nodes is None:
                                current_nodes = []
                            else:
                                current_nodes = [current_nodes]
                        logger.info(f"Found {len(current_nodes)} nodes in pipeline")
                        updated_nodes = []
                        node_updated = False
                        if not isinstance(current_nodes, (list, tuple)):
                            current_nodes = [current_nodes]
                        for i, node in enumerate(current_nodes):
                            # Convert node to dict if it's an object (like CodeNode)
                            original_node = node
                            if not isinstance(node, dict):
                                try:
                                    # Try to_dict() method first
                                    if hasattr(node, 'to_dict'):
                                        node = node.to_dict()
                                    elif hasattr(node, 'type') or hasattr(node, 'namespace'):
                                        node_dict = {}
                                        if hasattr(node, 'type'):
                                            node_dict['type'] = node.type
                                        if hasattr(node, 'name'):
                                            node_dict['name'] = node.name
                                        if hasattr(node, 'namespace'):
                                            namespace = node.namespace
                                            if hasattr(namespace, 'packageName'):
                                                node_dict['namespace'] = {'packageName': namespace.packageName}
                                                if hasattr(namespace, 'serviceName'):
                                                    node_dict['namespace']['serviceName'] = namespace.serviceName
                                            elif isinstance(namespace, dict):
                                                node_dict['namespace'] = namespace
                                            else:
                                                node_dict['namespace'] = {}
                                        if hasattr(node, 'metadata'):
                                            metadata = node.metadata
                                            if isinstance(metadata, dict):
                                                node_dict['metadata'] = metadata
                                            elif hasattr(metadata, 'codeApplicationName'):
                                                node_dict['metadata'] = {'codeApplicationName': metadata.codeApplicationName}
                                            else:
                                                node_dict['metadata'] = {}
                                        for attr in ['id', 'inputs', 'outputs', 'config', 'projectId']:
                                            if hasattr(node, attr):
                                                node_dict[attr] = getattr(node, attr)
                                        node = node_dict
                                    elif hasattr(node, '__dict__'):
                                        node = dict(node.__dict__)
                                    else:
                                        node = dict(node)
                                except Exception as conv_error:
                                    logger.warning(f"Could not convert node {i} to dict: {conv_error}, trying to access as object")
                                    pass
                        
                        # Extract node properties (handle both dict and object)
                        if isinstance(node, dict):
                            node_type = node.get('type', '')
                            node_name = node.get('name', '')
                            node_namespace = node.get('namespace', {})
                            package_name = node_namespace.get('packageName', '') if isinstance(node_namespace, dict) else ''
                        else:
                            # Access as object attributes (fallback)
                            node_type = getattr(node, 'type', '')
                            node_name = getattr(node, 'name', '')
                            namespace_obj = getattr(node, 'namespace', None)
                            if namespace_obj:
                                if hasattr(namespace_obj, 'packageName'):
                                    package_name = namespace_obj.packageName
                                elif isinstance(namespace_obj, dict):
                                    package_name = namespace_obj.get('packageName', '')
                                else:
                                    package_name = ''
                            else:
                                package_name = ''
                        
                        logger.info(f"Node {i}: type={node_type}, name={node_name}, packageName={package_name}")
                        
                        # Check if this is the code node with stream-image package
                        is_code_node = node_type == 'code'
                        has_stream_image = (package_name == 'stream-image' or 
                                          node_name == 'stream-image' or
                                          'stream-image' in str(node).lower())
                        
                        if is_code_node and has_stream_image:
                            logger.info(f"Found code node to update: {node}")
                            # Create a deep copy of the node to modify
                            import copy
                            updated_node = copy.deepcopy(node)
                            
                            # Ensure namespace exists
                            if 'namespace' not in updated_node or not isinstance(updated_node.get('namespace'), dict):
                                updated_node['namespace'] = {}
                            
                            # Update the package name
                            updated_node['namespace']['packageName'] = new_package_name
                            logger.info(f"Updated namespace.packageName to: {new_package_name}")
                            
                            # Also update serviceName and name if they reference stream-image
                            if updated_node.get('name') == 'stream-image':
                                updated_node['name'] = new_package_name
                                logger.info(f"Updated node name to: {new_package_name}")
                            
                            if updated_node.get('namespace', {}).get('serviceName') == 'stream-image':
                                updated_node['namespace']['serviceName'] = new_package_name
                                logger.info(f"Updated namespace.serviceName to: {new_package_name}")
                            
                            if updated_node.get('metadata', {}).get('codeApplicationName') == 'stream-image':
                                if 'metadata' not in updated_node:
                                    updated_node['metadata'] = {}
                                updated_node['metadata']['codeApplicationName'] = new_package_name
                                logger.info(f"Updated metadata.codeApplicationName to: {new_package_name}")
                            
                            updated_nodes.append(updated_node)
                            node_updated = True
                            logger.info(f"Updated code node namespace: {updated_node.get('namespace')}")
                        else:
                            # Keep other nodes as-is (convert to dict if needed)
                            if not isinstance(node, dict):
                                try:
                                    node = node.to_dict() if hasattr(node, 'to_dict') else dict(node)
                                except:
                                    pass
                            updated_nodes.append(node)
                        
                        if node_updated:
                            connections = []
                            start_nodes = []
                            if hasattr(pipeline, 'connections'):
                                connections = pipeline.connections
                            elif hasattr(pipeline, 'composition') and hasattr(pipeline.composition, 'connections'):
                                connections = pipeline.composition.connections
                            if hasattr(pipeline, 'start_nodes'):
                                start_nodes = pipeline.start_nodes
                            elif hasattr(pipeline, 'startNodes'):
                                start_nodes = pipeline.startNodes
                            elif hasattr(pipeline, 'composition') and hasattr(pipeline.composition, 'startNodes'):
                                start_nodes = pipeline.composition.startNodes
                            logger.info(f"Updating pipeline with {len(updated_nodes)} nodes, {len(connections)} connections, {len(start_nodes)} startNodes")
                            pipeline_update = {
                                "nodes": updated_nodes,
                                "connections": connections,
                                "startNodes": start_nodes
                            }
                            if updated_nodes:
                                logger.info(f"First node in update: {updated_nodes[0]}")
                            headers = {'Authorization': f'Bearer {dl.token()}'}
                            base_url = dl.environment()
                            logger.info(f"Patching pipeline {pipeline.id} with updated nodes...")
                            update_response = requests.patch(
                                f"{base_url}/pipelines/{pipeline.id}",
                                json=pipeline_update,
                                headers=headers
                            )
                            logger.info(f"Pipeline update response: {update_response.status_code}")
                            if update_response.status_code != 200:
                                logger.error(f"Update response text: {update_response.text}")
                            if update_response.status_code == 200:
                                logger.info("Pipeline nodes updated successfully with new package name")
                                pipeline = project.pipelines.get(pipeline_id=pipeline.id)
                                verify_nodes = pipeline.nodes
                                for vnode in verify_nodes:
                                    if vnode.get('type') == 'code':
                                        v_package = vnode.get('namespace', {}).get('packageName', '')
                                        logger.info(f"Verified: code node packageName is now: {v_package}")
                                if retry < max_install_retries - 1:
                                    logger.info(f"Retrying pipeline installation with new package name (attempt {retry + 2}/{max_install_retries})...")
                                    continue
                            else:
                                logger.error(f"Failed to update pipeline nodes: {update_response.status_code} - {update_response.text}")
                        else:
                            logger.warning("Could not find code node to update")
                    except Exception as update_error:
                        logger.error(f"Error updating pipeline nodes: {update_error}")
                        import traceback
                        logger.debug(traceback.format_exc())
                
                    # If we get here and haven't succeeded, check if it's a retryable error
                    if not install_success and retry < max_install_retries - 1:
                        if error_message and ('already exist' in error_message.lower() or 'package' in error_message.lower()):
                            import time as time_module
                            time_module.sleep(2)
                            logger.info(f"Retrying pipeline installation (attempt {retry + 2}/{max_install_retries})...")
                            continue
            if not install_success:
                logger.warning("Pipeline installation had issues, but pipeline was created. You may need to install it manually.")
        
        return {
            'pipeline_id': pipeline.id,
            'pipeline_name': pipeline.name,
            'project_id': project.id,
            'status': pipeline.status,
            'app_id': app.id if app else None,
            'model_id': model.id if model else None,
            'message': 'Created pipeline with 2 nodes: stream-image -> ResNet'
        }
    
    def execute_pipeline_batch(self, pipeline_id: str = None, dataset_id: str = None, progress_callback=None) -> dict:
        """
        Execute pipeline on all items in dataset.
        
        Args:
            pipeline_id: Pipeline ID to execute (24 character hex string)
            dataset_id: Dataset ID (default: configured dataset_id)
        
        Returns:
            dict with execution info
        """
        if dataset_id is None:
            dataset_id = self.default_dataset_id
        
        # Validate pipeline_id
        if not pipeline_id or len(pipeline_id) != 24:
            return {
                'error': f'Invalid pipeline_id: "{pipeline_id}". Must be a 24 character hex string.',
                'example': '6970e972f5e45d56fb4c36c0'
            }
        
        logger.info(f"Executing pipeline {pipeline_id} on dataset {dataset_id}")
        
        headers = {'Authorization': f'Bearer {dl.token()}'}
        base_url = dl.environment()
        
        # Get dataset to count items
        dataset = dl.datasets.get(dataset_id=dataset_id)
        
        # Build filters for items in the stress-test folder (any date)
        filters = dl.Filters(resource=dl.FiltersResource.ITEM)
        filters.add(field='hidden', values=False)
        filters.add(field='type', values='file')
        filters.add(field='dir', values='/stress-test/*')
        
        # Get item count
        items_count = dataset.items.list(filters=filters).items_count
        logger.info(f"Found {items_count} items to process in /stress-test/")
        
        if items_count == 0:
            # Try without folder filter
            filters2 = dl.Filters(resource=dl.FiltersResource.ITEM)
            filters2.add(field='hidden', values=False)
            filters2.add(field='type', values='file')
            items_count = dataset.items.list(filters=filters2).items_count
            logger.info(f"Total items in dataset: {items_count}")
            filters = filters2
        
        # Execute batch using correct format from piper source
        try:
            filter_obj = filters.prepare()['filter']
            
            # Correct format from piper source: batch.query.context.datasets
            payload = {
                "batch": {
                    "query": {
                        "filter": filter_obj,
                        "resource": "items",
                        "context": {
                            "datasets": [dataset_id]
                        }
                    },
                    "args": {}
                }
            }
            
            result = None
            logger.info(f"Trying payload format: {payload}")
            logger.info(f"POST {base_url}/pipelines/{pipeline_id}/execute")
            response = requests.post(
                f"{base_url}/pipelines/{pipeline_id}/execute",
                json=payload,
                headers=headers
            )
            
            logger.info(f"API response status: {response.status_code}")
            batch_execution_id = None
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Batch execution started: {result}")
                
                # Extract batch execution ID if available
                batch_execution_id = result.get('id') or result.get('_id') or result.get('execution_id')
                logger.info(f"Extracted batch_execution_id: {batch_execution_id}")
            else:
                logger.warning(f"Direct API failed: {response.status_code} - {response.text}")
                # Try using pipeline_executions.create_batch via SDK
                logger.info("Trying SDK create_batch...")
                try:
                    pipeline = dl.pipelines.get(pipeline_id=pipeline_id)
                    
                    # Try SDK's pipeline_executions.create_batch
                    filters_dict = filters.prepare()
                    logger.info(f"Calling pipeline_executions.create_batch with filters: {filters_dict}")
                    
                    # The SDK might handle datasetId internally
                    batch_result = pipeline.pipeline_executions.create_batch(
                        filters=filters,
                        dataset_id=dataset_id
                    )
                    logger.info(f"SDK create_batch result: {batch_result}")
                    
                    # Extract execution ID from batch_result if it's an object
                    if hasattr(batch_result, 'id'):
                        batch_execution_id = batch_result.id
                    elif isinstance(batch_result, dict):
                        batch_execution_id = batch_result.get('id') or batch_result.get('_id')
                    
                    # Start progress polling in background thread AND wait for completion
                    if progress_callback:
                        # Send initial progress update
                        progress_callback('execute_pipeline_batch', 'running', 85, f'Batch execution started via SDK, waiting for statistics...')
                        
                        completion_event = threading.Event()
                        
                        def poll_with_completion():
                            """Wrapper that signals completion"""
                            try:
                                logger.info(f"[Poll Thread] Starting polling for pipeline {pipeline_id}, batch_execution_id={batch_execution_id}")
                                if batch_execution_id:
                                    logger.info(f"[Poll Thread] Using batch execution progress polling")
                                    self._poll_batch_execution_progress(pipeline_id, batch_execution_id, items_count, progress_callback)
                                else:
                                    logger.info(f"[Poll Thread] Using pipeline state progress polling")
                                    self._poll_pipeline_state_progress(pipeline_id, items_count, progress_callback)
                                logger.info(f"[Poll Thread] Polling completed")
                            except Exception as poll_error:
                                logger.error(f"[Poll Thread] Error in polling: {poll_error}", exc_info=True)
                                if progress_callback:
                                    progress_callback('execute_pipeline_batch', 'failed', 95, f'Polling error: {str(poll_error)}', error=str(poll_error))
                            finally:
                                logger.info(f"[Poll Thread] Setting completion event")
                                completion_event.set()
                        
                        # Start polling in background
                        poll_thread = Thread(target=poll_with_completion, daemon=False)
                        poll_thread.start()
                        logger.info(f"[Main Thread] Poll thread started (thread ID: {poll_thread.ident}, is_alive: {poll_thread.is_alive()})")
                        
                        # Give polling thread a moment to start
                        import time as time_module
                        time_module.sleep(1)
                        logger.info(f"[Main Thread] Poll thread status after 1s: is_alive={poll_thread.is_alive()}")
                        
                        # Wait for completion (with timeout)
                        logger.info(f"[Main Thread] Waiting for batch execution to complete (max 24 hours)...")
                        event_set = completion_event.wait(timeout=3600 * 24)  # 24 hour max
                        
                        if not event_set:
                            logger.warning("[Main Thread] Batch execution polling timed out after 24 hours")
                            return {
                                'success': False,
                                'pipeline_id': pipeline_id,
                                'dataset_id': dataset_id,
                                'items_count': items_count,
                                'message': 'Batch execution started but polling timed out',
                                'timeout': True
                            }
                        else:
                            logger.info("[Main Thread] Batch execution polling completed - event was set")
                    else:
                        # No progress callback - just start polling in background without waiting
                        if batch_execution_id:
                            Thread(target=self._poll_batch_execution_progress, args=(
                                pipeline_id, batch_execution_id, items_count, progress_callback
                            ), daemon=True).start()
                        else:
                            Thread(target=self._poll_pipeline_state_progress, args=(
                                pipeline_id, items_count, progress_callback
                            ), daemon=True).start()
                    
                    return {
                        'success': True,
                        'pipeline_id': pipeline_id,
                        'dataset_id': dataset_id,
                        'batch_result': str(batch_result),
                        'items_count': items_count,
                        'message': 'Batch execution completed via SDK'
                    }
                except Exception as sdk_error:
                    logger.error(f"SDK create_batch also failed: {sdk_error}")
                    return {
                        'error': f'All batch API formats failed. SDK error: {str(sdk_error)}',
                        'pipeline_id': pipeline_id,
                        'dataset_id': dataset_id
                    }
            
            # Start progress polling in background thread AND wait for completion
            if progress_callback:
                # Send initial progress update immediately
                logger.info(f"[Main Thread] Sending initial progress update for batch execution")
                progress_callback('execute_pipeline_batch', 'running', 85, f'Batch execution started, initializing polling...')
                
                completion_event = threading.Event()
                
                def poll_with_completion():
                    """Wrapper that signals completion"""
                    try:
                        logger.info(f"[Poll Thread] Starting polling for pipeline {pipeline_id}, batch_execution_id={batch_execution_id}, items_count={items_count}")
                        logger.info(f"[Poll Thread] Progress callback available: {progress_callback is not None}")
                        # Send immediate progress update when polling starts
                        if progress_callback:
                            logger.info(f"[Poll Thread] Sending initial polling update")
                            progress_callback('execute_pipeline_batch', 'running', 85, f'Polling started, waiting for statistics...')
                            logger.info(f"[Poll Thread] Initial polling update sent")
                        else:
                            logger.warning(f"[Poll Thread] No progress callback available!")
                        
                        if batch_execution_id:
                            logger.info(f"[Poll Thread] Using batch execution progress polling")
                            self._poll_batch_execution_progress(pipeline_id, batch_execution_id, items_count, progress_callback)
                        else:
                            logger.info(f"[Poll Thread] Using pipeline state progress polling")
                            self._poll_pipeline_state_progress(pipeline_id, items_count, progress_callback)
                        logger.info(f"[Poll Thread] Polling completed normally - all items processed")
                    except Exception as poll_error:
                        logger.error(f"[Poll Thread] Error in polling: {poll_error}", exc_info=True)
                        if progress_callback:
                            try:
                                progress_callback('execute_pipeline_batch', 'failed', 95, f'Polling error: {str(poll_error)}', error=str(poll_error))
                            except Exception as callback_err:
                                logger.error(f"[Poll Thread] Error in failure callback: {callback_err}")
                    finally:
                        logger.info(f"[Poll Thread] Setting completion event to signal main thread")
                        completion_event.set()
                        logger.info(f"[Poll Thread] Completion event set - main thread will be notified")
                
                # Start polling in background
                poll_thread = Thread(target=poll_with_completion, daemon=False)
                poll_thread.start()
                logger.info(f"[Main Thread] Poll thread started (thread ID: {poll_thread.ident}, is_alive: {poll_thread.is_alive()})")
                
                # Give polling thread a moment to start and send first update
                import time as time_module
                time_module.sleep(2)  # Give it 2 seconds to start and send first update
                logger.info(f"[Main Thread] Poll thread status after 2s: is_alive={poll_thread.is_alive()}")
                
                # Wait for completion (with timeout)
                logger.info(f"[Main Thread] Waiting for batch execution to complete (max 24 hours)...")
                event_set = completion_event.wait(timeout=3600 * 24)  # 24 hour max
                
                if not event_set:
                    logger.warning("[Main Thread] Batch execution polling timed out after 24 hours")
                    return {
                        'success': False,
                        'pipeline_id': pipeline_id,
                        'dataset_id': dataset_id,
                        'items_count': items_count,
                        'message': 'Batch execution started but polling timed out',
                        'timeout': True
                    }
                else:
                    logger.info("[Main Thread] Batch execution polling completed - event was set")
                    # Check if polling actually completed successfully or if it was an error
                    # The polling thread should have logged completion status
            else:
                # No progress callback - just start polling in background without waiting
                logger.warning("[Main Thread] No progress callback - starting polling in background without waiting")
                if batch_execution_id:
                    Thread(target=self._poll_batch_execution_progress, args=(
                        pipeline_id, batch_execution_id, items_count, progress_callback
                    ), daemon=True).start()
                else:
                    Thread(target=self._poll_pipeline_state_progress, args=(
                        pipeline_id, items_count, progress_callback
                    ), daemon=True).start()
                # Return immediately since we're not waiting
                return {
                    'success': True,
                    'pipeline_id': pipeline_id,
                    'dataset_id': dataset_id,
                    'items_count': items_count,
                    'message': f'Batch execution started (polling in background, no progress callback)'
                }
            
        except Exception as e:
            logger.error(f"execute_batch failed: {e}", exc_info=True)
            return {
                'error': f'Failed to execute pipeline: {str(e)}',
                'pipeline_id': pipeline_id,
                'dataset_id': dataset_id
            }
        
        logger.info(f"[Main Thread] Batch execution function returning - polling completed")
        
        return {
            'success': True,
            'pipeline_id': pipeline_id,
            'dataset_id': dataset_id,
            'items_count': items_count,
            'message': f'Batch execution completed on {items_count} items'
        }
    
    def _poll_batch_execution_progress(self, pipeline_id: str, batch_execution_id: str, items_count: int, progress_callback):
        """Poll batch execution progress using pipeline statistics API only"""
        import time as time_module
        start_time = time_module.time()
        max_poll_time = 3600 * 24  # 24 hours max
        poll_interval = 5  # Poll every 5 seconds
        
        last_success = 0
        last_failed = 0
        
        # Send initial progress update
        if progress_callback:
            progress_callback('execute_pipeline_batch', 'running', 85, f'Starting progress polling for {items_count} items...')
            logger.info(f"[Poll Batch] Initial progress update sent")
        
        poll_count = 0
        consecutive_no_stats_count = 0  # Track consecutive polls without statistics
        max_no_stats_retries = 12  # Allow up to 12 consecutive polls (1 minute) without stats before giving up
        logger.info(f"[Poll Batch] Starting polling loop (max_time={max_poll_time}s, interval={poll_interval}s)")
        logger.info(f"[Poll Batch] Will wait up to {max_no_stats_retries * poll_interval}s for statistics to become available")
        
        while time_module.time() - start_time < max_poll_time:
            poll_count += 1
            elapsed_sec = int(time_module.time() - start_time)
            elapsed_str_loop = f"{elapsed_sec // 60}m {elapsed_sec % 60}s"
            logger.info(f"[Poll Batch] Poll iteration #{poll_count} (elapsed: {elapsed_str_loop})")
            try:
                # Calculate elapsed time
                elapsed = time_module.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                
                # Get pipeline statistics via API directly (don't need pipeline object or execution)
                # This is the main source of progress info
                headers = {'Authorization': f'Bearer {dl.token()}'}
                base_url = dl.environment()
                
                pipeline_stats = None
                pipeline_not_found = False
                
                try:
                    # Get pipeline statistics with execution counters
                    stats_response = requests.get(
                        f"{base_url}/pipelines/{pipeline_id}/statistics",
                        headers=headers,
                        timeout=30  # 30 second timeout for the request
                    )
                    
                    if stats_response.status_code == 200:
                        pipeline_stats = stats_response.json()
                        consecutive_no_stats_count = 0  # Reset counter when stats are available
                        logger.info(f"[Poll Batch] ✓ Statistics available (poll #{poll_count})")
                    elif stats_response.status_code == 404:
                        # Check if it's pipeline not found or just no statistics yet
                        error_text = stats_response.text.lower()
                        if 'pipeline not found' in error_text or ('not found' in error_text and 'statistics' not in error_text):
                            logger.error(f"[Poll Batch] Pipeline {pipeline_id} not found (404) - pipeline may have been deleted")
                            pipeline_not_found = True
                        else:
                            # No statistics yet, pipeline might not have started - wait and retry
                            consecutive_no_stats_count += 1
                            logger.info(f"[Poll Batch] No statistics yet (poll #{poll_count}, consecutive: {consecutive_no_stats_count}/{max_no_stats_retries})")
                            logger.info(f"[Poll Batch] Pipeline may still be starting - will wait and retry...")
                            
                            # If we've waited long enough without stats, check if pipeline exists
                            if consecutive_no_stats_count >= max_no_stats_retries:
                                logger.warning(f"[Poll Batch] No statistics after {max_no_stats_retries} polls - checking if pipeline exists...")
                                # Try to verify pipeline exists before giving up
                                try:
                                    pipeline_check = requests.get(
                                        f"{base_url}/pipelines/{pipeline_id}",
                                        headers=headers,
                                        timeout=10
                                    )
                                    if pipeline_check.status_code == 404:
                                        logger.error(f"[Poll Batch] Pipeline {pipeline_id} does not exist - breaking")
                                        pipeline_not_found = True
                                    else:
                                        logger.info(f"[Poll Batch] Pipeline exists - statistics may take longer, continuing to wait...")
                                        consecutive_no_stats_count = 0  # Reset and continue waiting
                                except Exception as check_error:
                                    logger.warning(f"[Poll Batch] Could not verify pipeline existence: {check_error} - continuing to wait")
                                    consecutive_no_stats_count = 0  # Reset and continue
                    else:
                        logger.warning(f"Failed to get pipeline statistics: {stats_response.status_code} - {stats_response.text[:200]}")
                        consecutive_no_stats_count += 1
                except requests.exceptions.Timeout:
                    logger.warning(f"[Poll Batch] Request timeout while fetching statistics (poll #{poll_count}) - will retry")
                    consecutive_no_stats_count += 1
                    pipeline_stats = None
                except Exception as stats_error:
                    error_msg = str(stats_error)
                    if '404' in error_msg or ('not found' in error_msg.lower() and 'pipeline' in error_msg.lower()):
                        logger.error(f"[Poll Batch] Pipeline {pipeline_id} not found - pipeline may have been deleted")
                        pipeline_not_found = True
                    else:
                        logger.debug(f"Could not get pipeline statistics via API: {stats_error} - will retry")
                        consecutive_no_stats_count += 1
                    pipeline_stats = None
                
                # Break if pipeline not found
                if pipeline_not_found:
                    if progress_callback:
                        try:
                            progress_callback('execute_pipeline_batch', 'failed', 95, f'Pipeline {pipeline_id} not found - may have been deleted', error=f'Pipeline not found: {pipeline_id}')
                        except:
                            pass
                    break
                
                # Calculate elapsed time (always calculate, even if stats not available)
                elapsed = time_module.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                
                if pipeline_stats:
                    # Parse execution counters from statistics
                    execution_counters = pipeline_stats.get('pipelineExecutionCounters', [])
                    success_count = 0
                    failed_count = 0
                    count_in_progress = 0
                    
                    for counter in execution_counters:
                        status = counter.get('status', '').lower()
                        count = counter.get('count', 0)
                        if status == 'success':
                            success_count = count
                        elif status == 'failed':
                            failed_count = count
                        elif status in ['in-progress', 'inprogress', 'running', 'pending']:
                            count_in_progress = count
                    
                    completed = success_count + failed_count
                    # Only mark as completed if ALL items are done (completed >= items_count)
                    # AND there are no items in progress
                    if completed >= items_count and count_in_progress == 0:
                        pipeline_status = 'completed'
                    elif count_in_progress > 0 or completed < items_count:
                        pipeline_status = 'running'
                    else:
                        pipeline_status = 'unknown'
                    
                    # Calculate progress
                    progress_pct = (completed / items_count * 100) if items_count > 0 else 0
                    
                    # Calculate overall progress first (needed for log callbacks)
                    overall_progress = 85 + (progress_pct / 100) * 10
                else:
                    # Statistics not available yet - show initial progress
                    success_count = 0
                    failed_count = 0
                    count_in_progress = items_count  # Assume all are in progress if stats not available
                    completed = 0
                    pipeline_status = 'running'
                    progress_pct = 0
                    overall_progress = 85
                
                # Update progress with detailed counts - ALWAYS call callback to ensure UI updates
                # This runs on EVERY poll iteration, regardless of whether stats are available
                try:
                    if pipeline_stats:
                        progress_msg = f"Progress: {completed}/{items_count} items completed ({progress_pct:.1f}%) | Success: {success_count} | Failed: {failed_count} | In Progress: {count_in_progress} | Elapsed: {elapsed_str}"
                        logger.info(f"[Batch Progress] Stats available: {progress_msg}")
                    else:
                        # Statistics not available yet - show waiting message with elapsed time
                        progress_msg = f"Progress: Waiting for statistics... | Elapsed: {elapsed_str} | Poll #{poll_count}"
                        logger.info(f"[Batch Progress] No stats yet: {progress_msg}")
                except Exception as progress_error:
                    logger.error(f"[Batch Progress] Error building progress message: {progress_error}", exc_info=True)
                    progress_msg = f"Progress: Polling... | Elapsed: {elapsed_str} | Poll #{poll_count}"
                
                # ALWAYS call progress callback on every poll iteration - CRITICAL for UI updates
                try:
                    if progress_callback:
                        logger.info(f"[Poll Batch] Calling progress callback (poll #{poll_count}): {progress_msg[:80]}...")
                        progress_callback('execute_pipeline_batch', 'running', overall_progress, progress_msg)
                        logger.info(f"[Poll Batch] ✓ Progress callback completed successfully")
                    else:
                        logger.warning(f"[Poll Batch] ⚠ No progress callback provided - UI won't update!")
                except Exception as callback_error:
                    logger.error(f"[Poll Batch] ✗ Error calling progress callback: {callback_error}", exc_info=True)
                
                last_success = success_count
                last_failed = failed_count
                
                # Check if complete (only if we have stats)
                if pipeline_stats:
                        # Only break if ALL items are completed (completed >= items_count)
                        if pipeline_status == 'completed' and completed >= items_count:
                            final_msg = f"Batch execution completed: {completed}/{items_count} items processed | Success: {success_count} | Failed: {failed_count} | Time: {elapsed_str}"
                            logger.info(f"[Poll Batch] Pipeline completed: {final_msg}")
                            if progress_callback:
                                try:
                                    progress_callback('execute_pipeline_batch', 'completed', 95, final_msg)
                                except Exception as e:
                                    logger.error(f"[Poll Batch] Error in completion callback: {e}")
                            break
                        elif pipeline_status == 'failed':
                            final_msg = f"Batch execution failed: {completed}/{items_count} items processed | Success: {success_count} | Failed: {failed_count} | Time: {elapsed_str}"
                            logger.error(f"[Poll Batch] Pipeline failed: {final_msg}")
                            if progress_callback:
                                try:
                                    progress_callback('execute_pipeline_batch', 'failed', 95, final_msg, error="Pipeline execution failed")
                                except Exception as e:
                                    logger.error(f"[Poll Batch] Error in failure callback: {e}")
                            break
                
                time_module.sleep(poll_interval)
                
            except Exception as e:
                logger.error(f"[Poll Batch] Error in polling loop: {e}", exc_info=True)
                # Still try to send progress update even on error
                if progress_callback:
                    try:
                        elapsed = time_module.time() - start_time
                        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                        progress_callback('execute_pipeline_batch', 'running', 85, f'Polling error (retrying): {str(e)[:50]}... | Elapsed: {elapsed_str}')
                    except:
                        pass
                time_module.sleep(poll_interval)
    
    def _poll_pipeline_state_progress(self, pipeline_id: str, items_count: int, progress_callback):
        """Poll pipeline state for execution progress and fetch execution logs"""
        import time as time_module
        start_time = time_module.time()
        max_poll_time = 3600 * 24  # 24 hours max
        poll_interval = 5  # Poll every 5 seconds
        
        last_completed = 0
        seen_execution_ids = set()  # Track which executions we've already logged
        
        # Send initial progress update
        if progress_callback:
            progress_callback('execute_pipeline_batch', 'running', 85, f'Starting progress polling for {items_count} items...')
            logger.info(f"[Poll Pipeline State] Initial progress update sent")
        
        poll_count = 0
        consecutive_no_stats_count = 0  # Track consecutive polls without statistics
        max_no_stats_retries = 12  # Allow up to 12 consecutive polls (1 minute) without stats before giving up
        logger.info(f"[Poll Pipeline State] Starting polling loop (max_time={max_poll_time}s, interval={poll_interval}s)")
        logger.info(f"[Poll Pipeline State] Will wait up to {max_no_stats_retries * poll_interval}s for statistics to become available")
        
        while time_module.time() - start_time < max_poll_time:
            poll_count += 1
            logger.info(f"[Poll Pipeline State] Poll iteration #{poll_count} (elapsed: {int((time_module.time() - start_time) // 60)}m {int((time_module.time() - start_time) % 60)}s)")
            try:
                # Get pipeline statistics via API directly (don't need pipeline object)
                # This is more accurate than state and doesn't require the pipeline object
                headers = {'Authorization': f'Bearer {dl.token()}'}
                base_url = dl.environment()
                
                pipeline_stats = None
                pipeline_not_found = False
                
                try:
                    # Get pipeline statistics with execution counters
                    stats_response = requests.get(
                        f"{base_url}/pipelines/{pipeline_id}/statistics",
                        headers=headers,
                        timeout=30  # 30 second timeout for the request
                    )
                    
                    if stats_response.status_code == 200:
                        pipeline_stats = stats_response.json()
                        consecutive_no_stats_count = 0  # Reset counter when stats are available
                        logger.info(f"[Poll Pipeline State] ✓ Statistics available (poll #{poll_count})")
                    elif stats_response.status_code == 404:
                        # Check if it's pipeline not found or just no statistics yet
                        error_text = stats_response.text.lower()
                        if 'pipeline not found' in error_text or ('not found' in error_text and 'statistics' not in error_text):
                            logger.error(f"[Poll Pipeline State] Pipeline {pipeline_id} not found (404) - pipeline may have been deleted")
                            pipeline_not_found = True
                        else:
                            # No statistics yet, pipeline might not have started - wait and retry
                            consecutive_no_stats_count += 1
                            logger.info(f"[Poll Pipeline State] No statistics yet (poll #{poll_count}, consecutive: {consecutive_no_stats_count}/{max_no_stats_retries})")
                            logger.info(f"[Poll Pipeline State] Pipeline may still be starting - will wait and retry...")
                            
                            # If we've waited long enough without stats, check if pipeline exists
                            if consecutive_no_stats_count >= max_no_stats_retries:
                                logger.warning(f"[Poll Pipeline State] No statistics after {max_no_stats_retries} polls - checking if pipeline exists...")
                                # Try to verify pipeline exists before giving up
                                try:
                                    pipeline_check = requests.get(
                                        f"{base_url}/pipelines/{pipeline_id}",
                                        headers=headers,
                                        timeout=10
                                    )
                                    if pipeline_check.status_code == 404:
                                        logger.error(f"[Poll Pipeline State] Pipeline {pipeline_id} does not exist - breaking")
                                        pipeline_not_found = True
                                    else:
                                        logger.info(f"[Poll Pipeline State] Pipeline exists - statistics may take longer, continuing to wait...")
                                        consecutive_no_stats_count = 0  # Reset and continue waiting
                                except Exception as check_error:
                                    logger.warning(f"[Poll Pipeline State] Could not verify pipeline existence: {check_error} - continuing to wait")
                                    consecutive_no_stats_count = 0  # Reset and continue
                    else:
                        logger.warning(f"Failed to get pipeline statistics: {stats_response.status_code} - {stats_response.text[:200]}")
                        consecutive_no_stats_count += 1
                except requests.exceptions.Timeout:
                    logger.warning(f"[Poll Pipeline State] Request timeout while fetching statistics (poll #{poll_count}) - will retry")
                    consecutive_no_stats_count += 1
                    pipeline_stats = None
                except Exception as stats_error:
                    error_msg = str(stats_error)
                    if '404' in error_msg or ('not found' in error_msg.lower() and 'pipeline' in error_msg.lower()):
                        logger.error(f"[Poll Pipeline State] Pipeline {pipeline_id} not found - pipeline may have been deleted")
                        pipeline_not_found = True
                    else:
                        logger.debug(f"Could not get pipeline statistics via API: {stats_error} - will retry")
                        consecutive_no_stats_count += 1
                    pipeline_stats = None
                
                # Break if pipeline not found
                if pipeline_not_found:
                    if progress_callback:
                        try:
                            progress_callback('execute_pipeline_batch', 'failed', 95, f'Pipeline {pipeline_id} not found - may have been deleted', error=f'Pipeline not found: {pipeline_id}')
                        except:
                            pass
                    break
                
                # Calculate elapsed time (always calculate, even if stats not available)
                elapsed = time_module.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                
                if pipeline_stats:
                    # Parse execution counters from statistics
                    execution_counters = pipeline_stats.get('pipelineExecutionCounters', [])
                    success_count = 0
                    failed_count = 0
                    count_in_progress = 0
                    
                    for counter in execution_counters:
                        status = counter.get('status', '').lower()
                        count = counter.get('count', 0)
                        if status == 'success':
                            success_count = count
                        elif status == 'failed':
                            failed_count = count
                        elif status in ['in-progress', 'inprogress', 'running', 'pending']:
                            count_in_progress = count
                    
                    completed = success_count + failed_count
                    # Only mark as completed if ALL items are done (completed >= items_count)
                    # AND there are no items in progress
                    if completed >= items_count and count_in_progress == 0:
                        pipeline_status = 'completed'
                    elif count_in_progress > 0 or completed < items_count:
                        pipeline_status = 'running'
                    else:
                        pipeline_status = 'unknown'
                    
                    # Calculate progress
                    progress_pct = (completed / items_count * 100) if items_count > 0 else 0
                    
                    # Calculate overall progress (needed for log callbacks)
                    overall_progress = 85 + (progress_pct / 100) * 10
                else:
                    # Statistics not available yet - show initial progress
                    success_count = 0
                    failed_count = 0
                    count_in_progress = items_count  # Assume all are in progress if stats not available
                    completed = 0
                    pipeline_status = 'running'
                    progress_pct = 0
                    overall_progress = 85
                    
                    # Try to fetch execution logs for visibility
                    try:
                        project_id = pipeline.project.id
                        project = dl.projects.get(project_id=project_id)
                        
                        # Get recent executions for log display
                        execution_filters = dl.Filters(resource=dl.FiltersResource.EXECUTION)
                        execution_filters.add(field='pipeline.id', values=pipeline_id)
                        execution_filters.add(field='status', values=['running', 'inprogress', 'pending', 'completed', 'success', 'failed'])
                        execution_filters.sort_by(field='created_at', value=dl.FiltersOrderByDirection.DESCENDING)
                        
                        recent_executions = project.executions.list(filters=execution_filters, page_size=10)
                        
                        for execution in recent_executions.items[:5]:  # Process top 5
                            exec_id = execution.id
                            if exec_id not in seen_execution_ids:
                                seen_execution_ids.add(exec_id)
                                
                                # Try to get logs for this execution
                                try:
                                    # Get execution logs
                                    exec_logs = project.logs.list(execution_id=exec_id, page_size=20)
                                    
                                    if exec_logs and len(exec_logs) > 0:
                                        # Log the most recent log entries
                                        for log_entry in exec_logs[-5:]:  # Last 5 log entries
                                            log_msg = log_entry.get('message', '')
                                            log_level = log_entry.get('level', 'INFO')
                                            log_func = log_entry.get('function_name', '')
                                            
                                            if log_msg:
                                                log_display = f"[Execution {exec_id[:8]}...] {log_func}: {log_msg}"
                                                if progress_callback:
                                                    progress_callback('execute_pipeline_batch', 'running', overall_progress, log_display)
                                                logger.info(f"Execution log: {log_display}")
                                    
                                    # Also log execution status
                                    exec_status = execution.status
                                    if exec_status in ['completed', 'success', 'failed']:
                                        status_msg = f"Execution {exec_id[:8]}... {exec_status}"
                                        if progress_callback:
                                            progress_callback('execute_pipeline_batch', 'running', overall_progress, status_msg)
                                        logger.info(status_msg)
                                        
                                except Exception as log_error:
                                    # Log fetching is optional, don't fail the whole polling
                                    logger.debug(f"Could not fetch logs for execution {exec_id}: {log_error}")
                                    
                    except Exception as exec_error:
                        # Execution log fetching is optional
                        logger.debug(f"Could not fetch execution logs: {exec_error}")
                    
                    # Update progress with detailed counts - ALWAYS call callback to ensure UI updates
                    if pipeline_stats:
                        progress_msg = f"Progress: {completed}/{items_count} items completed ({progress_pct:.1f}%) | Success: {success_count} | Failed: {failed_count} | In Progress: {count_in_progress} | Elapsed: {elapsed_str}"
                    else:
                        # Statistics not available yet - show waiting message
                        progress_msg = f"Progress: Waiting for statistics... | Elapsed: {elapsed_str}"
                    
                    if progress_callback:
                        progress_callback('execute_pipeline_batch', 'running', overall_progress, progress_msg)
                    logger.info(f"[Batch Progress] {progress_msg}")
                    last_completed = completed
                    
                    # Check if complete (only if we have stats)
                    if pipeline_stats:
                        # Only break if ALL items are completed (completed >= items_count)
                        if pipeline_status == 'completed' and completed >= items_count:
                            final_msg = f"Batch execution completed: {completed}/{items_count} items processed | Success: {success_count} | Failed: {failed_count} | Time: {elapsed_str}"
                            logger.info(f"[Poll Pipeline State] Pipeline completed: {final_msg}")
                            if progress_callback:
                                progress_callback('execute_pipeline_batch', 'completed', 95, final_msg)
                            break
                        elif pipeline_status == 'failed':
                            final_msg = f"Batch execution failed: {completed}/{items_count} items processed | Success: {success_count} | Failed: {failed_count} | Time: {elapsed_str}"
                            logger.error(f"[Poll Pipeline State] Pipeline failed: {final_msg}")
                            if progress_callback:
                                progress_callback('execute_pipeline_batch', 'failed', 95, final_msg, error="Pipeline execution failed")
                            break
                
                time_module.sleep(poll_interval)
                
            except Exception as e:
                logger.warning(f"Error polling pipeline state progress: {e}")
                time_module.sleep(poll_interval)
    
    def run_full_stress_test(self,
                             max_images: int = 50000,
                             dataset_id: str = None,
                             project_id: str = None,
                             dataset_name: str = None,
                             driver_id: str = None,
                             pipeline_name: str = None,
                             num_workers: int = 50,
                             pipeline_concurrency: int = 30,
                             pipeline_max_replicas: int = 12,
                             stream_image_concurrency: int = None,
                             stream_image_max_replicas: int = None,
                             resnet_concurrency: int = None,
                             resnet_max_replicas: int = None,
                             coco_dataset: str = 'all',
                             create_dataset: bool = False,
                             skip_download: bool = False,
                             skip_link_items: bool = False,
                             skip_pipeline: bool = False,
                             skip_execute: bool = False,
                             link_base_url: str = None,
                             progress_callback=None) -> dict:
        """
        Run the full stress test flow:
        0. (Optional) Create dataset if it doesn't exist
        1. Download COCO images to NFS (up to 123k from train+val) - AUTO-SKIPPED if dataset has items
        2. Create link items in dataset - AUTO-SKIPPED if dataset has items
        3. Create pipeline
        4. Execute pipeline batch
        
        Args:
            max_images: Number of images to download (default: 50000)
            dataset_id: Dataset ID for link items (will be created if create_dataset=True and doesn't exist).
                       If dataset already has items, download and link_items steps will be auto-skipped.
            project_id: Project ID (required if creating dataset and default_dataset_id doesn't exist)
            dataset_name: Dataset name (used if creating dataset)
            driver_id: Driver ID for dataset creation (mandatory)
            pipeline_name: Name for the pipeline
            num_workers: Number of parallel download workers
            coco_dataset: 'train2017' (118k), 'val2017' (5k), or 'all' (123k)
            create_dataset: Create dataset if it doesn't exist (default: False)
            skip_download: Skip image download step (also auto-skipped if dataset has items)
            skip_link_items: Skip link item creation (also auto-skipped if dataset has items)
            skip_pipeline: Skip pipeline creation
            skip_execute: Skip pipeline execution
            link_base_url: Base URL for link items (overrides instance default)
            stream_image_concurrency: Code node (stream-image) concurrency (default: pipeline_concurrency)
            stream_image_max_replicas: Code node max replicas (default: pipeline_max_replicas)
            resnet_concurrency: ResNet node concurrency (default: pipeline_concurrency)
            resnet_max_replicas: ResNet node max replicas (default: pipeline_max_replicas)
        
        Returns:
            dict with full results
        """
        # Default per-node concurrency/replicas from pipeline-wide values
        _stream_image_concurrency = stream_image_concurrency if stream_image_concurrency is not None else pipeline_concurrency
        _stream_image_max_replicas = stream_image_max_replicas if stream_image_max_replicas is not None else pipeline_max_replicas
        _resnet_concurrency = resnet_concurrency if resnet_concurrency is not None else 10
        _resnet_max_replicas = resnet_max_replicas if resnet_max_replicas is not None else pipeline_max_replicas

        # Use storage path from Link Base URL when provided (no date: reuse existing downloads)
        effective_storage_path = _storage_path_from_link_base_url(link_base_url) if link_base_url else None
        if effective_storage_path is None:
            effective_storage_path = self.storage_path
        results = {
            'date': self.date_str,
            'storage_path': effective_storage_path,
            'max_images': max_images,
            'steps': []
        }
        
        # Step 0: Handle dataset creation/selection
        # When dataset_id is empty, we always create a new dataset (since we don't specify it in the test)
        # But first, check storage and existing datasets to determine if we need to download more images
        
        # Check actual files in storage
        actual_files_on_disk = 0
        if os.path.exists(effective_storage_path):
            actual_files_on_disk = len([f for f in os.listdir(effective_storage_path) 
                                       if f.lower().endswith(('.jpg', '.jpeg', '.png'))])
        
        logger.info(f"Storage check: {actual_files_on_disk} files found in {effective_storage_path}")
        
        # Track if dataset_id was originally empty (before any creation)
        original_dataset_id_empty = (dataset_id is None or dataset_id == '')
        
        # If dataset_id is empty, check if datasets exist in project and compare counts
        need_new_dataset = False
        dataset_has_items = False
        items_count = 0
        existing_dataset_id = None
        
        if create_dataset and original_dataset_id_empty:
            # Dataset field is empty - check if datasets exist in project
            if project_id:
                try:
                    project = dl.projects.get(project_id=project_id)
                    datasets = list(project.datasets.list().all())
                    
                    if datasets:
                        logger.info(f"Found {len(datasets)} existing dataset(s) in project")
                        # Check each dataset to see if items count matches storage files count
                        for ds in datasets:
                            try:
                                filters = dl.Filters(resource=dl.FiltersResource.ITEM)
                                filters.add(field='hidden', values=False)
                                filters.add(field='type', values='file')
                                ds_items_count = ds.items.list(filters=filters).items_count
                                
                                logger.info(f"Dataset '{ds.name}' ({ds.id}) has {ds_items_count} items, storage has {actual_files_on_disk} files")
                                
                                if ds_items_count != actual_files_on_disk:
                                    need_new_dataset = True
                                    logger.info(f"⚠️  Dataset items count ({ds_items_count}) does not match storage files count ({actual_files_on_disk})")
                                    logger.info(f"   → Will create a new dataset to ensure consistency")
                                    break
                                else:
                                    # Items match, but we still need a new dataset since dataset_id is empty
                                    # (user didn't specify which dataset to use)
                                    need_new_dataset = True
                                    logger.info(f"✓ Dataset items count matches storage, but dataset_id is empty")
                                    logger.info(f"   → Will create a new dataset since no specific dataset was specified")
                                    break
                            except Exception as e:
                                logger.warning(f"Could not check items in dataset {ds.id}: {e}")
                                need_new_dataset = True
                                break
                    else:
                        logger.info("No existing datasets found in project - will create new dataset")
                        need_new_dataset = True
                except Exception as e:
                    logger.warning(f"Could not list datasets in project: {e}")
                    need_new_dataset = True
            else:
                logger.info("No project_id provided - will create new dataset")
                need_new_dataset = True
        
        # Track if we're creating a new dataset (will be used later)
        was_newly_created = False
        
        # Create dataset if needed
        if create_dataset:
            if progress_callback:
                progress_callback('create_dataset', 'running', 5, 'Creating/Getting dataset...')
            logger.info("=" * 60)
            logger.info("STEP 0: Creating/Getting dataset")
            logger.info("=" * 60)
            
            if need_new_dataset or original_dataset_id_empty:
                was_newly_created = True
                if need_new_dataset:
                    logger.info("📝 Creating new dataset:")
                    if actual_files_on_disk > 0:
                        logger.info(f"   Reason: Dataset field is empty and existing datasets don't match storage")
                        logger.info(f"   Storage has {actual_files_on_disk} files, will create fresh dataset")
                    else:
                        logger.info(f"   Reason: Dataset field is empty - creating new dataset for this test run")
                else:
                    logger.info("📝 Creating new dataset (dataset_id was empty)")
            
            # When creating a new dataset (because dataset_id was empty), use a unique name to avoid reusing old datasets
            if need_new_dataset or original_dataset_id_empty:
                # Add timestamp to ensure unique dataset name
                import time as time_module
                unique_suffix = time_module.strftime('%Y%m%d-%H%M%S')
                if dataset_name:
                    unique_dataset_name = f"{dataset_name}-{unique_suffix}"
                else:
                    unique_dataset_name = f'stress-test-dataset-{self.date_str}-{unique_suffix}'
                logger.info(f"Using unique dataset name: {unique_dataset_name} (to avoid reusing existing datasets)")
            else:
                unique_dataset_name = dataset_name
            
            dataset_result = self.create_dataset(
                project_id=project_id, 
                dataset_name=unique_dataset_name,
                driver_id=driver_id
            )
            results['steps'].append({
                'step': 'create_dataset',
                'result': dataset_result
            })
            
            if 'error' in dataset_result:
                logger.error(f"Failed to create/get dataset: {dataset_result['error']}")
                results['error'] = dataset_result['error']
                if progress_callback:
                    progress_callback('create_dataset', 'failed', 5, f"Failed: {dataset_result['error']}", error=dataset_result['error'])
                return results
            
            dataset_id = dataset_result['dataset_id']
            results['dataset_id'] = dataset_id
            logger.info(f"Using dataset: {dataset_id}")
            if progress_callback:
                progress_callback('create_dataset', 'completed', 10, f'Dataset ready: {dataset_id}')
        else:
            # Not creating dataset: use provided dataset_id only (do not substitute default when user left field empty)
            if original_dataset_id_empty:
                dataset_id = None  # User left dataset field empty and did not create - no dataset
            else:
                dataset_id = (dataset_id or '').strip() if dataset_id else ''
                dataset_id = dataset_id if dataset_id else None
                if dataset_id is None:
                    dataset_id = self.default_dataset_id
            results['dataset_id'] = dataset_id
            if progress_callback:
                if dataset_id:
                    progress_callback('create_dataset', 'skipped', 5, f'Using existing dataset: {dataset_id}')
                else:
                    progress_callback('create_dataset', 'skipped', 5, 'No dataset specified (download only, no link items)')
        
        # Check if dataset exists and has items - if so, skip download and link_items
        # BUT: if we just created a new dataset (because dataset_id was empty), it won't have items yet
        if dataset_id and not was_newly_created:
            try:
                dataset = dl.datasets.get(dataset_id=dataset_id)
                # Check if dataset has items in /stress-test/ folder (any date)
                filters = dl.Filters(resource=dl.FiltersResource.ITEM)
                filters.add(field='hidden', values=False)
                filters.add(field='type', values='file')
                filters.add(field='dir', values='/stress-test/*')
                items_count = dataset.items.list(filters=filters).items_count
                
                if items_count == 0:
                    # Try without folder filter to check total items
                    filters2 = dl.Filters(resource=dl.FiltersResource.ITEM)
                    filters2.add(field='hidden', values=False)
                    filters2.add(field='type', values='file')
                    items_count = dataset.items.list(filters=filters2).items_count
                
                dataset_has_items = items_count > 0
                
                if dataset_has_items:
                    logger.info(f"Dataset {dataset_id} already has {items_count} items - will skip download and link_items steps")
                    if progress_callback:
                        progress_callback('dataset_check', 'completed', 10, f'Dataset has {items_count} items - skipping download and link creation')
            except Exception as e:
                logger.warning(f"Could not check dataset items: {e}")
                dataset_has_items = False
        elif was_newly_created:
            logger.info(f"Newly created dataset {dataset_id} - will proceed with download and link_items steps")
            dataset_has_items = False  # New dataset won't have items yet
        
        filenames = []
        
        # Step 1: Download images
        # When dataset_id was empty (and we created a new dataset), check if we need to download more images
        # Compare max_images with actual_files_on_disk
        need_more_downloads = False
        images_to_download = max_images
        
        if was_newly_created:
            # Dataset field was empty - check if we need more downloads
            if max_images > actual_files_on_disk:
                need_more_downloads = True
                images_to_download = max_images - actual_files_on_disk
                logger.info(f"📥 Download check: Max Images={max_images}, Storage has={actual_files_on_disk}")
                logger.info(f"   → Need to download {images_to_download} more images to reach {max_images} total")
            else:
                logger.info(f"✓ Download check: Max Images={max_images}, Storage has={actual_files_on_disk}")
                logger.info(f"   → Storage already has enough files (or more), no additional download needed")
        
        # Auto-skip if dataset has items (unless explicitly requested or we need more downloads)
        should_skip_download = (skip_download or dataset_has_items) and not need_more_downloads
        
        if not should_skip_download:
            if need_more_downloads:
                # Download only the remaining needed images
                if progress_callback:
                    progress_callback('download_images', 'running', 15, f'Downloading {images_to_download} additional images (need {max_images} total, have {actual_files_on_disk})...')
                logger.info("=" * 60)
                logger.info(f"STEP 1: Downloading {images_to_download} additional COCO images")
                logger.info(f"        (Target: {max_images} total, Currently: {actual_files_on_disk} in storage)")
                logger.info("=" * 60)
                
                # Download the remaining images (create link items on the fly when we will do link items)
                do_link_on_download = not (skip_link_items or dataset_has_items)
                download_result = self.download_images(
                    max_images=max_images, num_workers=num_workers, dataset=coco_dataset, progress_callback=progress_callback,
                    create_link_on_download=do_link_on_download,
                    dataset_id=dataset_id, link_base_url=link_base_url, project_id=project_id,
                    workflow_id=getattr(self, '_current_workflow_id', None)
                )
            else:
                # Normal download
                if progress_callback:
                    progress_callback('download_images', 'running', 15, f'Downloading {max_images} COCO images...')
                logger.info("=" * 60)
                logger.info(f"STEP 1: Downloading {max_images} COCO images")
                logger.info("=" * 60)
                do_link_on_download = not (skip_link_items or dataset_has_items)
                download_result = self.download_images(
                    max_images=max_images, num_workers=num_workers, dataset=coco_dataset, progress_callback=progress_callback,
                    create_link_on_download=do_link_on_download,
                    dataset_id=dataset_id, link_base_url=link_base_url, project_id=project_id,
                    workflow_id=getattr(self, '_current_workflow_id', None)
                )
            
            results['steps'].append({
                'step': 'download_images',
                'result': download_result
            })
            wf_id = getattr(self, '_current_workflow_id', None)
            if wf_id and workflow_progress.get(wf_id, {}).get('status') == 'cancelling':
                logger.info("Workflow cancelled after download step - exiting early")
                return results
            filenames = download_result.get('files', [])
            # Limit filenames to max_images (download_images returns all files in storage, not just downloaded ones)
            if len(filenames) < max_images:
                logger.warning(f"Shortfall: requested {max_images} images but only {len(filenames)} files in storage (missing {max_images - len(filenames)}). Check download/link step for failures.")
            filenames = sorted(filenames)[:max_images]
            if len(filenames) < max_images:
                logger.info(f"Using {len(filenames)} files (requested {max_images})")
            if progress_callback:
                progress_callback('download_images', 'completed', 40, f'Downloaded {len(filenames)} images')
        else:
            # Get existing files (use same path as download would use when link_base_url is set)
            if os.path.exists(effective_storage_path):
                all_filenames = sorted([f for f in os.listdir(effective_storage_path) 
                            if f.lower().endswith(('.jpg', '.jpeg', '.png'))])
                # Limit to max_images if we have more than needed (sorted for deterministic set)
                filenames = all_filenames[:max_images] if len(all_filenames) > max_images else all_filenames
                if len(all_filenames) > max_images:
                    logger.info(f"Limiting to {max_images} files from {len(all_filenames)} available in storage")
            else:
                filenames = []
            skip_reason = 'Dataset already has items' if dataset_has_items else 'Skipped by user'
            results['steps'].append({'step': 'download_images', 'skipped': True, 'reason': skip_reason, 'existing_files': len(filenames), 'dataset_items': items_count})
            if progress_callback:
                if dataset_has_items:
                    progress_callback('download_images', 'skipped', 15, f'Skipped: Dataset already has {items_count} items')
                else:
                    progress_callback('download_images', 'skipped', 15, f'Using {len(filenames)} existing images')
        
        # Step 2: Create link items (only when download step was skipped - otherwise links are already ensured in the process-pool)
        # When we ran the download step, each worker ensures file exists + link item with correct URL (create or overwrite) for every item
        should_skip_link_items = skip_link_items or dataset_has_items or (not should_skip_download)
        if not should_skip_link_items:
            if progress_callback:
                progress_callback('create_link_items', 'running', 45, f'Creating {len(filenames)} link items...')
            logger.info("=" * 60)
            logger.info(f"STEP 2: Creating {len(filenames)} link items (download was skipped)")
            logger.info("=" * 60)
            
            link_result = self.create_link_items(
                dataset_id=dataset_id, 
                filenames=filenames, 
                num_workers=num_workers,
                progress_callback=progress_callback,
                link_base_url=link_base_url,
                project_id=project_id,
                workflow_id=getattr(self, '_current_workflow_id', None)
            )
            results['steps'].append({
                'step': 'create_link_items',
                'result': link_result
            })
            wf_id = getattr(self, '_current_workflow_id', None)
            if wf_id and workflow_progress.get(wf_id, {}).get('status') == 'cancelling':
                logger.info("Workflow cancelled after link items step - exiting early")
                return results
            if progress_callback:
                created = link_result.get('created', 0)
                progress_callback('create_link_items', 'completed', 60, f'Created {created} link items')
        else:
            if not should_skip_download:
                skip_reason = 'Link items already ensured in download step (process-pool)'
            else:
                skip_reason = 'Dataset already has items' if dataset_has_items else 'Skipped by user'
            results['steps'].append({'step': 'create_link_items', 'skipped': True, 'reason': skip_reason, 'dataset_items': items_count})
            if progress_callback:
                if not should_skip_download:
                    progress_callback('create_link_items', 'skipped', 60, 'Link items ensured in download step')
                elif dataset_has_items:
                    progress_callback('create_link_items', 'skipped', 45, f'Skipped: Dataset already has {items_count} items')
                else:
                    progress_callback('create_link_items', 'skipped', 45, 'Skipped link items creation')
        
        # Step 3: Create pipeline (requires dataset_id)
        pipeline_id = None
        logger.info("=" * 60)
        logger.info(f"STEP 3: Checking pipeline creation - skip_pipeline={skip_pipeline}, dataset_id={dataset_id!r}")
        logger.info("=" * 60)
        if progress_callback:
            progress_callback('pipeline_check', 'running', 60, f'Checking pipeline creation: skip_pipeline={skip_pipeline}')
            logger.info(f"[Progress] Sent pipeline_check update: skip_pipeline={skip_pipeline}")
        
        if not skip_pipeline and dataset_id:
            logger.info(f"[Step 3] Pipeline creation NOT skipped - proceeding with creation")
            if progress_callback:
                progress_callback('create_pipeline', 'running', 65, 'Creating pipeline with ResNet...')
            logger.info("=" * 60)
            logger.info("STEP 3: Creating pipeline")
            logger.info("=" * 60)
            
            try:
                pipeline_result = self.create_pipeline(
                    pipeline_name=pipeline_name,
                    project_id=project_id,
                    pipeline_concurrency=pipeline_concurrency,
                    pipeline_max_replicas=pipeline_max_replicas,
                    stream_image_concurrency=_stream_image_concurrency,
                    stream_image_max_replicas=_stream_image_max_replicas,
                    resnet_concurrency=_resnet_concurrency,
                    resnet_max_replicas=_resnet_max_replicas,
                    progress_callback=progress_callback
                )
            except Exception as create_err:
                logger.error(f"create_pipeline raised: {create_err}", exc_info=True)
                pipeline_result = {'error': str(create_err), 'pipeline_id': None}
            logger.info(f"create_pipeline returned: {pipeline_result}")
            results['steps'].append({
                'step': 'create_pipeline',
                'result': pipeline_result
            })
            # Always use pipeline_id when present so batch execution can run even if install had issues
            pipeline_id = pipeline_result.get('pipeline_id') if isinstance(pipeline_result, dict) else None
            logger.info(f"Extracted pipeline_id: {pipeline_id}")
            if progress_callback:
                if pipeline_result and pipeline_result.get('error'):
                    error_msg = pipeline_result.get('error', 'Unknown error')
                    logger.error(f"Pipeline creation failed: {error_msg}")
                    progress_callback('create_pipeline', 'failed', 65, f"Failed: {error_msg}", error=error_msg)
                else:
                    logger.info(f"Pipeline created successfully: {pipeline_id}")
                    progress_callback('create_pipeline', 'completed', 80, f'Pipeline created: {pipeline_id}')
                    # Store pipeline_id in workflow progress for cancellation
                    if hasattr(self, '_current_workflow_id') and self._current_workflow_id:
                        if self._current_workflow_id in workflow_progress:
                            if workflow_progress[self._current_workflow_id].get('result') is None:
                                workflow_progress[self._current_workflow_id]['result'] = {}
                            workflow_progress[self._current_workflow_id]['result']['pipeline_id'] = pipeline_id
        else:
            skip_reason = 'skip_pipeline=True' if skip_pipeline else 'No dataset specified (dataset_id required for pipeline)'
            logger.warning("=" * 60)
            logger.warning(f"STEP 3: Pipeline creation SKIPPED - {skip_reason}")
            logger.warning("=" * 60)
            if progress_callback:
                progress_callback('create_pipeline', 'skipped', 65, f'Skipped: {skip_reason}')
                logger.info(f"[Progress] Sent create_pipeline skipped update")
            results['steps'].append({'step': 'create_pipeline', 'skipped': True, 'reason': skip_reason})
            logger.warning(f"[Step 3] Pipeline creation was skipped - pipeline_id will be None")
        
        # Step 4: Execute pipeline batch
        logger.info("=" * 60)
        logger.info(f"STEP 4: Checking batch execution - skip_execute={skip_execute}, pipeline_id={pipeline_id}")
        logger.info("=" * 60)
        if progress_callback:
            progress_callback('execute_check', 'running', 82, f'Checking execution conditions: skip_execute={skip_execute}, pipeline_id={pipeline_id}')
        
        if not skip_execute and pipeline_id and dataset_id:
            logger.info(f"✓ Conditions met: skip_execute=False, pipeline_id={pipeline_id}, dataset_id={dataset_id} - proceeding with batch execution")
            if progress_callback:
                progress_callback('execute_pipeline_batch', 'running', 85, 'Executing pipeline batch...')
            logger.info("=" * 60)
            logger.info("STEP 4: Executing pipeline batch")
            logger.info("=" * 60)
            
            logger.info(f"Starting batch execution for pipeline {pipeline_id} on dataset {dataset_id}")
            if progress_callback:
                progress_callback('execute_pipeline_batch', 'running', 85, f'Starting batch execution on pipeline {pipeline_id}...')
            
            logger.info(f"[Workflow] About to call execute_pipeline_batch - this will wait for batch execution to complete")
            execute_result = self.execute_pipeline_batch(
                pipeline_id=pipeline_id,
                dataset_id=dataset_id,
                progress_callback=progress_callback
            )
            
            logger.info(f"[Workflow] execute_pipeline_batch returned: {execute_result}")
            logger.info(f"[Workflow] Checking if batch execution actually completed...")
            
            results['steps'].append({
                'step': 'execute_pipeline_batch',
                'result': execute_result
            })
            
            if progress_callback:
                items_count = execute_result.get('items_count', 0)
                if execute_result.get('timeout'):
                    logger.warning(f"[Workflow] Batch execution timed out after 24 hours")
                    progress_callback('execute_pipeline_batch', 'failed', 95, f'Batch execution timed out after 24 hours', error='Timeout')
                elif 'error' in execute_result:
                    error_msg = execute_result.get('error', 'Unknown error')
                    logger.error(f"[Workflow] Batch execution failed: {error_msg}")
                    progress_callback('execute_pipeline_batch', 'failed', 95, f"Failed: {error_msg}", error=error_msg)
                elif execute_result.get('success'):
                    logger.info(f"[Workflow] Batch execution completed successfully on {items_count} items")
                    progress_callback('execute_pipeline_batch', 'completed', 95, f'Batch execution completed on {items_count} items')
                else:
                    logger.warning(f"[Workflow] Batch execution returned but success status unclear: {execute_result}")
                    progress_callback('execute_pipeline_batch', 'completed', 95, f'Batch execution finished (status unclear)')
        else:
            if skip_execute:
                skip_reason = 'skip_execute=True'
            elif not dataset_id:
                skip_reason = 'No dataset specified (dataset_id required for execution)'
            elif not pipeline_id:
                skip_reason = 'pipeline_id is None (pipeline creation may have failed)'
            else:
                skip_reason = 'Conditions not met'
            logger.warning("=" * 60)
            logger.warning(f"STEP 4: Pipeline execution SKIPPED - {skip_reason}")
            logger.warning(f"STEP 4: skip_execute={skip_execute}, pipeline_id={pipeline_id}")
            logger.warning("=" * 60)
            if progress_callback:
                progress_callback('execute_pipeline_batch', 'skipped', 85, f'Skipped pipeline execution: {skip_reason}')
                logger.info(f"[Progress] Sent execute_pipeline_batch skipped update: {skip_reason}")
            results['steps'].append({'step': 'execute_pipeline_batch', 'skipped': True, 'reason': skip_reason})
            logger.warning(f"[Step 4] Batch execution was skipped - workflow will complete without executing batch")
        
        # Check if we actually executed the batch
        executed_batch = any(step.get('step') == 'execute_pipeline_batch' and not step.get('skipped') for step in results['steps'])
        logger.info("=" * 60)
        logger.info(f"Workflow completion check: executed_batch={executed_batch}")
        steps_summary = [f"{s.get('step')}: {'skipped' if s.get('skipped') else 'executed'}" for s in results['steps']]
        logger.info(f"All steps: {steps_summary}")
        logger.info("=" * 60)
        
        if not executed_batch:
            logger.warning("=" * 60)
            logger.warning("WARNING: Pipeline batch execution was not performed!")
            logger.warning("WARNING: Check if pipeline was created and skip_execute flag.")
            logger.warning("=" * 60)
            if progress_callback:
                progress_callback('warning', 'warning', 100, 'Workflow completed but batch execution was not performed', result=results)
        
        results['success'] = True
        logger.info("=" * 60)
        logger.info("STRESS TEST COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Steps completed: {[step.get('step') for step in results['steps']]}")
        logger.info(f"Executed batch: {executed_batch}")
        logger.info("=" * 60)
        
        if progress_callback:
            completion_msg = 'Workflow completed successfully' if executed_batch else 'Workflow completed but batch execution was not performed'
            progress_callback('complete', 'completed', 100, completion_msg, result=results)
            logger.info(f"[Progress] Sent workflow completion update: {completion_msg}")
        
        return results
    
    def run(self):
        """This method is called by Dataloop, but for custom servers we start in __init__"""
        # Server is already started in __init__, just log that we're ready
        logger.info("StressTestServer.run() called - server should already be running")
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down server...")
            if self.server:
                self.server.shutdown()


if __name__ == '__main__':
    # For local testing
    server = StressTestServer()
    server.run()
