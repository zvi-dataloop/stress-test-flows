"""
Standalone worker for process-pool image download + link item creation.
This module is imported by child processes only, so it must not import main.py.
"""
import os
import json
import io
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import dtlpy as dl


def _remote_path_from_link_base_url(link_base_url: str, date_str: str) -> str:
    if not link_base_url or not date_str:
        return f'/stress-test/{date_str}'
    parsed = urlparse(link_base_url)
    path = (parsed.path or '').strip('/')
    if path.endswith('/' + date_str):
        path = path[:-len(date_str) - 1].rstrip('/')
    parts = [p for p in path.split('/') if p]
    folder = parts[-1] if parts else 'stress-test'
    return f'/{folder}/{date_str}'


def _upload_one_link_item_standalone(dataset, filename: str, link_base_url_full: str, date_str: str, overwrite: bool = True):
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
    remote_path = _remote_path_from_link_base_url(link_base_url_full, date_str)
    dataset.items.upload(
        local_path=buffer,
        remote_path=remote_path,
        remote_name=json_filename,
        overwrite=overwrite
    )


def run_download_chunk(args):
    """
    Entry point for process pool: download a chunk of URLs and create link items when requested.
    Args: (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id, date_str, workflow_id, create_link, threads_per_process)
    Returns: (downloaded, failed, skipped, error_str or None)
    """
    log = logging.getLogger('stress-test-server')
    try:
        (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id, date_str,
         workflow_id, create_link, threads_per_process) = args
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
                if link_dataset is not None and link_base_url_full and not _is_cancelled():
                    try:
                        _upload_one_link_item_standalone(link_dataset, filename, link_base_url_full, date_str, overwrite=True)
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
        log.exception("Worker chunk failed with exception")
        return ([], [], [], str(e))
