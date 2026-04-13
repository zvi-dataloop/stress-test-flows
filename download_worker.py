"""
Standalone worker for process-pool image download + optional dataset upload.
This module is imported by child processes only, so it must not import main.py.
upload_mode: 'none' | 'link' | 'file'
  - none: download only to storage_path
  - link: download + link JSON items (legacy NFS + URL)
  - file: download to temp path + dataset.items.upload binary file, then delete local file
"""
import os
import json
import io
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import dtlpy as dl


def _remote_path_from_link_base_url(link_base_url: str) -> str:
    """Derive remote path from Link Base URL. Link URL = serve-agent path + path inside storage; strip serve-agent/."""
    if not link_base_url:
        return '/stress-test'
    parsed = urlparse(link_base_url)
    path = (parsed.path or '').strip('/')
    if not path:
        return '/stress-test'
    if path.startswith('serve-agent/'):
        path = path[len('serve-agent/'):].lstrip('/')
    parts = [p for p in path.split('/') if p]
    folder = parts[-1] if parts else 'stress-test'
    return f'/{folder}'


def _upload_one_link_item_standalone(dataset, filename: str, link_base_url_full: str, overwrite: bool = True):
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


def _upload_one_file_to_dataset_standalone(dataset, filepath: str, filename: str, remote_path: str = '/'):
    dataset.items.upload(
        local_path=filepath,
        remote_path=remote_path,
        remote_name=filename,
        overwrite=True,
    )


def run_download_chunk(args):
    """
    Entry point for process pool.
    Args: (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id, workflow_id, upload_mode, threads_per_process [, progress_queue])
    upload_mode: 'none' | 'link' | 'file'
    progress_queue: optional; when provided, put(1) per completed item so main process can show accurate progress.
    Returns: (downloaded, failed, skipped, error_str or None)
    """
    log = logging.getLogger('stress-test-server')
    try:
        parts = list(args)
        progress_queue = parts.pop() if len(parts) == 9 else None
        (chunk_urls, storage_path, link_base_url_full, dataset_id, project_id,
         workflow_id, upload_mode, threads_per_process) = parts
        upload_mode = upload_mode or 'none'
        cancel_file = f"/tmp/stress_cancel_{workflow_id}" if workflow_id else None

        def _is_cancelled():
            return cancel_file and os.path.exists(cancel_file)

        ds = None
        if upload_mode == 'file' and dataset_id:
            try:
                pid = project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
                if pid:
                    project = dl.projects.get(project_id=pid)
                    ds = project.datasets.get(dataset_id=dataset_id)
                else:
                    ds = dl.datasets.get(dataset_id=dataset_id)
            except Exception as e:
                log.warning(f"Could not get dataset in worker: {e}")
        elif upload_mode == 'link' and dataset_id and link_base_url_full:
            try:
                pid = project_id or os.environ.get('PROJECT_ID') or os.environ.get('DL_PROJECT_ID')
                if pid:
                    project = dl.projects.get(project_id=pid)
                    ds = project.datasets.get(dataset_id=dataset_id)
                else:
                    ds = dl.datasets.get(dataset_id=dataset_id)
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

                if upload_mode == 'file':
                    if ds is None or _is_cancelled():
                        if os.path.isfile(filepath):
                            try:
                                os.remove(filepath)
                            except OSError:
                                pass
                        return {'success': False, 'url': url, 'error': 'no_dataset'}
                    try:
                        _upload_one_file_to_dataset_standalone(ds, filepath, filename, '/')
                    except Exception as up_err:
                        log.warning(f"File upload failed for {filename}: {up_err}")
                        try:
                            os.remove(filepath)
                        except OSError:
                            pass
                        return {'success': False, 'url': url, 'error': str(up_err)}
                    try:
                        os.remove(filepath)
                    except OSError:
                        pass
                    return {'success': True, 'filename': filename, 'path': None, 'skipped': False}

                if upload_mode == 'link' and ds is not None and link_base_url_full and not _is_cancelled():
                    try:
                        _upload_one_link_item_standalone(ds, filename, link_base_url_full, overwrite=True)
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
                if progress_queue is not None:
                    try:
                        progress_queue.put(1)
                    except Exception:
                        pass
        return (downloaded, failed, skipped, None)
    except Exception as e:
        log.exception("Worker chunk failed with exception")
        return ([], [], [], str(e))
