#!/usr/bin/env python3
"""
Generate coco_all_ids.json from COCO annotations (one-time ~250MB download).
Run from project root: python scripts/generate_coco_ids.py
Then commit the generated coco_all_ids.json so download_images skips the slow annotations download.
"""
import os
import json
import zipfile
import io
import requests

URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'coco_all_ids.json')

def main():
    print("Downloading COCO annotations (~250MB)...")
    response = requests.get(URL, timeout=600)
    response.raise_for_status()
    print("Extracting image IDs...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open('annotations/instances_train2017.json') as f:
            train_annotations = json.load(f)
        with z.open('annotations/instances_val2017.json') as f:
            val_annotations = json.load(f)
    train_ids = [img['id'] for img in train_annotations.get('images', [])]
    val_ids = [img['id'] for img in val_annotations.get('images', [])]
    data = {'train2017': train_ids, 'val2017': val_ids}
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    print(f"Wrote {OUTPUT_PATH} ({len(train_ids)} train, {len(val_ids)} val). Commit this file to avoid the 6+ min annotations download on first run.")

if __name__ == '__main__':
    main()
