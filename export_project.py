#!/usr/bin/env python3

import os
import zipfile
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
EXPORT_DIR = os.path.expanduser("~/relationship_engine_exports")

EXCLUDE = {
    ".env",
    ".git",
    "private_data",
    "__pycache__",
    ".venv",
    "venv"
}

def should_exclude(path):
    parts = set(path.split(os.sep))
    return bool(parts & EXCLUDE) or path.endswith(".db")

def main():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = os.path.join(
        EXPORT_DIR,
        f"relationship_engine_export_{timestamp}.zip"
    )

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for foldername, subfolders, filenames in os.walk(PROJECT_ROOT):
            rel_folder = os.path.relpath(foldername, PROJECT_ROOT)
            if should_exclude(rel_folder):
                continue

            for filename in filenames:
                rel_file = os.path.join(rel_folder, filename)
                if should_exclude(rel_file):
                    continue

                abs_file = os.path.join(foldername, filename)
                zipf.write(abs_file, rel_file)

    print(f"Export created at: {zip_path}")

if __name__ == "__main__":
    main()
