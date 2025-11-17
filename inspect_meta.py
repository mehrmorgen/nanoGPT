import pickle
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python inspect_meta.py <path_to_meta.pkl>")
    sys.exit(1)

meta_path = Path(sys.argv[1])

if not meta_path.exists():
    print(f"Error: {meta_path} not found.")
    sys.exit(1)

try:
    with open(meta_path, "rb") as f:
        meta = pickle.load(f)
    print(meta)
except Exception as e:
    print(f"Error reading pickle file: {e}")
    sys.exit(1)
