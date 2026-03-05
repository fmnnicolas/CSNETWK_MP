# util.py
import base64
from pathlib import Path

def b64_encode_file(path: str) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("ascii")

def b64_save(b64: str, out_path: str) -> None:
    Path(out_path).write_bytes(base64.b64decode(b64.encode("ascii")))
