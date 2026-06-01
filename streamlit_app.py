"""Streamlit Community Cloud and Hugging Face Spaces entrypoint.

Hosted Streamlit runtimes expect a top-level entrypoint script. This file
forwards to the real dashboard so local, Docker, Streamlit Cloud, and Hugging
Face Spaces deployments share one codebase.
"""

from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

runpy.run_path(str(ROOT / "src" / "dashboard" / "app.py"), run_name="__main__")
