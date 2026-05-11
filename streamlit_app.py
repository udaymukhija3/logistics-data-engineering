"""Streamlit Community Cloud entrypoint.

Streamlit Cloud expects a top-level entrypoint script. This file forwards to the
real dashboard so the Cloud deploy and the local/Render Docker deploy share one
codebase.

Usage:
    1. Push this repo to GitHub.
    2. On https://streamlit.io/cloud, "New app" -> pick this repo.
    3. Set "Main file path" to ``streamlit_app.py``.
    4. Set "Python version" to 3.11 and "Requirements file" to
       ``requirements-streamlit.txt``.

The bundled ``data/sample`` directory is committed, so the app renders without
any external infrastructure.
"""

from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

runpy.run_path(str(ROOT / "src" / "dashboard" / "app.py"), run_name="__main__")
