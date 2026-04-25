"""Vercel serverless entry point — wraps the Flask app."""
import sys
from pathlib import Path

# Ensure the project root is on the Python path so `app` can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import app  # noqa: E402
