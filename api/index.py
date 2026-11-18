# Vercel entry point: Vercel's Python runtime serves the WSGI `app` below.
# Settings come from Vercel → Project → Settings → Environment Variables:
#   PM_PASSWORD, TURSO_DATABASE_URL, TURSO_AUTH_TOKEN
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import application  # noqa: E402

app = application
