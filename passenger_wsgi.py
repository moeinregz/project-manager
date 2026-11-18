# -*- coding: utf-8 -*-
"""
فایل اجرای برنامه روی هاست (cPanel → Setup Python App، یا PythonAnywhere).
هاست این فایل رو اجرا می‌کنه، نه app.py رو مستقیم.

⚠️ رمز ورود رو همین پایین عوض کن. بدون رمز، برنامه روی اینترنت باز نمی‌شه.
"""
import os
import sys

# ↓↓↓ رمز ورودت رو اینجا بنویس (حداقل ۸ کاراکتر) ↓↓↓
PASSWORD = "CHANGE-ME"

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
# The host's control panel can also set PM_PASSWORD; that wins over the line above.
os.environ.setdefault("PM_PASSWORD", PASSWORD)
os.environ.setdefault("PM_DB", os.path.join(HERE, "projects.db"))

if len(os.environ["PM_PASSWORD"]) < 8 or os.environ["PM_PASSWORD"] == "CHANGE-ME":
    raise RuntimeError("رمز ورود تنظیم نشده: توی passenger_wsgi.py مقدار PASSWORD رو عوض کن (حداقل ۸ کاراکتر).")

from app import application  # noqa: E402
