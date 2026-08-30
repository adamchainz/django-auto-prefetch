from __future__ import annotations

import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SECRET_KEY = "NOTASECRET"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
        "ATOMIC_REQUESTS": True,
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = ["tests"]

USE_TZ = True

# Enable auto-prefetching for ManyToMany and reverse ForeignKey relationships
AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS = True
