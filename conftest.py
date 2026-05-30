import os
import tempfile

os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("DATA_ROOT", f"{tempfile.gettempdir()}/test_stonks")
os.environ.setdefault("PORT", "8765")
