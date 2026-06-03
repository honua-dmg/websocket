import os
from dotenv import load_dotenv

load_dotenv(".env.local" if os.path.exists(".env.local") else ".env.docker")


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(
            f"Required environment variable '{name}' is not set. "
            f"Check .env.local or .env.docker."
        )
    return value


REDIS_URL = _require("REDIS_URL")
DATA_ROOT = _require("DATA_ROOT")
PORT = int(os.getenv("PORT", "8765"))
