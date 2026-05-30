import os
from dotenv import load_dotenv

load_dotenv(".env.local" if os.path.exists(".env.local") else ".env.docker")

REDIS_URL = os.environ["REDIS_URL"]
DATA_ROOT = os.environ["DATA_ROOT"]
PORT = int(os.getenv("PORT", "8765"))
