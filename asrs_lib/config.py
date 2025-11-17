import os
from urllib.parse import urlparse
from dotenv import load_dotenv

def load():
    # Load environment variables from .env file
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(dotenv_path=env_path)

    # Validate OPC UA endpoint configuration
    endpoint = os.getenv("OPCUA_ENDPOINT", "").strip()
    if not endpoint:
        raise RuntimeError("OPCUA_ENDPOINT not set in .env")
    u = urlparse(endpoint)
    if u.scheme != "opc.tcp" or not u.hostname or not u.port:
        raise RuntimeError(f"Invalid OPC UA endpoint: {endpoint}")

    cfg = {
        "OPCUA_ENDPOINT": endpoint,
        "DB_HOST": os.getenv("DB_HOST", "127.0.0.1"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_USER": os.getenv("DB_USER", "postgres"),
        "DB_PASS": os.getenv("DB_PASS", "postgres"),
        "DB_NAME": os.getenv("DB_NAME", "asrs"),
        "API_HOST": os.getenv("API_HOST", "0.0.0.0"),
        "API_PORT": int(os.getenv("API_PORT", "8001")),
    }
    return cfg
