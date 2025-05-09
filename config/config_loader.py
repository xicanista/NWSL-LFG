# Load environment variables from .env file
load_dotenv()

# Config values
ASA_API_BASE = os.getenv("ASA_API_BASE")
DB_NAME = os.getenv("DB_NAME")
LOG_FILE = os.getenv("LOG_FILE", "app.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s'
)
