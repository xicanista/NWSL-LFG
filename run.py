from core.logger import get_logger
from core.ingest import run_ingestion

# Force file logging enabled here
logger = get_logger(__name__, log_to_file=True)

def main():
    logger.info("⚽ Starting NWSL data ingestion...")
    run_ingestion()
    logger.info("✅ Done.")

if __name__ == "__main__":
    main()
