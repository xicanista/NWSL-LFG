from core.logger import get_logger
from core.ingest import run_ingestion

logger = get_logger(__name__)

def main():
    installdb = input("Would you like to set up the database? (Y/N) ")
    if installdb.lower() in ("y", "yes"):
        logger.info("⚽ Starting NWSL data ingestion...")
        run_ingestion()
        logger.info("✅ Ingestion complete.")
    else:
        print("The database will not be set up.")

if __name__ == "__main__":
    main()
