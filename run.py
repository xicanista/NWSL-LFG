from core.ingest import run_ingestion

def main():
    print("⚽ Loading NWSL data...")
    run_ingestion()
    print("✅ Done.")

if __name__ == "__main__":
    main()
