import schedule
import time
import subprocess
from datetime import datetime

def run_etl():
    """Run ETL pipeline"""
    print(f"[{datetime.now()}] Running ETL...")
    subprocess.run(['python', 'ingestion/main.py'], check=True)

# Schedule every day at 2 AM
schedule.every().day.at("02:00").do(run_etl)

# Run scheduler
while True:
    schedule.run_pending()
    time.sleep(60)