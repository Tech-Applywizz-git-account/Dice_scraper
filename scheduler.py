import sys
from redis import Redis
from rq import Queue

from config import REDIS_URL
from api_client import get_active_clients
from worker import process_client
from database import init_db

def run_scheduler():
    print("Daily scraping started")
    
    # Initialize DB (create unique constraint if needed)
    try:
        init_db()
    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("Proceeding anyway, as the constraint might already exist...")
        
    if not REDIS_URL:
        print("CRITICAL ERROR: REDIS_URL is required to run the scheduler.")
        sys.exit(1)
        
    try:
        active_ids = get_active_clients()
    except Exception as e:
        print(f"Failed to fetch active clients: {e}")
        sys.exit(1)
        
    print(f"Active clients found: {len(active_ids)}")
    if not active_ids:
        print("No active clients found. Exiting.")
        sys.exit(0)
    
    redis_conn = Redis.from_url(REDIS_URL)
    q = Queue('default', connection=redis_conn)
    
    for awl_id in active_ids:
        # Enqueue each client ID as a separate task
        # result_ttl=0 ensures we don't store job results in Redis, saving memory on the free tier.
        q.enqueue(process_client, awl_id, job_timeout='1h', result_ttl=0, retry=None)
        print(f"Queued {awl_id}")
        
    print(f"Successfully queued {len(active_ids)} clients. Scheduler exiting.")

if __name__ == "__main__":
    run_scheduler()
