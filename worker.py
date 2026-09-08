import argparse
import sys
import time
from rq import Worker
from redis import Redis

from config import REDIS_URL
from api_client import get_active_clients, get_client_details, extract_client_requirements, get_job_roles
from filtering import filter_jobs_for_client
from database import upsert_jobs
from process_clients import get_jobs_from_dice

def build_search_roles(requirements, role_data):
    """Combines API roles into a list of search strings."""
    search_roles = []
    if requirements.get("role"):
        search_roles.append(requirements["role"])
    
    alt_roles = requirements.get("alternate_roles")
    if alt_roles and str(alt_roles).strip().lower() != 'na':
        search_roles.extend([r.strip() for r in str(alt_roles).split(',') if r.strip()])
        
    return search_roles

def process_client(applywizz_id):
    """
    Main worker function for a single applywizz_id.
    This function will be executed by RQ workers.
    """
    print(f"\nWorker started for {applywizz_id}")
    
    try:
        print(f"Fetching client details...")
        client_details = get_client_details(applywizz_id)
        requirements = extract_client_requirements(client_details)
        role_data = get_job_roles()
        
        search_roles = build_search_roles(requirements, role_data)
        locations = requirements.get("locations", [])
        if not locations:
            locations = ["United States"]
            
        print(f"Requirements loaded. Roles: {search_roles}, Locations count: {len(locations)}")
        
        all_jobs = []
        # Scrape Dice for each role and location combination
        print("Dice scraping started...")
        for role in search_roles:
            if not role: continue
            for location in locations:
                if not location: continue
                # print(f"Scraping Dice for role: '{role}' in '{location}' for {applywizz_id}")
                jobs = get_jobs_from_dice(role, location, "USA")
                all_jobs.extend(jobs)
                # Respect rate limits, small sleep
                time.sleep(1)
                
        print(f"Dice jobs fetched: {len(all_jobs)}")
        
        matched_jobs = filter_jobs_for_client(all_jobs, requirements)
        print(f"Jobs after filtering: {len(matched_jobs)}")
        
        db_jobs = []
        for job in matched_jobs:
            db_jobs.append({
                'url': job.job_url,
                'title': job.title,
                'company': job.company_name,
                'applywizz_id': applywizz_id,
                'company_email': getattr(job, 'emails', None) # JobPost uses emails
            })
            
        inserted, skipped = upsert_jobs(db_jobs)
        print(f"Inserted: {inserted}")
        print(f"Duplicates skipped: {skipped}")
        print(f"{applywizz_id} completed.")
        
    except Exception as e:
        print(f"ERROR processing {applywizz_id}: {str(e)}")
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Render Worker for Dice Scraper")
    parser.add_argument('--test-mode', action='store_true', help="Run first 5 active clients synchronously without Redis")
    args = parser.parse_args()
    
    if args.test_mode:
        print("Running in TEST MODE...")
        from database import init_db
        init_db()
        
        active_ids = get_active_clients()
        test_ids = active_ids[:5]
        print(f"Testing with clients: {test_ids}")
        for awl_id in test_ids:
            try:
                process_client(awl_id)
            except Exception as e:
                print(f"Error processing {awl_id}: {e}")
        sys.exit(0)
        
    if not REDIS_URL:
        print("CRITICAL ERROR: REDIS_URL is required to run the worker.")
        sys.exit(1)
        
    redis_conn = Redis.from_url(REDIS_URL)
    print("Worker successfully connected to Redis. Starting to consume tasks...")
    worker = Worker(['default'], connection=redis_conn)
    worker.work()
