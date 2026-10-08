import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from config import REDIS_URL, TEST_CLIENT_IDS
from api_client import get_active_clients, get_client_details, extract_client_requirements, get_job_roles
from filtering import filter_jobs_for_client, resolve_allowed_roles, extract_job_experience, clean_role_name
from process_clients import get_jobs_from_dice

def build_search_roles(requirements, role_data):
    """Combines API roles and domain metadata into a prioritized list of search strings."""
    primary_role = requirements.get("role", "")
    alt_roles = requirements.get("alternate_roles", "")
    country = requirements.get("country", "United States")
    allowed = resolve_allowed_roles(primary_role, alt_roles, role_data, country)
    
    search_roles = []
    # 1. Primary role first
    if primary_role:
        search_roles.append(primary_role)
        
    # 2. Client's explicit alternate roles
    if alt_roles and str(alt_roles).strip().lower() != 'na':
        for alt in str(alt_roles).split(','):
            a_clean = alt.strip()
            if a_clean and a_clean.lower() not in [s.lower() for s in search_roles]:
                search_roles.append(a_clean)
                
    # 3. Domain expanded roles from role_data
    for r in allowed:
        if r.lower() not in [s.lower() for s in search_roles]:
            search_roles.append(r)
            
    # Limit search roles to the top 4 most relevant to avoid excessive scraping requests
    return search_roles[:4]

def process_client(applywizz_id, to_db: bool = True):
    """
    Worker function for a single applywizz_id.
    If to_db is True, inserts results into Azure PostgreSQL.
    If to_db is False, returns the list of matched job detail dicts (for CSV export).
    """
    print(f"\nWorker started for {applywizz_id}")
    
    try:
        print(f"Fetching client details...")
        client_details = get_client_details(applywizz_id)
        requirements = extract_client_requirements(client_details)
        role_data = get_job_roles()
        requirements["role_data"] = role_data
        
        search_roles = build_search_roles(requirements, role_data)
        locations = requirements.get("locations", [])
        if not locations:
            locations = ["United States"]
            
        print(f"Requirements loaded. Roles: {search_roles}, Locations count: {len(locations)}")
        
        all_jobs = []
        job_trace_map = {}
        # Scrape Dice for each role and location combination
        print("Dice scraping started...")
        client_country = requirements.get("country") or "United States"
        for role in search_roles:
            if not role: continue
            for location in locations:
                if not location: continue
                # print(f"Scraping Dice for role: '{role}' in '{location}' for {applywizz_id}")
                jobs = get_jobs_from_dice(role, location, client_country)
                for j in jobs:
                    j_url = getattr(j, 'job_url', None)
                    if j_url and j_url not in job_trace_map:
                        job_trace_map[j_url] = (role, location)
                all_jobs.extend(jobs)
                # Respect rate limits, small sleep
                time.sleep(1)
                
        print(f"Dice jobs fetched: {len(all_jobs)}")
        
        matched_jobs = filter_jobs_for_client(all_jobs, requirements)
        print(f"Jobs after filtering: {len(matched_jobs)}")
        
        client_records = []
        for job in matched_jobs:
            loc = getattr(job, 'location', None)
            loc_str = loc.display_location() if loc and hasattr(loc, 'display_location') else str(loc or '')
            
            raw_job_exp = getattr(job, 'experience', None)
            if raw_job_exp:
                job_exp_str = str(raw_job_exp).strip()
            else:
                desc = getattr(job, 'description', '') or ''
                extracted_str = None
                if desc:
                    try:
                        from jobspy_enhanced.dice.util import extract_experience_from_description
                        extracted_str = extract_experience_from_description(desc)
                    except Exception:
                        pass
                if extracted_str:
                    job_exp_str = extracted_str
                else:
                    num_exp = extract_job_experience(job)
                    if num_exp is not None:
                        job_exp_str = f"{int(num_exp) if num_exp.is_integer() else num_exp} years"
                    else:
                        job_exp_str = "Not Specified"
            
            client_loc = requirements.get('client_location') or requirements.get('state_of_residence') or requirements.get('country') or "United States"
            client_pref_loc = requirements.get('client_preferred_locations') or ", ".join(requirements.get('locations', [])) or requirements.get('country') or "United States"
                
            alt_roles = str(requirements.get('alternate_roles') or '').strip()
            if alt_roles.lower() == 'na':
                alt_roles = ''
            if not alt_roles and role_data:
                resolved_alts = resolve_allowed_roles(requirements.get('role', ''), '', role_data, requirements.get('country', 'United States'))
                primary_clean = clean_role_name(requirements.get('role', ''))
                alts_only = [r for r in resolved_alts if r.lower() != primary_clean.lower()]
                if alts_only:
                    alt_roles = ", ".join(alts_only)
                
            search_keyword, search_location = job_trace_map.get(job.job_url, ("", ""))
            job_type_val = getattr(job, 'job_type', "")
            if job_type_val and isinstance(job_type_val, list):
                types_list = []
                for v in job_type_val:
                    if hasattr(v, 'value'):
                        val = v.value
                        types_list.append(val[0] if isinstance(val, (list, tuple)) and val else str(val))
                    else:
                        types_list.append(str(v))
                job_type_val = ", ".join(types_list)
            elif job_type_val and hasattr(job_type_val, 'value'):
                val = job_type_val.value
                job_type_val = val[0] if isinstance(val, (list, tuple)) and val else str(val)
            else:
                job_type_val = str(job_type_val or "")
                
            record = {
                'awl_id': applywizz_id,
                'client_experience': requirements.get('experience_raw', requirements.get('experience', '')),
                'client_location': client_loc,
                'client_preferred_locations': client_pref_loc,
                'client_role': requirements.get('role', ''),
                'client_alternative_roles': alt_roles,
                'job_role': job.title,
                'job_experience': job_exp_str,
                'job_location': loc_str,
                'job_url': job.job_url,
                'company': job.company_name or '',
                'company_email': requirements.get('company_email', None),
                'scraped_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if 'timezone' in globals() else None,
                # DB / internal compatibility fields
                'url': job.job_url,
                'title': job.title,
                'description': getattr(job, 'description', '') or "",
                'location': loc_str,
                'job_type': str(job_type_val),
                'scraped_experience': str(getattr(job, 'experience', "") or ""),
                'search_keyword': search_keyword,
                'search_location': search_location,
                'posted_at': getattr(job, 'posted_at', '') or "",
                'updated_at': getattr(job, 'updated_at', '') or ""
            }
            client_records.append(record)
            
        if to_db:
            from database import upsert_jobs
            db_jobs = [{
                'url': r['url'],
                'title': r['title'],
                'company': r['company'],
                'applywizz_id': r['awl_id'],
                'company_email': r['company_email'],
                'description': r['description'],
                'location': r['location'],
                'job_type': r['job_type'],
                'scraped_experience': r['scraped_experience'],
                'search_keyword': r['search_keyword'],
                'search_location': r['search_location'],
                'posted_at': r['posted_at'],
                'updated_at': r['updated_at']
            } for r in client_records]
            
            inserted, skipped = upsert_jobs(db_jobs)
            print(f"Inserted: {inserted}")
            print(f"Duplicates skipped: {skipped}")
            
        print(f"{applywizz_id} completed.")
        return client_records
        
    except Exception as e:
        print(f"ERROR processing {applywizz_id}: {str(e)}")
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Render Worker for Dice Scraper")
    parser.add_argument('--test-mode', action='store_true', help="Run local test without Redis/Database and export to CSV")
    parser.add_argument('--clients', '--ids', type=str, default=None, help="Comma-separated client IDs to test (e.g. AWL-39223,AWL-32830)")
    parser.add_argument('--limit', type=int, default=None, help="Number of active clients to test (default: all active clients)")
    parser.add_argument('--csv', type=str, default="test_results_clients.csv", help="Output CSV path for test mode")
    args = parser.parse_args()
    
    if args.test_mode:
        test_ids = []
        if args.clients:
            test_ids = [c.strip() for c in args.clients.split(',') if c.strip()]
            print(f"Running in TEST MODE with specified client IDs from CLI: {test_ids}")
        elif TEST_CLIENT_IDS:
            test_ids = [c.strip() for c in TEST_CLIENT_IDS.split(',') if c.strip()]
            print(f"Running in TEST MODE with client IDs from TEST_CLIENT_IDS: {test_ids}")
        else:
            try:
                active_ids = get_active_clients()
                if args.limit:
                    print(f"Running in TEST MODE with first {args.limit} of {len(active_ids)} active clients...")
                    test_ids = active_ids[:args.limit]
                else:
                    print(f"Running in TEST MODE with all {len(active_ids)} active clients...")
                    test_ids = active_ids
            except Exception as e:
                print(f"Failed to fetch active clients: {e}")
                sys.exit(1)
                
        print(f"Testing with clients: {test_ids}")
        
        all_test_results = []
        for awl_id in test_ids:
            try:
                records = process_client(awl_id, to_db=False)
                all_test_results.extend(records)
            except Exception as e:
                print(f"Error processing {awl_id}: {e}")
                
        # Write results to CSV file with exact user-requested columns
        csv_path = args.csv
        fieldnames = [
            'awl_id',
            'client_experience',
            'client_location',
            'client_preferred_locations',
            'client_role',
            'client_alternative_roles',
            'job_role',
            'job_experience',
            'job_location',
            'job_url',
            'company',
            'company_email',
            'scraped_at',
            'posted_at',
            'updated_at',
            'job_type',
            'scraped_experience'
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_test_results)
            
        print(f"\n[SUCCESS] Saved {len(all_test_results)} matched jobs for {len(test_ids)} clients to {csv_path}")
        sys.exit(0)
        
    if not REDIS_URL:
        print("CRITICAL ERROR: REDIS_URL is required to run the worker in production.")
        sys.exit(1)
        
    from rq import Worker
    from redis import Redis
    redis_conn = Redis.from_url(REDIS_URL)
    print("Worker successfully connected to Redis. Starting to consume tasks...")
    worker = Worker(['default'], connection=redis_conn)
    worker.work()
