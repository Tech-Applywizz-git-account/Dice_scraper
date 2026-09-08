import csv
import sys
import argparse
import re
from typing import Dict, List, Any
import jobspy_enhanced.dice
from jobspy_enhanced.model import ScraperInput, Site, Country, JobPost

# In-memory cache for dice search queries
# Key: (search_term, location, country) -> List[JobPost]
DICE_CACHE = {}

def get_jobs_from_dice(search_term: str, location: str, country_str: str) -> List[JobPost]:
    if not search_term:
        return []
        
    cache_key = (search_term, location, country_str)
    if cache_key in DICE_CACHE:
        return DICE_CACHE[cache_key]

    try:
        country_enum = Country.from_string(country_str) if country_str and str(country_str).strip() else Country.USA
    except ValueError:
        country_enum = Country.USA

    scraper_input = ScraperInput(
        site_type=[Site.DICE],
        search_term=search_term,
        location=location if location and str(location).strip() else "United States",
        country=country_enum,
        results_wanted=30, # Get a batch of jobs to filter
        hours_old=72 # Limit to recent jobs for relevance
    )

    scraper = jobspy_enhanced.dice.Dice()
    try:
        job_response = scraper.scrape(scraper_input)
        jobs = job_response.jobs
        DICE_CACHE[cache_key] = jobs
        return jobs
    except Exception as e:
        print(f"Error scraping for '{search_term}' at '{location}': {e}")
        return []

def extract_years(exp_str: Any) -> float:
    if not exp_str:
        return 0.0
    # Try to find a number in the string
    match = re.search(r'(\d+)', str(exp_str))
    if match:
        return float(match.group(1))
    return 0.0

def matches_preferences(job: JobPost, prefs: str) -> bool:
    if not prefs or str(prefs).strip().lower() == 'na':
        return True
    
    prefs_lower = str(prefs).lower()
    title_lower = (job.title or "").lower()
    desc_lower = (job.description or "").lower()
    
    # Heuristic: avoid roles
    if "avoid" in prefs_lower or "do not apply" in prefs_lower or "don't apply" in prefs_lower:
        avoid_terms = []
        if "architect" in prefs_lower: avoid_terms.append("architect")
        if "senior" in prefs_lower: avoid_terms.append("senior")
        if "lead" in prefs_lower: avoid_terms.append("lead")
        if "manager" in prefs_lower: avoid_terms.append("manager")
        
        for term in avoid_terms:
            if term in title_lower:
                return False
                
    # Heuristic: apply only to specific terms
    if "apply only to" in prefs_lower:
        match = re.search(r'apply only to([^:]+)', prefs_lower)
        if match:
            only_roles = [r.strip() for r in match.group(1).split(',')]
            # Check if any of these roles are in the title
            if only_roles and not any(r in title_lower for r in only_roles if r):
                return False

    return True

def process_clients(csv_path: str, test_mode: bool = False):
    results = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            if test_mode and count >= 5:
                break
            
            awl_id = row.get('APW ID', '')
            name = row.get('Name', '')
            email = row.get('Email', '')
            target_role = row.get('Target Job Role', '')
            alt_roles = row.get('Alternate Job Roles', '')
            experience = row.get('Experience (years)', '')
            location = row.get('Location', '')
            country = row.get('Country', '')
            sponsorship = row.get('Sponsorship Required', '').strip().lower()
            work_pref = row.get('Work Preference', '').strip().lower()
            exclude_companies = row.get('Exclude Companies', '')
            client_prefs = row.get('Client Preferences', '')
            
            if not awl_id or not str(awl_id).startswith('AWL'):
                continue
                
            print(f"\nProcessing client: {name} ({awl_id})")
            
            search_roles = [target_role]
            if alt_roles and str(alt_roles).strip().lower() != 'na':
                search_roles.extend([r.strip() for r in alt_roles.split(',') if r.strip()])
                
            client_exp = extract_years(experience)
            
            exclude_list = [c.strip().lower() for c in exclude_companies.split(',')] if exclude_companies and str(exclude_companies).strip().lower() != 'na' else []
            
            client_jobs = {} # keyed by job_url for deduplication
            
            for role in search_roles:
                if not role: continue
                
                print(f"  Searching Dice for role: '{role}' in '{location}'")
                jobs = get_jobs_from_dice(role, location, country)
                
                for job in jobs:
                    if job.job_url in client_jobs:
                        continue
                    
                    # Apply filtering
                    
                    # 1. Company exclusion
                    comp_name = (job.company_name or "").lower()
                    if comp_name and any(exc in comp_name for exc in exclude_list if exc):
                        continue
                        
                    # 2. Experience check
                    job_exp_str = getattr(job, 'experience', '')
                    if job_exp_str:
                        job_exp = extract_years(job_exp_str)
                        # If client has little experience and job demands much more
                        if client_exp >= 0 and job_exp > client_exp + 2:
                            continue
                            
                    # 3. Work preference check
                    if work_pref == 'remote':
                        if not job.is_remote:
                            continue
                    elif work_pref == 'hybrid':
                        # Note: job_type, description, or title might mention hybrid
                        is_hybrid = False
                        desc_lower = (job.description or "").lower()
                        if "hybrid" in desc_lower or "hybrid" in (job.title or "").lower():
                            is_hybrid = True
                        if not is_hybrid:
                            continue
                            
                    # 4. Sponsorship check
                    # Strict filtering for sponsorship is hard without deep NLP. 
                    # If sponsorship is required, we can look for "no c2c", "no sponsorship" in desc
                    desc_lower = (job.description or "").lower()
                    if sponsorship == 'yes':
                        if "no sponsorship" in desc_lower or "no h1b" in desc_lower or "no corp to corp" in desc_lower or "no c2c" in desc_lower:
                            continue
                    
                    # 5. Client Preferences
                    if not matches_preferences(job, client_prefs):
                        continue
                    
                    client_jobs[job.job_url] = job
            
            print(f"  Found {len(client_jobs)} matching jobs for {name}")
            for url, job in client_jobs.items():
                results.append({
                    'AWL-ID': awl_id,
                    'CLIENT NAME': name,
                    'EMAIL': email,
                    'DICE JOB LINKS': job.job_url,
                    'DICE JOB URL TITLE': job.title,
                    'DICE JOB URL COMPANY': job.company_name
                })
                
            count += 1
            
    # Write final output
    output_csv = "final_results_test.csv" if test_mode else "final_results.csv"
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['AWL-ID', 'CLIENT NAME', 'EMAIL', 'DICE JOB LINKS', 'DICE JOB URL TITLE', 'DICE JOB URL COMPANY']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
        
    print(f"\nDone! Processed {count} clients. Results saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process clients from CSV and search Dice jobs.")
    parser.add_argument('csv_path', help="Path to the input CSV file")
    parser.add_argument('--test-mode', action='store_true', help="Only process the first 5 clients")
    args = parser.parse_args()
    
    process_clients(args.csv_path, args.test_mode)
