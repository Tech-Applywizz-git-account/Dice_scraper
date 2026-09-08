from process_clients import extract_years, matches_preferences

def filter_jobs_for_client(jobs, requirements):
    """
    Applies the existing business rules to filter scraped jobs based on API requirements.
    """
    filtered_jobs = []
    seen_urls = set()
    
    client_exp = requirements.get("experience", 0.0)
    exclude_list = [c.strip().lower() for c in requirements.get("exclude_companies", []) if c.strip()]
    work_pref = requirements.get("work_preference", "all").lower()
    sponsorship = requirements.get("sponsorship", "no").lower()
    
    # client_prefs is not mapped directly in the API as a single string field, 
    # but we will default it to 'na' to satisfy the existing matches_preferences
    client_prefs = "na"
    
    for job in jobs:
        if job.job_url in seen_urls:
            continue
            
        # 1. Company exclusion
        comp_name = (job.company_name or "").lower()
        if comp_name and any(exc in comp_name for exc in exclude_list if exc):
            continue
            
        # 2. Experience check
        job_exp_str = getattr(job, 'experience', '')
        if job_exp_str:
            job_exp = extract_years(job_exp_str)
            if client_exp >= 0 and job_exp > client_exp + 2:
                continue
                
        # 3. Work preference check
        if work_pref == 'remote':
            if not job.is_remote:
                continue
        elif work_pref == 'hybrid':
            is_hybrid = False
            desc_lower = (job.description or "").lower()
            if "hybrid" in desc_lower or "hybrid" in (job.title or "").lower():
                is_hybrid = True
            if not is_hybrid:
                continue
                
        # 4. Sponsorship check
        desc_lower = (job.description or "").lower()
        if sponsorship == 'yes':
            if "no sponsorship" in desc_lower or "no h1b" in desc_lower or "no corp to corp" in desc_lower or "no c2c" in desc_lower:
                continue
                
        # 5. Client Preferences (custom rule matching)
        if not matches_preferences(job, client_prefs):
            continue
            
        seen_urls.add(job.job_url)
        filtered_jobs.append(job)
        
    return filtered_jobs
