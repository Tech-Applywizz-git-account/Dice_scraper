def analyze_match(client_req, job):
    """
    Compares the client requirement against the scraped job.
    Returns structured analysis and relevance score.
    """
    
    # 1. Role Match (40 pts)
    # Check if the search keyword or any required roles appear in the title
    required_role = str(client_req.get('role', '')).lower()
    alt_roles = str(client_req.get('alternate_roles', '')).lower().split(',')
    roles_to_check = [required_role] + [r.strip() for r in alt_roles if r.strip()]
    
    job_title = str(job.get('title', '')).lower()
    
    role_match = False
    for r in roles_to_check:
        if r and r in job_title:
            role_match = True
            break
            
    role_score = 40 if role_match else 0
    if not role_match:
        # Check description
        job_desc = str(job.get('description', '')).lower()
        for r in roles_to_check:
            if r and r in job_desc:
                role_score = 20 # Partial points if in description but not title
                break
                
    # 2. Experience Match (20 pts)
    # The scraper already filters > client_exp + 2, but we recalculate here for diagnostic
    client_exp = float(client_req.get('experience', 0.0))
    job_exp_str = str(job.get('scraped_experience', '')).lower()
    
    import re
    job_exp = 0.0
    exp_match = re.search(r'(\d+)', job_exp_str)
    if exp_match:
        job_exp = float(exp_match.group(1))
        
    experience_match = True
    if client_exp > 0 and job_exp > (client_exp + 2):
        experience_match = False
        
    exp_score = 20 if experience_match else 0

    # 3. Location Match (15 pts)
    locations = client_req.get('locations', [])
    job_loc = str(job.get('location', '')).lower()
    location_match = False
    if not locations:
        location_match = True
    else:
        for loc in locations:
            if loc.lower() in job_loc:
                location_match = True
                break
    
    # If remote, location match is irrelevant or implicitly true
    job_type = str(job.get('job_type', '')).lower()
    is_remote = "remote" in job_loc or "remote" in job_type
    if is_remote and client_req.get('work_preference', '') == 'remote':
        location_match = True
        
    loc_score = 15 if location_match else 0
    
    # 4. Work Mode Match (15 pts)
    work_pref = str(client_req.get('work_preference', 'all')).lower()
    work_mode_match = True
    
    job_desc = str(job.get('description', '')).lower()
    if work_pref == 'remote':
        if not is_remote and "remote" not in job_desc:
            work_mode_match = False
    elif work_pref == 'hybrid':
        if "hybrid" not in job_loc and "hybrid" not in job_type and "hybrid" not in job_title and "hybrid" not in job_desc:
            work_mode_match = False
            
    work_score = 15 if work_mode_match else 0
            
    # 5. Sponsorship Match (10 pts)
    sponsorship = str(client_req.get('sponsorship', 'no')).lower()
    sponsorship_match = True
    if sponsorship == 'yes':
        if "no sponsorship" in job_desc or "no h1b" in job_desc or "no corp to corp" in job_desc or "no c2c" in job_desc:
            sponsorship_match = False
            
    sponsorship_score = 10 if sponsorship_match else 0
    
    # 6. Company Exclusions
    exclude_list = [c.strip().lower() for c in client_req.get('exclude_companies', []) if c.strip()]
    company_name = str(job.get('company', '')).lower()
    company_match = True
    if company_name and any(exc in company_name for exc in exclude_list if exc):
        company_match = False
        
    # Total Score
    total_score = role_score + exp_score + loc_score + work_score + sponsorship_score
    if not company_match:
        total_score = 0
        
    overall_status = "relevant" if total_score >= 80 else "needs_review" if total_score >= 50 else "potential_mismatch"
    
    return {
        "role_match": role_match,
        "experience_match": experience_match,
        "location_match": location_match,
        "work_mode_match": work_mode_match,
        "sponsorship_match": sponsorship_match,
        "company_match": company_match,
        "overall_status": overall_status,
        "score_breakdown": {
            "role": f"{role_score} / 40",
            "experience": f"{exp_score} / 20",
            "location": f"{loc_score} / 15",
            "work_mode": f"{work_score} / 15",
            "sponsorship": f"{sponsorship_score} / 10"
        },
        "relevance_score": total_score
    }
