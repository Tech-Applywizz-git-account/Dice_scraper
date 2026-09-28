import csv
import re
import sys
import time
from typing import Any, Tuple, List, Dict

import filtering
from api_client import get_client_details, extract_client_requirements, get_job_roles
from filtering import (
    filter_jobs_for_client,
    resolve_allowed_roles,
    extract_job_experience,
    clean_role_name,
)
from worker import build_search_roles
from process_clients import get_jobs_from_dice

CLIENTS = [
    "AWL-27273",
    "AWL-31101",
    "AWL-32313",
    "AWL-33055",
    "AWL-32241",
    "AWL-33838",
    "AWL-39198",
    "AWL-33043",
    "AWL-32458",
    "AWL-28569",
    "AWL-32063",
    "AWL-35238"
]

OUTPUT_CSV = "test_results_contract_clients.csv"

def matches_employment_type_contract(job: Any, client_exp: float = 0.0) -> Tuple[bool, str]:
    """
    Accepts ONLY contract jobs (Contract W2, Contract Corp To Corp, Contract Independent, C2C, 1099, etc.).
    Rejects Full Time only, Internship, or missing employment types.
    """
    emp_type = getattr(job, 'employment_type', None) or getattr(job, 'w2_c2c_type', None)
    if not emp_type and getattr(job, 'job_type', None):
        types_str = [jt.value[0] if hasattr(jt, 'value') else str(jt) for jt in job.job_type if jt]
        if types_str:
            emp_type = ", ".join(types_str)
            
    if not emp_type:
        return False, "missing_employment_type"
        
    emp_str = str(emp_type).strip()
    if not emp_str or emp_str.lower() in ['none', 'null', 'not specified', 'unknown', 'na', 'n/a', '']:
        return False, "missing_employment_type"
        
    emp_lower = emp_str.lower()
    
    contract_patterns = [
        r'\bcontract\s+w-?2\b',
        r'\bcontract\s+independent\b',
        r'\bcontract\s+corp\s+to\s+corp\b',
        r'\bcontract\s+c2c\b',
        r'\bcontract\s+to\s+hire\b',
        r'\bcontract\b',
        r'\bindependent\s+contractor\b',
        r'\bcorp\s*[-–]?\s*to\s*[-–]?\s*corp\b',
        r'\bc2c\b',
        r'\bc-2-c\b',
        r'\b1099\b',
        r'\bw-?2\b',
    ]
    if any(re.search(pat, emp_lower) for pat in contract_patterns):
        return True, "contract"
        
    return False, "not_contract"

# In-memory monkeypatch for contract filtering: existing files remain untouched!
filtering.matches_employment_type = matches_employment_type_contract

def process_client_contract(applywizz_id: str) -> List[Dict[str, Any]]:
    print(f"\n==========================================")
    print(f"Processing client {applywizz_id} (CONTRACT JOBS ONLY)")
    print(f"==========================================")
    
    client_details = get_client_details(applywizz_id)
    requirements = extract_client_requirements(client_details)
    role_data = get_job_roles()
    requirements["role_data"] = role_data
    
    search_roles = build_search_roles(requirements, role_data)
    locations = requirements.get("locations", [])
    if not locations:
        locations = ["United States"]
        
    print(f"Roles to search ({len(search_roles)}): {search_roles}")
    print(f"Locations ({len(locations)}): {locations}")
    
    all_jobs = []
    client_country = requirements.get("country") or "United States"
    for role in search_roles:
        if not role:
            continue
        for location in locations:
            if not location:
                continue
            jobs = get_jobs_from_dice(role, location, client_country)
            all_jobs.extend(jobs)
            time.sleep(0.5)
            
    print(f"Total raw Dice jobs fetched: {len(all_jobs)}")
    
    matched_jobs = filter_jobs_for_client(all_jobs, requirements)
    print(f"Contract jobs matched after filtering: {len(matched_jobs)}")
    
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
                
        emp_type_str = str(getattr(job, 'employment_type', None) or getattr(job, 'w2_c2c_type', None) or '')
        
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
            'job_employment_type': emp_type_str,
            'job_company': job.company_name or '',
            'job_url': job.job_url,
        }
        client_records.append(record)
        
    return client_records

def main():
    print(f"Starting Contract-only test run for {len(CLIENTS)} clients...")
    client_counts = {}
    all_records = []
    
    for cid in CLIENTS:
        try:
            records = process_client_contract(cid)
            client_counts[cid] = len(records)
            all_records.extend(records)
        except Exception as e:
            print(f"ERROR processing {cid}: {e}")
            client_counts[cid] = f"Error: {e}"
            
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
        'job_employment_type',
        'job_company',
        'job_url'
    ]
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)
        
    print(f"\n==========================================")
    print(f"CONTRACT RUN SUMMARY ({len(CLIENTS)} clients)")
    print(f"==========================================")
    for cid, cnt in client_counts.items():
        print(f"  {cid}: {cnt} contract jobs")
    print(f"Total Contract Jobs Matched: {len(all_records)}")
    print(f"Saved results to: {OUTPUT_CSV}")
    print(f"==========================================")

if __name__ == '__main__':
    main()
