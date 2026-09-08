import requests
from config import ACTIVE_CLIENTS_URL, CLIENT_DETAILS_URL, JOB_ROLES_URL

def get_active_clients():
    """Fetches the list of active applywizz_ids."""
    response = requests.get(ACTIVE_CLIENTS_URL)
    response.raise_for_status()
    data = response.json()
    return data.get("applywizz_ids", [])

def get_client_details(applywizz_id):
    """Fetches details for a specific client."""
    response = requests.get(CLIENT_DETAILS_URL, params={"applywizz_id": applywizz_id})
    response.raise_for_status()
    return response.json()

def get_job_roles():
    """Fetches the job roles metadata."""
    response = requests.get(JOB_ROLES_URL)
    response.raise_for_status()
    return response.json()

def extract_client_requirements(client_details):
    """
    Extracts needed requirements from the client details API response.
    """
    client_info = client_details.get("client", {})
    add_info = client_details.get("additional_information", {})
    
    applywizz_id = client_info.get("applywizz_id")
    
    # Try to get the role from additional_information first, fallback to job_role_preferences
    role = add_info.get("role")
    if not role:
        job_prefs = client_info.get("job_role_preferences", [])
        if job_prefs:
            role = job_prefs[0]
            
    alternate_roles = add_info.get("alternate_job_roles", "")
    
    # Try to parse experience to float, default to 0.0
    experience_str = add_info.get("experience", "")
    experience = 0.0
    try:
        import re
        match = re.search(r'(\d+)', str(experience_str))
        if match:
            experience = float(match.group(1))
    except Exception:
        pass
        
    sponsorship = client_info.get("sponsorship", False)
    
    work_preference = str(add_info.get("work_preferences", "")).lower()
    if work_preference not in ["remote", "hybrid"]:
        work_preference = "all"
        
    exclude_companies = add_info.get("exclude_companies", "[]")
    try:
        import json
        if exclude_companies.startswith("["):
            exclude_list = json.loads(exclude_companies)
            # Handle list of strings vs string of comma separated
            if len(exclude_list) == 1 and "," in exclude_list[0]:
                exclude_list = [x.strip() for x in exclude_list[0].split(",")]
        else:
            exclude_list = [x.strip() for x in exclude_companies.split(",")]
    except Exception:
        exclude_list = [x.strip() for x in exclude_companies.split(",") if x.strip()]
        
    locations = client_info.get("location_preferences", [])
    
    return {
        "applywizz_id": applywizz_id,
        "name": client_info.get("full_name", ""),
        "email": client_info.get("personal_email", ""),
        "role": role,
        "alternate_roles": alternate_roles,
        "experience": experience,
        "sponsorship": "yes" if sponsorship else "no",
        "work_preference": work_preference,
        "exclude_companies": exclude_list,
        "locations": locations
    }
