import os
import json
import re
import requests
from config import ACTIVE_CLIENTS_URL, CLIENT_DETAILS_URL, JOB_ROLES_URL, CLIENTS_FILE
from filtering import extract_state_from_location

def get_active_clients():
    """
    Fetches the list of active applywizz_ids.
    If CLIENTS_FILE (default: 'clients.json') exists and is non-empty, reads IDs from the JSON file.
    Otherwise, falls back to the live ACTIVE_CLIENTS_URL endpoint.
    """
    if CLIENTS_FILE and os.path.exists(CLIENTS_FILE):
        try:
            with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                ids = [str(x).strip() for x in data if str(x).strip()]
                if ids:
                    print(f"Loaded {len(ids)} clients from {CLIENTS_FILE}: {ids}")
                    return ids
            elif isinstance(data, dict):
                ids = data.get("applywizz_ids") or data.get("clients") or []
                ids = [str(x).strip() for x in ids if str(x).strip()]
                if ids:
                    print(f"Loaded {len(ids)} clients from {CLIENTS_FILE}: {ids}")
                    return ids
        except Exception as e:
            print(f"Warning: Failed to read {CLIENTS_FILE}: {e}. Falling back to API.")

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
    client_info = client_details.get("client") or {}
    add_info = client_details.get("additional_information") or {}
    
    applywizz_id = client_info.get("applywizz_id")
    
    # Try to get the role from additional_information first, fallback to job_role_preferences
    role = add_info.get("role")
    if not role:
        job_prefs = client_info.get("job_role_preferences", [])
        if job_prefs:
            role = str(job_prefs[0]).replace("_", " ").strip()
    elif isinstance(role, str):
        role = role.replace("_", " ").strip()
            
    alternate_roles = add_info.get("alternate_job_roles", "")
    if not alternate_roles or str(alternate_roles).strip().lower() in ["", "na", "none", "null"]:
        job_prefs = client_info.get("job_role_preferences", [])
        if len(job_prefs) > 1:
            alternate_roles = ", ".join(str(p).replace("_", " ").strip() for p in job_prefs[1:] if p)
        else:
            alternate_roles = ""
    elif isinstance(alternate_roles, str):
        alternate_roles = alternate_roles.replace("_", " ").strip()
    
    # Parse experience, preserving raw string and supporting decimals
    experience_str = str(add_info.get("experience", "")).strip()
    experience = 0.0
    try:
        import re
        match = re.search(r'(\d+(?:\.\d+)?)', experience_str)
        if match:
            experience = float(match.group(1))
    except Exception:
        pass
        
    sponsorship = client_info.get("sponsorship", False)
    
    raw_locations = client_info.get("location_preferences", []) or []
    NON_GEO_WORK_MODES = {"onsite", "remote", "hybrid", "work from home", "wfh", "open to remote", "remote only"}
    
    work_preference = str(add_info.get("work_preferences", "")).lower().strip()
    if work_preference not in ["remote", "hybrid"]:
        lower_loc_modes = [str(l).strip().lower() for l in raw_locations if str(l).strip().lower() in NON_GEO_WORK_MODES]
        if "remote" in lower_loc_modes and "onsite" not in lower_loc_modes and "hybrid" not in lower_loc_modes:
            work_preference = "remote"
        elif "hybrid" in lower_loc_modes and "onsite" not in lower_loc_modes and "remote" not in lower_loc_modes:
            work_preference = "hybrid"
        else:
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
        
    country = client_info.get("country") or add_info.get("country")
    if not country:
        zip_country = str(add_info.get("zip_or_country", "")).strip()
        if any(w in zip_country.lower() for w in ["united states", "usa", "us"]):
            country = "United States"
        elif any(w in zip_country.lower() for w in ["united kingdom", "uk"]):
            country = "United Kingdom"
        elif "canada" in zip_country.lower():
            country = "Canada"
        elif "ireland" in zip_country.lower():
            country = "Ireland"
        elif zip_country and zip_country.lower() != 'na':
            country = zip_country
        else:
            country = "United States"
            
    state_residence = str(add_info.get("state_of_residence", "")).strip()
    home_state = extract_state_from_location(state_residence)
    client_residence_str = state_residence if state_residence and state_residence.lower() != 'na' else country
    willing_to_relocate = add_info.get("willing_to_relocate", True)
    
    raw_locations = client_info.get("location_preferences", [])
    valid_locs = [
        str(l).strip() for l in raw_locations 
        if str(l).strip() and str(l).strip().lower() not in ["no", "none", "na", "n/a", "nil", "null"]
        and str(l).strip().lower() not in NON_GEO_WORK_MODES
    ]
    
    locations = []
    def add_location(loc_name):
        if not loc_name:
            return
        loc_clean = str(loc_name).strip()
        if not loc_clean or loc_clean.lower() in ["no", "none", "na", "n/a", "nil", "null"] or loc_clean.lower() in NON_GEO_WORK_MODES:
            return
        if loc_clean.lower() not in [l.lower() for l in locations]:
            locations.append(loc_clean)

    # 1. First: Client's explicit preferred locations
    for loc in valid_locs:
        if loc and loc.lower() != country.lower():
            add_location(loc)

    # 2. Then: Client location (residence / home state)
    if state_residence and state_residence.lower() not in ["na", "none", "no", "n/a", country.lower()]:
        if home_state and home_state.lower() != state_residence.lower():
            add_location(state_residence)
            add_location(home_state)
        elif home_state:
            add_location(home_state)
        else:
            add_location(state_residence)
    elif home_state:
        add_location(home_state)

    # 3. Then: Whole client's country at the end
    if country:
        add_location(country)
    else:
        add_location("United States")

    preferred_locations_str = ", ".join(locations)
    
    return {
        "applywizz_id": applywizz_id,
        "name": client_info.get("full_name", ""),
        "email": client_info.get("personal_email", ""),
        "company_email": client_info.get("company_email", ""),
        "role": role,
        "alternate_roles": alternate_roles,
        "experience": experience,
        "experience_raw": experience_str,
        "sponsorship": "yes" if sponsorship else "no",
        "work_preference": work_preference,
        "exclude_companies": exclude_list,
        "locations": locations,
        "client_location": client_residence_str,
        "client_preferred_locations": preferred_locations_str,
        "country": country,
        "state_of_residence": state_residence,
        "willing_to_relocate": willing_to_relocate
    }
