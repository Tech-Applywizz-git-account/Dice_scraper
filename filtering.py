import re
import math
import itertools
from typing import List, Dict, Any, Optional, Set

# US States mapping: 2-letter postal code <-> full lowercase state name
US_STATES = {
    'al': 'alabama', 'ak': 'alaska', 'az': 'arizona', 'ar': 'arkansas', 'ca': 'california',
    'co': 'colorado', 'ct': 'connecticut', 'de': 'delaware', 'fl': 'florida', 'ga': 'georgia',
    'hi': 'hawaii', 'id': 'idaho', 'il': 'illinois', 'in': 'indiana', 'ia': 'iowa',
    'ks': 'kansas', 'ky': 'kentucky', 'la': 'louisiana', 'me': 'maine', 'md': 'maryland',
    'ma': 'massachusetts', 'mi': 'michigan', 'mn': 'minnesota', 'ms': 'mississippi', 'mo': 'missouri',
    'mt': 'montana', 'ne': 'nebraska', 'nv': 'nevada', 'nh': 'new hampshire', 'nj': 'new jersey',
    'nm': 'new mexico', 'ny': 'new york', 'nc': 'north carolina', 'nd': 'north dakota', 'oh': 'ohio',
    'ok': 'oklahoma', 'or': 'oregon', 'pa': 'pennsylvania', 'ri': 'rhode island', 'sc': 'south carolina',
    'sd': 'south dakota', 'tn': 'tennessee', 'tx': 'texas', 'ut': 'utah', 'vt': 'vermont',
    'va': 'virginia', 'wa': 'washington', 'wv': 'west virginia', 'wi': 'wisconsin', 'wy': 'wyoming',
    'dc': 'district of columbia'
}
US_STATE_NAMES = {v: k for k, v in US_STATES.items()}


def extract_state_from_location(raw: str) -> Optional[str]:
    """
    Extracts canonical full state name (e.g. 'Texas', 'Connecticut') from:
    - 'City, ST' (e.g. 'San Antonio, TX' -> 'Texas')
    - 'City, State' (e.g. 'West Haven, Connecticut' -> 'Connecticut')
    - 'State' or 'ST' (e.g. 'Colorado' -> 'Colorado', 'TX' -> 'Texas')
    """
    raw = str(raw or '').strip()
    if not raw or raw.lower() in ['na', 'none', 'null', 'no', 'n/a']:
        return None
    m = re.search(r',\s*([A-Za-z\s]+)$', raw)
    if m:
        st_part = m.group(1).strip().lower()
        if st_part in US_STATES:
            return US_STATES[st_part].title()
        elif st_part in US_STATE_NAMES:
            return st_part.title()
    raw_lower = raw.lower()
    if raw_lower in US_STATE_NAMES:
        return raw.title()
    if raw_lower in US_STATES:
        return US_STATES[raw_lower].title()
    return None


def clean_role_name(role: str) -> str:
    """Removes country/visa suffixes like 'for UK', 'for Canada', '(citizen/h4ead)'."""
    if not role:
        return ""
    r = re.sub(r'\s+for\s+[a-zA-Z\s]+', '', role, flags=re.IGNORECASE)
    r = re.sub(r'\(.*?\)', '', r)
    return r.strip().lower()


def resolve_allowed_roles(role: str, alternate_roles: Any, role_data: Optional[List[Dict[str, Any]]] = None, country: str = "United States") -> List[str]:
    """
    Combines primary role, client alternate roles, and metadata from get_job_roles()
    into a comprehensive, normalized list of allowed job roles for the client's domain.
    Prioritizes exact role matches from role_data to prevent domain pollution.
    """
    allowed = set()
    
    primary_clean = clean_role_name(role)
    if primary_clean:
        allowed.add(primary_clean)
        
    # Add client alternate roles from profile
    if alternate_roles:
        if isinstance(alternate_roles, str) and alternate_roles.strip().lower() != 'na':
            for alt in alternate_roles.split(','):
                alt_clean = clean_role_name(alt)
                if alt_clean:
                    allowed.add(alt_clean)
        elif isinstance(alternate_roles, list):
            for alt in alternate_roles:
                alt_clean = clean_role_name(str(alt))
                if alt_clean and alt_clean != 'na':
                    allowed.add(alt_clean)
                    
    # Lookup in role_data metadata from get_job_roles() (https://dashboard.apply-wizz.com/job-roles/)
    if role_data and primary_clean:
        exact_matches = []
        country_matches = []
        
        # 1. Exact match pass (with country & internship isolation)
        for entry in role_data:
            raw_name = entry.get('name', '')
            c_name = clean_role_name(raw_name)
            if c_name == primary_clean:
                is_intern = 'intern' in raw_name.lower() and 'intern' not in primary_clean
                if is_intern:
                    continue
                has_other_country = any(
                    f'for {c}' in raw_name.lower() 
                    for c in ['uk', 'ireland', 'canada', 'india', 'australia'] 
                    if c not in country.lower()
                )
                if not has_other_country:
                    exact_matches.append(entry)
                else:
                    country_matches.append(entry)
                    
        chosen = exact_matches if exact_matches else country_matches
        
        # 2. Fallback to substring matching only if no exact match exists
        if not chosen:
            for entry in role_data:
                raw_name = entry.get('name', '')
                c_name = clean_role_name(raw_name)
                if 'intern' in raw_name.lower() and 'intern' not in primary_clean:
                    continue
                if primary_clean in c_name or c_name in primary_clean:
                    chosen.append(entry)

        for entry in chosen:
            alt_str = entry.get('alternate_roles') or ''
            if alt_str and str(alt_str).strip().lower() != 'na':
                for alt in str(alt_str).split(','):
                    alt_clean = clean_role_name(alt)
                    if alt_clean:
                        allowed.add(alt_clean)

    return [r for r in allowed if r]


def is_inverted_token_match(tokens: List[str], title_raw: str, max_word_distance: int = 4) -> bool:
    """
    Validates inverted-order token matching within realistic boundaries:
    1. If title contains major separators ('|', ';', '•'), all tokens must reside in the SAME segment.
    2. The span between tokens must not exceed max_word_distance.
    Prevents false positives like matching 'AI Engineer' across
    'Security Engineer Offensive Security | Bug Bounty | Penetration Testing | AI Security'.
    """
    if not tokens or not title_raw:
        return False
        
    title_clean = re.sub(r'[^\w\s\+\#\.]', ' ', title_raw.lower())
    if not all(re.search(r'\b' + re.escape(t) + r'\b', title_clean) for t in tokens):
        return False

    # Check segments separated by major delimiters
    major_segments = [s.strip() for s in re.split(r'[\|;•]', title_raw) if s.strip()]
    for seg in major_segments:
        seg_clean = re.sub(r'[^\w\s\+\#\.]', ' ', seg.lower())
        words = [w for w in re.split(r'\s+', seg_clean.strip()) if w]
        indices = {t: [i for i, w in enumerate(words) if w == t] for t in tokens}
        if all(indices.values()):
            for combo in itertools.product(*[indices[t] for t in tokens]):
                if max(combo) - min(combo) <= len(tokens) + max_word_distance:
                    return True

    # If title has no major delimiters, check proximity across the entire title
    if not re.search(r'[\|;•]', title_raw):
        words = [w for w in re.split(r'\s+', title_clean.strip()) if w]
        indices = {t: [i for i, w in enumerate(words) if w == t] for t in tokens}
        if all(indices.values()):
            for combo in itertools.product(*[indices[t] for t in tokens]):
                if max(combo) - min(combo) <= len(tokens) + max_word_distance:
                    return True

    return False


def matches_job_role(job_title: str, allowed_roles: List[str]) -> bool:
    """
    Checks if job_title matches any of the client's allowed domain roles.
    Rejects domain mismatches (e.g. Software Engineer for Network Engineer).
    """
    if not allowed_roles:
        return True
        
    title_lower = (job_title or "").lower()
    title_clean = re.sub(r'[^\w\s\+\#\.]', ' ', title_lower)
    
    # Generic rejection for obvious non-technical or offensive security roles unless specifically in allowed roles
    unrelated_keywords = [
        "recruiter", "talent acquisition", "account executive", "account manager",
        "penetration tester", "penetration testing", "bug bounty", "offensive security", "ethical hacker"
    ]
    if any(re.search(r'\b' + re.escape(kw) + r'\b', title_clean) for kw in unrelated_keywords):
        if not any(kw in role for role in allowed_roles for kw in unrelated_keywords):
            return False
            
    for role in allowed_roles:
        role_clean = re.sub(r'[^\w\s\+\#\.]', ' ', role.lower()).strip()
        if not role_clean:
            continue
            
        # 1. Exact phrase match with word boundaries
        # e.g., 'network engineer' matches 'Senior Network Engineer', 'Network Engineer II'
        if re.search(r'\b' + re.escape(role_clean) + r'\b', title_clean):
            return True
            
        # 2. Match multi-word roles with intermediate specialization words
        # e.g., 'network engineer' matches 'Network Security Engineer', 'Network Systems Engineer'
        tokens = role_clean.split()
        if len(tokens) >= 2:
            pattern = r'\b' + r'\b.*?\b'.join([re.escape(t) for t in tokens]) + r'\b'
            if re.search(pattern, title_clean):
                return True
                
        # 3. Special handling for tech keywords like '.net', 'devops', 'full stack'
        if role_clean in ['net', '.net', 'dotnet']:
            if re.search(r'\b(\.net|dotnet|c\#)\b', title_lower):
                return True
        elif role_clean == 'full stack':
            if re.search(r'\bfull[\s\-_/]*stack\b', title_lower):
                return True
                
        # 4. Match all tokens in close proximity (handles inverted titles like 'Engineer - Network Security', 'ITS Sr Engineer / IS Network')
        # Tokens must not span across pipe/bullet delimiters, and must be within a close word window
        if len(tokens) >= 2 and all(re.search(r'\b' + re.escape(t) + r'\b', title_clean) for t in tokens):
            if is_inverted_token_match(tokens, job_title):
                return True
            
    return False


INTERNSHIP_KEYWORDS_REGEX = re.compile(
    r'\b(intern|interns|internship|internships|co-op|coop|co-operative|trainee|apprentice|apprenticeship|student)\b',
    re.IGNORECASE
)

SENIORITY_EXECUTIVE_REGEX = re.compile(
    r'\b(principal|staff(?:\s+engineer)?|architect|director|vp|vice\s+president|head\s+of|chief|executive)\b',
    re.IGNORECASE
)

SENIORITY_LEAD_REGEX = re.compile(
    r'\b(lead|team\s+lead|tech\s+lead|manager|engineering\s+manager)\b',
    re.IGNORECASE
)

SENIORITY_SENIOR_REGEX = re.compile(
    r'\b(senior|sr\.?|sr\b)\b',
    re.IGNORECASE
)

def is_internship_or_trainee_role(title: str) -> bool:
    """Checks if a job title represents an internship or trainee position."""
    if not title:
        return False
    return bool(INTERNSHIP_KEYWORDS_REGEX.search(title))


def is_seniority_compatible(job_title: str, client_exp: float, client_wants_internship: bool = False) -> bool:
    """
    Validates whether the seniority level in job_title is appropriate for client_exp.
    Rules:
    1. Intern/Trainee:
       - If client has >= 1.0 year of experience and didn't explicitly request an internship, reject.
    2. Executive / Principal / Staff / Architect / Director / VP:
       - Requires client to have at least 6.0 years of experience.
    3. Lead / Manager:
       - Requires client to have at least 5.0 years of experience.
    4. Senior:
       - Requires client to have at least 2.5 years of experience.
    """
    if not job_title:
        return True
        
    t_lower = job_title.lower()
    
    # 1. Internship / Trainee
    if is_internship_or_trainee_role(t_lower):
        if not client_wants_internship and client_exp >= 1.0:
            return False
            
    # 2. Executive / Principal / Staff / Architect / Director / VP
    if SENIORITY_EXECUTIVE_REGEX.search(t_lower):
        if client_exp < 6.0:
            return False
            
    # 3. Lead / Manager
    if SENIORITY_LEAD_REGEX.search(t_lower):
        if client_exp < 5.0:
            return False
            
    # 4. Senior
    if SENIORITY_SENIOR_REGEX.search(t_lower):
        if client_exp < 4.0:
            return False
            
    return True


def calculate_max_experience(client_exp_val: float, client_exp_raw: str = "") -> float:
    """
    Calculates the maximum allowed job experience.
    Rule:
    - If client experience is a whole number (e.g., 5, '5', '5.0'): max is 5 ('if it is 5 just get 5 year').
    - If client experience is fractional (e.g., 5.5) or plus ('5+'): max is 6 ('if it is 5.5 or 5+ get 6 years only not more than 1 year+').
    - Strictly never more than 1 year over client experience.
    """
    raw = str(client_exp_raw or "").strip()
    has_plus = '+' in raw
    
    val = float(client_exp_val) if client_exp_val else 0.0
    if raw:
        m = re.search(r'(\d+(?:\.\d+)?)', raw)
        if m:
            try:
                val = float(m.group(1))
            except ValueError:
                pass
                
    if val <= 0:
        return 1.0
        
    is_fractional = (val % 1.0 != 0)
    
    if has_plus or is_fractional:
        max_exp = float(math.ceil(val) if is_fractional else int(val) + 1)
        if max_exp > val + 1.0:
            max_exp = val + 1.0
        return max_exp
    else:
        return float(val)


def extract_job_experience(job) -> Optional[float]:
    """Extracts required experience years from job.experience or job.description."""
    job_exp_str = getattr(job, 'experience', '') or ''
    if job_exp_str:
        # e.g., '5+ years', '5-7 years', '6 years'
        m = re.search(r'(\d+(?:\.\d+)?)', str(job_exp_str))
        if m:
            return float(m.group(1))
            
    # Fallback to job.description
    desc = getattr(job, 'description', '') or ''
    if desc:
        try:
            from jobspy_enhanced.dice.util import extract_experience_from_description
            extracted = extract_experience_from_description(desc)
            if extracted:
                m = re.search(r'(\d+(?:\.\d+)?)', str(extracted))
                if m:
                    return float(m.group(1))
        except Exception:
            pass
            
    return None


def matches_location_preference(job, location_preferences: List[str], work_preference: str = "all") -> bool:
    """
    Checks if job matches the client's location preferences.
    Handles state names, 2-letter state abbreviations, cities, and remote options.
    """
    if not location_preferences:
        return True
        
    # Check if nationwide or unconstrained
    for p in location_preferences:
        if not p: continue
        p_clean = str(p).strip().lower()
        if p_clean in ["united states", "usa", "us", "all", "na", "no", "none", "n/a"]:
            return True
            
    # Extract preferred state codes, names, and cities
    pref_state_codes: Set[str] = set()
    pref_state_names: Set[str] = set()
    pref_cities: Set[str] = set()
    allow_remote = False
    
    for p in location_preferences:
        if not p: continue
        p_str = str(p).strip().lower()
        if p_str == 'remote':
            allow_remote = True
            continue
            
        # Check if single state code (e.g. 'ct')
        if p_str in US_STATES:
            pref_state_codes.add(p_str)
            pref_state_names.add(US_STATES[p_str])
            continue
            
        # Check if single state name (e.g. 'connecticut')
        if p_str in US_STATE_NAMES:
            pref_state_names.add(p_str)
            pref_state_codes.add(US_STATE_NAMES[p_str])
            continue
            
        # Check comma separated city/state (e.g. 'Dallas, TX' or 'Hartford, CT')
        if ',' in p_str:
            parts = [x.strip() for x in p_str.split(',')]
            if len(parts) >= 2:
                pref_cities.add(parts[0])
                st_part = parts[1]
                if st_part in US_STATES:
                    pref_state_codes.add(st_part)
                    pref_state_names.add(US_STATES[st_part])
                elif st_part in US_STATE_NAMES:
                    pref_state_names.add(st_part)
                    pref_state_codes.add(US_STATE_NAMES[st_part])
                continue
                
        # Otherwise treat as city
        pref_cities.add(p_str)

    is_remote = getattr(job, 'is_remote', False)
    
    # If job is 100% remote
    if is_remote:
        # If client's work preference is remote, or client explicitly added 'remote' to location preferences
        if work_preference == 'remote' or allow_remote:
            return True
        # If client allows 'all' (hybrid/onsite/remote) and job has no conflicting physical state requirement
        if work_preference == 'all' and not getattr(job, 'location', None):
            return True
            
    loc = getattr(job, 'location', None)
    if not loc:
        # If no location given, only match if remote is allowed
        return is_remote and (work_preference in ['all', 'remote'] or allow_remote)
        
    city = (getattr(loc, 'city', '') or '').strip().lower()
    state = (getattr(loc, 'state', '') or '').strip().lower()
    display_loc = (loc.display_location() if hasattr(loc, 'display_location') else str(loc)).lower()
    
    # Normalize job state to code and name
    job_state_code = ''
    job_state_name = ''
    if state in US_STATES:
        job_state_code = state
        job_state_name = US_STATES[state]
    elif state in US_STATE_NAMES:
        job_state_name = state
        job_state_code = US_STATE_NAMES[state]
    else:
        # Try finding state in display_loc
        for st_c, st_n in US_STATES.items():
            if re.search(r'\b' + re.escape(st_c) + r'\b', display_loc) or re.search(r'\b' + re.escape(st_n) + r'\b', display_loc):
                job_state_code = st_c
                job_state_name = st_n
                break
                
    # If job has a physical state and it contradicts the client's preferences: reject!
    if job_state_code and pref_state_codes and job_state_code not in pref_state_codes:
        return False
        
    # Check for positive state match
    if job_state_code and job_state_code in pref_state_codes:
        return True
    if job_state_name and job_state_name in pref_state_names:
        return True
        
    # Check for city match
    if city and city in pref_cities:
        return True
        
    # Check display_loc match against preferred strings
    for p in location_preferences:
        p_clean = str(p).strip().lower()
        if p_clean in display_loc:
            return True

    return False


def filter_jobs_for_client(jobs, requirements):
    """
    Applies business rules to filter scraped jobs based on API requirements:
    1. Company exclusion
    2. Domain / Job Roles match (rejects domain mismatches)
    3. Experience check (strict matching: 5 -> <=5; 5.5/5+ -> <=6)
    4. Location preference check (state/city matching, remote handling)
    5. Work preference check (remote / hybrid / all)
    6. Sponsorship check
    7. Custom client preferences (avoid architect, lead, etc.)
    """
    from process_clients import matches_preferences

    filtered_jobs = []
    seen_urls = set()
    
    client_exp_val = requirements.get("experience", 0.0)
    client_exp_raw = requirements.get("experience_raw", "")
    max_exp = calculate_max_experience(client_exp_val, client_exp_raw)
    
    exclude_list = [c.strip().lower() for c in requirements.get("exclude_companies", []) if c.strip()]
    work_pref = requirements.get("work_preference", "all").lower()
    sponsorship = requirements.get("sponsorship", "no").lower()
    locations = requirements.get("locations", [])
    
    # Resolve domain allowed roles
    primary_role = requirements.get("role", "")
    alternate_roles = requirements.get("alternate_roles", "")
    role_data = requirements.get("role_data", None)
    allowed_roles = resolve_allowed_roles(primary_role, alternate_roles, role_data)
    
    client_prefs = "na"
    rejection_reasons = {"company": 0, "role": 0, "seniority": 0, "experience": 0, "location": 0, "work_pref": 0, "sponsorship": 0, "client_prefs": 0}
    
    for job in jobs:
        if not job or not getattr(job, 'job_url', None):
            continue
            
        if job.job_url in seen_urls:
            continue
            
        # 1. Company exclusion
        comp_name = (getattr(job, 'company_name', '') or "").lower()
        if comp_name and any(exc in comp_name for exc in exclude_list if exc and exc != 'na'):
            rejection_reasons["company"] += 1
            continue
            
        # 2. Domain / Job Role check (only get jobs in the domain job roles)
        job_title = getattr(job, 'title', '') or ''
        if not matches_job_role(job_title, allowed_roles):
            rejection_reasons["role"] += 1
            continue
            
        # 2b. Seniority & Internship check
        client_roles_text = f"{primary_role} {alternate_roles}"
        client_wants_internship = is_internship_or_trainee_role(client_roles_text)
        client_exp_float = float(client_exp_val) if client_exp_val else 0.0
        if not is_seniority_compatible(job_title, client_exp_float, client_wants_internship):
            rejection_reasons["seniority"] += 1
            continue
            
        # 3. Experience check (strict max experience)
        job_exp = extract_job_experience(job)
        if job_exp is not None and job_exp > max_exp:
            rejection_reasons["experience"] += 1
            continue
            
        # 4. Location preference check
        if not matches_location_preference(job, locations, work_pref):
            rejection_reasons["location"] += 1
            continue
            
        # 5. Work preference check
        is_remote = getattr(job, 'is_remote', False)
        desc_lower = (getattr(job, 'description', '') or "").lower()
        title_lower = job_title.lower()
        
        if work_pref == 'remote':
            if not is_remote:
                rejection_reasons["work_pref"] += 1
                continue
        elif work_pref == 'hybrid':
            is_hybrid = False
            if "hybrid" in desc_lower or "hybrid" in title_lower:
                is_hybrid = True
            if not is_hybrid:
                rejection_reasons["work_pref"] += 1
                continue
                
        # 6. Sponsorship check
        if sponsorship == 'yes':
            if "no sponsorship" in desc_lower or "no h1b" in desc_lower or "no corp to corp" in desc_lower or "no c2c" in desc_lower:
                rejection_reasons["sponsorship"] += 1
                continue
                
        # 7. Client Preferences (custom rule matching)
        if not matches_preferences(job, client_prefs):
            rejection_reasons["client_prefs"] += 1
            continue
            
        seen_urls.add(job.job_url)
        filtered_jobs.append(job)
        
    print(f"Filter stats for {requirements.get('applywizz_id', 'client')}: {len(jobs)} input -> {len(filtered_jobs)} matched. Rejections: {rejection_reasons}")
    return filtered_jobs

