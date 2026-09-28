import json
import re
import html
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict, Any
from bs4 import BeautifulSoup

from jobspy_enhanced.model import (
    JobPost,
    Location,
    JobType,
    Compensation,
    CompensationInterval,
)
from jobspy_enhanced.util import (
    extract_emails_from_text,
    markdown_converter,
)
from jobspy_enhanced.dice.constant import SKILL_JUNK_RE

def clean_url(url: str) -> str:
    if not url:
        return ""
    url = url.replace('\\/', '/').replace('\\u002F', '/').replace('\\u0026', '&').replace('\\"', '"').replace('\\\\', '\\')
    url = url.split('?')[0].split('#')[0]
    return re.sub(r'[\\"].*$', '', url).strip()

def find_career_url_in_dict(obj: Any) -> Optional[str]:
    career_patterns = ['workday', 'taleo', 'greenhouse', 'lever', 'icims', 'myworkday', 'salesforce.wd12', 'jobvite', 'smartrecruiters']
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and any(p in value.lower() for p in career_patterns) and 'http' in value:
                m = re.search(r'(https?://[^\s"\'<>]+)', value)
                if m:
                    url = clean_url(m.group(1))
                    if 'dice.com' not in url:
                        return url
            elif isinstance(value, (dict, list)):
                result = find_career_url_in_dict(value)
                if result:
                    return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_career_url_in_dict(item)
            if result:
                return result
    return None

def normalize_employment_badge(badge_text: str) -> Optional[str]:
    """Normalizes an individual employment badge string from Dice header card."""
    if not badge_text:
        return None
    b = badge_text.strip().lower()
    
    # Contract / W2 / C2C variations
    if 'contract w2' in b or 'contract w-2' in b:
        return 'Contract W2'
    if 'contract independent' in b or 'independent contractor' in b:
        return 'Contract Independent'
    if 'contract corp to corp' in b or 'contract c2c' in b or 'corp to corp' in b or 'corp-to-corp' in b or 'c2c' in b:
        return 'Contract Corp To Corp'
    if '1099' in b:
        return '1099'
    if b == 'contract' or 'contract to hire' in b:
        return 'Contract'
    if b == 'w2' or b == 'w-2':
        return 'Contract W2'
        
    # Full Time variations
    if b in ['full time', 'full-time', 'fulltime', 'full_time']:
        return 'Full Time'
        
    # Internship variations
    if 'internship' in b or b in ['intern', 'co-op', 'coop', 'co-operative']:
        return 'Internship'
        
    # Part Time variations
    if b in ['part time', 'part-time', 'parttime', 'part_time']:
        return 'Part Time'
        
    return None

def extract_dice_employment_type(soup: BeautifulSoup, job_data: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Extracts the normalized employment type for the CURRENT Dice job.
    Prioritizes the job detail header card badges.
    Falls back to current job's structured data (JSON-LD or __NEXT_DATA__).
    Does NOT scan arbitrary page HTML or unrelated job recommendations.
    """
    if not soup:
        return None

    # 1. Primary: Extract from header card badges
    header_card = soup.find(attrs={'data-testid': 'job-detail-header-card'})
    if header_card:
        badge_elements = header_card.find_all(class_=re.compile(r'SeuiInfoBadge', re.I))
        if not badge_elements:
            badge_elements = header_card.find_all(attrs={'data-testid': re.compile(r'badge|chip', re.I)})
            
        found_badges = []
        for elem in badge_elements:
            text = elem.get_text(strip=True)
            norm = normalize_employment_badge(text)
            if norm and norm not in found_badges:
                found_badges.append(norm)
                
        if found_badges:
            return ", ".join(found_badges)
            
    # 2. Fallback: Structured JSON-LD for the current job (@type == 'JobPosting')
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            candidate = None
            if isinstance(data, dict) and data.get('@type') == 'JobPosting':
                candidate = data.get('employmentType')
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'JobPosting':
                        candidate = item.get('employmentType')
                        break
            if candidate:
                cand_str = str(candidate).strip()
                cand_lower = cand_str.lower()
                if 'full_time' in cand_lower or cand_lower == 'full time':
                    return 'Full Time'
                elif 'contractor' in cand_lower or 'contract' in cand_lower:
                    return 'Contract'
                elif 'intern' in cand_lower:
                    return 'Internship'
                elif 'part_time' in cand_lower or cand_lower == 'part time':
                    return 'Part Time'
                norm = normalize_employment_badge(cand_str)
                if norm:
                    return norm
        except Exception:
            continue
            
    # 3. Fallback: job_data from __NEXT_DATA__
    if job_data and isinstance(job_data, dict):
        raw_jt = job_data.get('employmentType') or job_data.get('jobType')
        if raw_jt:
            cand_str = str(raw_jt).strip()
            cand_lower = cand_str.lower()
            if 'full_time' in cand_lower or cand_lower == 'full time':
                return 'Full Time'
            elif 'contractor' in cand_lower or 'contract' in cand_lower:
                return 'Contract'
            elif 'intern' in cand_lower:
                return 'Internship'
            elif 'part_time' in cand_lower or cand_lower == 'part time':
                return 'Part Time'
            norm = normalize_employment_badge(cand_str)
            if norm:
                return norm

    return None

def extract_employment_type_from_raw_html(raw_html: str) -> Optional[str]:
    if not raw_html:
        return None
    patterns = [
        r'"employmentType"\s*:\s*"([^"]+)"',
        r'\\"employmentType\\"\s*:\s*\\"([^\\]+)\\"',
        r'"employment_type"\s*:\s*"([^"]+)"',
        r'\\"employment_type\\"\s*:\s*\\"([^\\]+)\\"',
        r'"jobType"\s*:\s*"([^"]+)"',
        r'\\"jobType\\"\s*:\s*\\"([^\\]+)\\"',
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_html, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value:
                return value
    return None

def extract_apply_url_from_raw_html(raw_html: str) -> Optional[str]:
    if not raw_html:
        return None
    patterns = [
        r'"applyUrl"\s*:\s*"(https?://[^"]+?)"',
        r'\\"applyUrl\\":\s*\\"(https?://[^\\]+?)\\"',
        r'"externalApplyUrl"\s*:\s*"(https?://[^"]+?)"',
        r'\\"externalApplyUrl\\":\s*\\"(https?://[^\\]+?)\\"',
        r'"directApplyUrl"\s*:\s*"(https?://[^"]+?)"',
        r'\\"directApplyUrl\\":\s*\\"(https?://[^\\]+?)\\"',
    ]
    career_domains = ['workday', 'myworkday', 'taleo', 'greenhouse', 'lever', 'icims', 'jobvite', 'smartrecruiters', 'breezy', 'bamboohr', 'applytojob', 'recruiting', 'careers', 'jobs', 'wd1.', 'wd5.', 'wd12.', 'employment']
    for pattern in patterns:
        for match in re.findall(pattern, raw_html):
            url = match.strip().replace('\\/', '/').replace('\\u002F', '/').replace('\\u0026', '&').replace('\\"', '"').replace('\\\\', '\\')
            url = re.sub(r'[\\"].*$', '', url)
            if url.startswith('http') and 'dice.com' not in url.lower():
                if any(d in url.lower() for d in career_domains) or len(url) > 30:
                    return url
    career_url_pattern = r'(https?://(?:www\.)?(?:[a-zA-Z0-9-]+\.)?(?:workday|myworkday|taleo|greenhouse|lever|icims|jobvite|smartrecruiters)[^"\s<>]+)'
    for url in re.findall(career_url_pattern, raw_html, re.IGNORECASE):
        url = url.replace('\\/', '/').replace('\\u002F', '/').replace('\\u0026', '&')
        url = re.sub(r'[\\"].*$', '', url)
        if url.startswith('http') and 'dice.com' not in url:
            return url
    for url in re.findall(r'applyUrl[^h]*?(https?://[^"\s\\]+)', raw_html, re.IGNORECASE):
        url = url.replace('\\/', '/').replace('\\u002F', '/').replace('\\u0026', '&')
        url = re.sub(r'[\\"].*$', '', url)
        if url.startswith('http') and 'dice.com' not in url.lower():
            return url
    return None

def clean_description(description: str) -> str:
    if not description:
        return ""
    try:
        description = description.encode().decode('unicode_escape')
    except:
        pass
    description = html.unescape(description)
    description = re.sub(r'<br\s*/?\s*>', '\n', description, flags=re.IGNORECASE)
    description = re.sub(r'<[^>]+>', '', description)
    description = re.sub(r'\n{3,}', '\n\n', description)
    description = re.sub(r' {2,}', ' ', description)
    return description.strip()

def extract_salary_from_description(description: str) -> Optional[Compensation]:
    if not description:
        return None
    hourly_patterns = [
        r'\$\s*([\d,.]+)\s*(?:-|to|–|—)\s*\$?\s*([\d,.]+)\s*(?:per\s*hour|\/\s*hour|hourly|\/\s*hr|\bph\b|an\s*hour)',
        r'\$\s*([\d,.]+)\s*(?:per\s*hour|\/\s*hour|hourly|\/\s*hr|\bph\b|an\s*hour)',
        r'(?:hourly|rate|pay)\s*:?\s*\$\s*([\d,.]+)\s*(?:/hr|per\s*hour)?',
    ]
    for pattern in hourly_patterns:
        for match in re.finditer(pattern, description, re.IGNORECASE):
            try:
                groups = match.groups()
                min_val = float(groups[0].replace(',', ''))
                max_val = float(groups[1].replace(',', '')) if len(groups) >= 2 and groups[1] else min_val
                if 8 <= min_val <= 300 and 8 <= max_val <= 300 and min_val <= max_val:
                    return Compensation(interval=CompensationInterval.HOURLY, min_amount=min_val, max_amount=max_val, currency='USD')
            except (ValueError, IndexError, AttributeError):
                continue
    k_patterns = [
        r'\$\s*([\d,.]+)\s*[kK]\s*(?:-|to|–|—)\s*\$?\s*([\d,.]+)\s*[kK]',
        r'(?<!\$)\b([\d,.]+)\s*[kK]\s*(?:-|to|–|—)\s*([\d,.]+)\s*[kK]\b',
        r'(?:salary|compensation|pay|rate|base|annual)\s*:?\s*\$?\s*([\d,.]+)\s*[kK]\b',
        r'\$\s*([\d,.]+)\s*[kK]\b',
    ]
    for pattern in k_patterns:
        for match in re.finditer(pattern, description, re.IGNORECASE):
            try:
                groups = match.groups()
                min_val = int(float(groups[0].replace(',', '')) * 1000)
                max_val = int(float(groups[1].replace(',', '')) * 1000) if len(groups) >= 2 and groups[1] else min_val
                if 20000 <= min_val <= 800000 and 20000 <= max_val <= 800000 and min_val <= max_val:
                    return Compensation(interval=CompensationInterval.YEARLY, min_amount=min_val, max_amount=max_val, currency='USD')
            except (ValueError, IndexError, AttributeError):
                continue
    annual_context_patterns = [
        r'(?:salary|compensation|pay)\s*(?:range|is|of)?\s*:?\s*\$\s*([\d,]+)\s*(?:-|to|–|—)\s*\$?\s*([\d,]+)',
        r'(?:base|annual|yearly)\s*(?:salary|compensation|pay)\s*:?\s*\$\s*([\d,]+)',
        r'\$\s*([\d,]+)\s*(?:-|to|–|—)\s*\$?\s*([\d,]+)\s*(?:annually|per\s*year|\/\s*year|yearly|a\s*year|\/yr)',
        r'\$\s*([\d,]+)\s*(?:annually|per\s*year|\/\s*year|yearly|a\s*year|\/yr)',
    ]
    for pattern in annual_context_patterns:
        for match in re.finditer(pattern, description, re.IGNORECASE):
            try:
                groups = match.groups()
                min_val = int(groups[0].replace(',', ''))
                max_val = int(groups[1].replace(',', '')) if len(groups) >= 2 and groups[1] else min_val
                if 15000 <= min_val <= 1500000 and 15000 <= max_val <= 1500000 and min_val <= max_val:
                    return Compensation(interval=CompensationInterval.YEARLY, min_amount=min_val, max_amount=max_val, currency='USD')
            except (ValueError, IndexError, AttributeError):
                continue
    for match in re.finditer(r'\$\s*([\d,]+)\s*(?:-|to|–|—)\s*\$?\s*([\d,]+)(?!\s*(?:per\s*hour|\/\s*hr|hourly|ph\b))', description, re.IGNORECASE):
        try:
            min_val = int(match.group(1).replace(',', ''))
            max_val = int(match.group(2).replace(',', ''))
            if 25000 <= min_val <= 800000 and 25000 <= max_val <= 800000 and min_val < max_val and (max_val - min_val) >= 5000:
                return Compensation(interval=CompensationInterval.YEARLY, min_amount=min_val, max_amount=max_val, currency='USD')
        except (ValueError, IndexError):
            continue
    match = re.search(r'\$\s*([\d,]+)\s*(?:per\s*year|\/\s*year|yearly|annually)', description, re.IGNORECASE)
    if match:
        try:
            val = int(match.group(1).replace(',', ''))
            if 15000 <= val <= 1500000:
                return Compensation(interval=CompensationInterval.YEARLY, min_amount=val, max_amount=val, currency='USD')
        except (ValueError, IndexError):
            pass
    return None

def extract_experience_from_description(description: str) -> Optional[str]:
    if not description:
        return None
        
    cleaned = description.replace('\xa0', ' ').replace('&nbsp;', ' ')
    
    word_to_num = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14,
        'fifteen': 15, 'twenty': 20, 'thirty': 30
    }
    
    dash_pattern = r'[-–—]|to'
    exp_suffix = r'(?:\s+of)?(?:\s+[\w\s/,\.\(\)]+?)?\s*(?:experience|exp\b|professional|work|relevant|related|development|engineering|coding|hands-on|industry)'

    # 1. Prefix-based patterns (e.g. 'Experience Range: 4 - 6 Years', 'Experience: 5+ years', 'Years of Experience: 3-5')
    p = re.search(
        r'(?:(?:years?\s+of\s+)?experience|exp)(?:\s+(?:range|level|required|requirement|needed))?[:\s]+(\d+)\s*(?:' + dash_pattern + r')\s*(\d+)\s*(?:years?|yrs?)?',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}-{p.group(2)} years"
        
    p = re.search(
        r'(?:(?:years?\s+of\s+)?experience|exp)(?:\s+(?:range|level|required|requirement|needed))?[:\s]+(\d+)\s*(\+)?\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}+ years" if p.group(2) else f"{p.group(1)} years"

    # 2. Minimum / at least / required (e.g. 'Minimum 4 years hands-on coding', 'At least 5 years of software engineering')
    p = re.search(
        r'(?:minimum|minimum\s+of|at\s+least|atleast|require[sd]?)\s+(?:of\s+)?(\d+)\s*(?:' + dash_pattern + r')\s*(\d+)\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}-{p.group(2)} years"

    p = re.search(
        r'(?:minimum|minimum\s+of|at\s+least|atleast|require[sd]?)\s+(?:of\s+)?(\d+)\s*(?:\+)?\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}+ years"

    # 3. Numeric ranges (e.g. '3-5 years software engineering experience', '2 - 4 years experience required')
    p = re.search(
        r'(\d+)\s*(?:' + dash_pattern + r')\s*(\d+)\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}-{p.group(2)} years"

    # 4. Plus suffix (e.g. '5+ years of software engineering', '5+ years of Python development', '5+ years with AWS')
    p = re.search(
        r'(\d+)\s*\+\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)}+ years"

    # 5. Simple years with experience / domain keywords
    p = re.search(
        r'(\d+)\s*(?:years?|yrs?)' + exp_suffix,
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{p.group(1)} years"

    # 6. Word numbers (e.g. 'five to seven years of relevant experience', 'three+ years of web development')
    w_keys = '|'.join(word_to_num.keys())
    p = re.search(
        r'\b(' + w_keys + r')\s*(?:' + dash_pattern + r')\s*(' + w_keys + r')\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        return f"{word_to_num[p.group(1).lower()]}-{word_to_num[p.group(2).lower()]} years"

    p = re.search(
        r'\b(' + w_keys + r')\s*(?:\+|plus)?\s*(?:years?|yrs?)',
        cleaned,
        re.IGNORECASE
    )
    if p:
        val = word_to_num[p.group(1).lower()]
        return f"{val}+ years" if ('+' in p.group(0) or 'plus' in p.group(0).lower()) else f"{val} years"

    return None

def is_valid_skill(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if len(t) < 2 or len(t) > 60 or '$' in t:
        return False
    return not SKILL_JUNK_RE.search(t)

def extract_skills_from_html(soup: BeautifulSoup) -> Optional[List[str]]:
    skills = []
    skills_headings = soup.find_all(['h3', 'h4', 'div', 'span'], string=re.compile(r'^\s*skills?\s*$', re.IGNORECASE))
    for heading in skills_headings:
        next_elem = heading.find_next_sibling()
        if next_elem and next_elem.name in ['ul', 'ol']:
            for li in next_elem.find_all('li'):
                t = li.get_text(strip=True)
                if t and len(t) < 50:
                    skills.append(t)
        parent = heading.parent
        if parent:
            next_elem = parent.find_next_sibling()
            if next_elem and next_elem.name in ['ul', 'ol']:
                for li in next_elem.find_all('li'):
                    t = li.get_text(strip=True)
                    if t and len(t) < 50:
                        skills.append(t)
    skills_containers = []
    for heading in skills_headings:
        parent = heading.parent
        if parent:
            skills_containers.append(parent)
        grandparent = parent.parent if parent else None
        if grandparent:
            skills_containers.append(grandparent)
    for container in skills_containers:
        for badge in container.find_all(class_=re.compile(r'skill|badge|chip|tag', re.I)):
            t = badge.get_text(strip=True)
            if t and 2 < len(t) < 50 and t not in skills:
                skills.append(t)
        for badge in container.find_all(attrs={'data-testid': re.compile(r'skill', re.I)}):
            t = badge.get_text(strip=True)
            if t and 2 < len(t) < 50 and t not in skills:
                skills.append(t)
    unique_skills = []
    seen = set()
    for skill in skills:
        if not is_valid_skill(skill):
            continue
        k = skill.strip().lower()
        if k not in seen:
            seen.add(k)
            unique_skills.append(skill.strip())
    return unique_skills if unique_skills else None

def extract_skills_from_json(job_data: Dict[str, Any]) -> Optional[List[str]]:
    skills = []
    for field in ['skills', 'qualifications', 'requirements', 'skillTags', 'technologies']:
        val = job_data.get(field)
        if val:
            if isinstance(val, list):
                for item in val:
                    skills.append(item['name'] if isinstance(item, dict) and 'name' in item else item) if isinstance(item, (dict, str)) else None
            elif isinstance(val, str):
                for s in re.split(r'[,\n•]', val):
                    s = s.strip()
                    if s and len(s) < 50 and not s.startswith('http'):
                        skills.append(s)
    if 'jobPosting' in job_data:
        for field in ['skills', 'qualifications', 'requirements', 'skillTags', 'technologies']:
            val = job_data['jobPosting'].get(field)
            if val:
                if isinstance(val, list):
                    skills.extend([str(s) for s in val if s])
                elif isinstance(val, str):
                    skills.extend([s.strip() for s in val.split(',') if s.strip()])
    unique_skills = []
    seen = set()
    for s in skills:
        if not isinstance(s, str) or not is_valid_skill(s):
            continue
        k = s.strip().lower()
        if k and k not in seen:
            seen.add(k)
            unique_skills.append(s.strip())
    return unique_skills if unique_skills else None

def extract_skills_from_text(text: str) -> Optional[List[str]]:
    if not text:
        return None
    skills = []
    common_skills = [
        'python', 'java', 'javascript', 'typescript', 'react', 'angular', 'vue',
        'node', 'express', 'django', 'flask', 'spring', 'hibernate',
        'sql', 'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch',
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'git',
        'machine learning', 'deep learning', 'tensorflow', 'pytorch', 'keras',
        'data science', 'data analysis', 'pandas', 'numpy', 'scikit-learn',
        'html', 'css', 'sass', 'less', 'bootstrap', 'tailwind',
        'c++', 'c#', '.net', 'php', 'ruby', 'go', 'rust', 'scala',
        'hadoop', 'spark', 'kafka', 'airflow', 'tableau', 'power bi'
    ]
    text_lower = text.lower()
    for skill in common_skills:
        if skill in text_lower:
            skills.append(skill.title())
    for item in re.findall(r'[•\-]\s*([A-Za-z][A-Za-z\s]{2,30})', text):
        item = item.strip()
        if item and len(item) < 50 and item.lower() not in [s.lower() for s in skills]:
            skills.append(item)
    return [s for s in skills if is_valid_skill(s)] or None

def extract_from_next_data(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    if not next_data_script or not next_data_script.string:
        return None
    try:
        next_data = json.loads(next_data_script.string)
        if 'props' in next_data and 'pageProps' in next_data['props']:
            page_props = next_data['props']['pageProps']
            if 'jobData' in page_props:
                return page_props['jobData']
            if 'dehydratedState' in page_props and 'queries' in page_props['dehydratedState']:
                for query in page_props['dehydratedState']['queries']:
                    if query and 'state' in query and 'data' in query['state']:
                        data = query['state']['data']
                        if isinstance(data, dict):
                            if data.get('jobPosting'):
                                return data['jobPosting']
                            elif data.get('jobId') or data.get('id'):
                                return data
                            elif 'job' in data:
                                return data['job']
    except Exception:
        pass
    return None

def is_job_expired(soup: BeautifulSoup, raw_html: str = "") -> bool:
    """
    Checks if a Dice job posting is expired or no longer available.
    Dice returns HTTP 200 even for expired postings, but displays an inline warning:
    e.g. 'Sorry this job is no longer available. The Similar Jobs shown below might interest you.'
    """
    if not soup and not raw_html:
        return False

    if soup:
        alert_elements = soup.find_all(attrs={"data-testid": "inline-message"})
        alert_elements += soup.find_all(attrs={"role": "alert"})
        for el in alert_elements:
            txt = el.get_text().lower()
            if any(p in txt for p in ["no longer available", "expired", "closed"]):
                return True

    text_to_check = soup.get_text().lower() if soup else (raw_html.lower() if raw_html else "")
    expiry_phrases = [
        "this job is no longer available",
        "job is no longer available",
        "sorry this job is no longer available",
        "this job has expired",
        "position has expired",
        "job posting has expired",
    ]
    if any(phrase in text_to_check for phrase in expiry_phrases):
        return True

    return False

def extract_structured_data(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get('@type') == 'JobPosting':
                return data
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'JobPosting':
                        return item
        except:
            continue
    return None

def extract_base_salary_from_raw_html(raw_html: str) -> Optional[Compensation]:
    if not raw_html:
        return None
    try:
        m = re.search(r'"baseSalary"\s*:\s*(\{[^}]+\})', raw_html)
        if m:
            salary_json = json.loads(m.group(1))
            currency = salary_json.get('currency', 'USD')
            if 'minValue' in salary_json and 'maxValue' in salary_json:
                mn, mx = float(salary_json['minValue']), float(salary_json['maxValue'])
                if mn < 500 and mx < 500:
                    return Compensation(interval=CompensationInterval.HOURLY, min_amount=mn, max_amount=mx, currency=currency)
                return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(mn), max_amount=int(mx), currency=currency)
            elif 'value' in salary_json:
                if isinstance(salary_json['value'], dict):
                    mn = salary_json['value'].get('minValue')
                    mx = salary_json['value'].get('maxValue')
                    if mn and mx:
                        mn, mx = float(mn), float(mx)
                        if mn < 500 and mx < 500:
                            return Compensation(interval=CompensationInterval.HOURLY, min_amount=mn, max_amount=mx, currency=currency)
                        return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(mn), max_amount=int(mx), currency=currency)
                else:
                    val = float(salary_json['value'])
                    if val < 500:
                        return Compensation(interval=CompensationInterval.HOURLY, min_amount=val, max_amount=val, currency=currency)
                    return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(val), max_amount=int(val), currency=currency)
    except Exception:
        pass
    return None

def extract_salary_from_json(job_data: Dict[str, Any]) -> Optional[Compensation]:
    base_salary = job_data.get('baseSalary') or job_data.get('salary') or job_data.get('compensation')
    if not base_salary:
        return None
    currency = 'USD'
    if isinstance(base_salary, dict):
        currency = base_salary.get('currency', 'USD')
        if 'minValue' in base_salary and 'maxValue' in base_salary:
            mn, mx = float(base_salary['minValue']), float(base_salary['maxValue'])
            if mn < 500 and mx < 500:
                return Compensation(interval=CompensationInterval.HOURLY, min_amount=mn, max_amount=mx, currency=currency)
            return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(mn), max_amount=int(mx), currency=currency)
        elif 'minValue' in base_salary:
            val = float(base_salary['minValue'])
            if val < 500:
                return Compensation(interval=CompensationInterval.HOURLY, min_amount=val, max_amount=val, currency=currency)
            return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(val), max_amount=int(val), currency=currency)
        elif 'value' in base_salary:
            if isinstance(base_salary['value'], dict):
                mn = base_salary['value'].get('minValue')
                mx = base_salary['value'].get('maxValue')
                if mn and mx:
                    mn, mx = float(mn), float(mx)
                    if mn < 500 and mx < 500:
                        return Compensation(interval=CompensationInterval.HOURLY, min_amount=mn, max_amount=mx, currency=currency)
                    return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(mn), max_amount=int(mx), currency=currency)
            else:
                try:
                    val = float(base_salary['value'])
                    if val < 500:
                        return Compensation(interval=CompensationInterval.HOURLY, min_amount=val, max_amount=val, currency=currency)
                    return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(val), max_amount=int(val), currency=currency)
                except (ValueError, TypeError):
                    pass
    elif isinstance(base_salary, (int, float, str)):
        try:
            val = float(base_salary)
            if val < 500:
                return Compensation(interval=CompensationInterval.HOURLY, min_amount=val, max_amount=val, currency='USD')
            return Compensation(interval=CompensationInterval.YEARLY, min_amount=int(val), max_amount=int(val), currency='USD')
        except (ValueError, TypeError):
            pass
    return None

NON_US_COUNTRIES = {
    'uk': 'UK', 'united kingdom': 'UK', 'great britain': 'UK', 'england': 'UK', 'scotland': 'UK', 'wales': 'UK', 'gb': 'UK',
    'ca': 'CA', 'canada': 'CA',
    'ie': 'IE', 'ireland': 'IE',
    'de': 'DE', 'germany': 'DE',
    'in': 'IN', 'india': 'IN',
    'au': 'AU', 'australia': 'AU',
}
CA_PROVINCES = {'on', 'ontario', 'bc', 'british columbia', 'ab', 'alberta', 'qc', 'quebec', 'mb', 'manitoba', 'sk', 'saskatchewan', 'ns', 'nova scotia', 'nb', 'new brunswick', 'nl', 'newfoundland', 'pe', 'prince edward island'}
US_COUNTRY_ALIASES = {'us', 'usa', 'united states'}

def parse_location(location_text: str) -> Optional[Location]:
    if not location_text:
        return None
    location_text = re.sub(r'^(location:?\s*|city:?\s*|in\s+)', '', location_text, flags=re.I)
    parts = [p.strip() for p in location_text.split(',') if p.strip()]
    if not parts:
        return None

    if len(parts) == 1:
        p_low = parts[0].lower()
        if p_low in NON_US_COUNTRIES:
            return Location(country=NON_US_COUNTRIES[p_low])
        elif p_low in US_COUNTRY_ALIASES:
            return Location(country='USA')
        return Location(city=parts[0])

    if len(parts) == 2:
        p0, p1 = parts[0], parts[1]
        p1_low = p1.lower()
        if p1_low in NON_US_COUNTRIES:
            return Location(city=p0, country=NON_US_COUNTRIES[p1_low])
        if p1_low in CA_PROVINCES:
            return Location(city=p0, state=p1, country='CA')
        if p1_low in US_COUNTRY_ALIASES:
            return Location(city=p0, country='USA')
        return Location(city=p0, state=p1, country='USA')

    p_last_low = parts[-1].lower()
    c_str = NON_US_COUNTRIES.get(p_last_low) or ('USA' if p_last_low in US_COUNTRY_ALIASES else parts[-1])
    return Location(city=parts[0], state=parts[1], country=c_str)

def parse_posted_date(date_text: str) -> Optional[str]:
    if not date_text:
        return None
    date_text = date_text.lower().strip()
    today = datetime.now()
    if 'today' in date_text:
        return today.strftime("%Y-%m-%d")
    elif 'yesterday' in date_text:
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r'(\d+)\s+days?\s+ago', date_text)
    if m:
        return (today - timedelta(days=int(m.group(1)))).strftime("%Y-%m-%d")
    return None

def map_job_type(job_type_text: str) -> Optional[JobType]:
    if not job_type_text:
        return None
    t = job_type_text.lower()
    mapping = {
        'full time': JobType.FULL_TIME, 'full-time': JobType.FULL_TIME, 'full_time': JobType.FULL_TIME,
        'part time': JobType.PART_TIME, 'part-time': JobType.PART_TIME, 'part_time': JobType.PART_TIME,
        'contract': JobType.CONTRACT, 'contractor': JobType.CONTRACT,
        'internship': JobType.INTERNSHIP,
    }
    for key, value in mapping.items():
        if key in t:
            return value
    return None

def classify_w2_c2c(title: str = "", description: str = "", full_page_text: str = "", employment_type: str = "") -> Optional[str]:
    """
    Classifies job into 'W2', 'C2C', or None.
    Uses normalized employment_type first.
    Never scans broad full_page_text to avoid matching JavaScript chunks or CSS classes.
    """
    emp_lower = (employment_type or "").lower().strip()
    if emp_lower:
        if any(k in emp_lower for k in ['c2c', 'c-2-c', 'corp to corp', 'corp-to-corp']):
            return 'C2C'
        if any(k in emp_lower for k in ['w2', 'w-2', '1099', 'contract independent', 'independent contractor']):
            return 'W2'
        if 'contract' in emp_lower:
            return 'W2'
        if 'full time' in emp_lower or 'internship' in emp_lower:
            return None

    # Targeted phrases in title only if employment_type is not present
    combined = f"{title}".lower()
    C2C_KEYWORDS = ['c2c', 'c-2-c', 'corp to corp', 'corp-to-corp']
    W2_KEYWORDS = ['contract w2', 'contract independent', 'contract corp to corp', '1099', 'independent contractor']
    
    if any(re.search(r'\b' + re.escape(kw) + r'\b', combined) for kw in C2C_KEYWORDS):
        return 'C2C'
    if any(re.search(r'\b' + re.escape(kw) + r'\b', combined) for kw in W2_KEYWORDS):
        return 'W2'
    return None

def extract_external_apply_url_fallback(soup: BeautifulSoup, job_data: Optional[Dict] = None) -> Optional[str]:
    if job_data:
        for field in ['applyUrl', 'externalApplyUrl', 'directApplyUrl', 'url']:
            u = job_data.get(field)
            if u and isinstance(u, str):
                u = clean_url(u)
                if u.startswith('http') and 'dice.com' not in u:
                    return u
    for selector in [{'data-testid': 'apply-button'}, {'data-testid': 'external-apply-button'}, {'data-testid': 'direct-apply-button'}]:
        btn = soup.find('a', attrs=selector)
        if btn and btn.get('href'):
            href = clean_url(btn.get('href'))
            if href.startswith('http') and 'dice.com' not in href:
                return href
    for script in soup.find_all('script'):
        text = script.string or ''
        for pattern in [r'applyUrl["\']?\s*:\s*["\'](https?://[^"\']+)["\']', r'externalApplyUrl["\']?\s*:\s*["\'](https?://[^"\']+)["\']']:
            m = re.search(pattern, text)
            if m:
                u = clean_url(m.group(1))
                if u.startswith('http') and 'dice.com' not in u:
                    return u
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        if any(p in href.lower() for p in ['taleo', 'workday', 'greenhouse', 'lever', 'icims', 'myworkday', 'salesforce.wd12']):
            href = clean_url(href)
            if href.startswith('http'):
                return href
    return None

def format_location_for_db(loc: Any) -> str:
    """Format location as 'TX, US' (state, country) for database storage."""
    parts = []
    if getattr(loc, 'state', None):
        parts.append(loc.state)
    if getattr(loc, 'country', None):
        country = str(loc.country)
        if '.' in country:
            country = country.split('.')[-1]
        parts.append(country)
    if parts:
        return ", ".join(parts)
    if getattr(loc, 'city', None):
        return loc.city
    return str(loc) if loc else ""

def format_compensation_for_db(comp: Any) -> str:
    """Format compensation as '$ 192700.0/yearly' for database storage."""
    if not comp:
        return ""
    currency = getattr(comp, 'currency', 'USD') or 'USD'
    symbol = '$' if 'USD' in str(currency).upper() else str(currency)
    min_amt = getattr(comp, 'min_amount', None)
    max_amt = getattr(comp, 'max_amount', None)
    interval = getattr(comp, 'interval', None)
    interval_str = ""
    if interval:
        interval_str = str(interval.value) if hasattr(interval, 'value') else str(interval)

    if min_amt and max_amt and min_amt != max_amt:
        return f"{symbol} {min_amt} - {max_amt}/{interval_str}" if interval_str else f"{symbol} {min_amt} - {max_amt}"
    elif min_amt or max_amt:
        amt = min_amt or max_amt
        return f"{symbol} {amt}/{interval_str}" if interval_str else f"{symbol} {amt}"
    return ""

def insert_to_supabase(supabase_client: Any, jobs: List[JobPost], search_term: str) -> int:
    """Insert jobs into Supabase dice_jobs table, skipping duplicates."""
    inserted = 0
    for job in jobs:
        record = {
            "job_id": job.id or "",
            "search_term": search_term,
            "title": job.title or "",
            "company_name": job.company_name or "",
            "company_logo_url": getattr(job, 'company_logo', None) or "",
            "location": format_location_for_db(job.location) if job.location else "",
            "is_remote": getattr(job, 'is_remote', False) or False,
            "job_type": ", ".join([jt.value[0] if hasattr(jt, 'value') else str(jt) for jt in job.job_type]) if job.job_type else "",
            "w2_c2c_type": getattr(job, 'w2_c2c_type', None) or "",
            "employment_type": getattr(job, 'employment_type', None) or "",
            "date_posted": str(job.date_posted) if job.date_posted else "",
            "job_url": str(job.job_url) if job.job_url else "",
            "apply_url": job.job_url_direct or "",
            "compensation": format_compensation_for_db(job.compensation) if job.compensation else "",
            "experience": str(getattr(job, 'experience', None) or ""),
            "skills": ", ".join(job.skills) if job.skills else "",
            "description": (job.description or "")[:5000],
        }

        try:
            supabase_client.table("dice_jobs").upsert(
                record, on_conflict="job_id"
            ).execute()
            inserted += 1
        except Exception as e:
            print(f"    ⚠ Error inserting {job.id}: {e}")

    return inserted
