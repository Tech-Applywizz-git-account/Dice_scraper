from __future__ import annotations
import json
import re
import urllib.parse
import os
import traceback
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client

from bs4 import BeautifulSoup

from jobspy_enhanced.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Country,
    Location,
)
from jobspy_enhanced.util import (
    create_session,
    create_logger,
)
from jobspy_enhanced.dice.constant import DICE_BASE_URL, SEARCH_TERMS
from jobspy_enhanced.dice import util

log = create_logger("Dice")

class Dice(Scraper):
    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes DiceScraper with the Dice base URL
        """
        super().__init__(Site.DICE, proxies=proxies)
        self.session = create_session(proxies=self.proxies, ca_cert=ca_cert, is_tls=False)
        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})
        else:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
        self.scraper_input = None
        self.jobs_per_page = 20
        self.num_workers = 10
        self.seen_urls = set()
        self.base_url = DICE_BASE_URL

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        self.scraper_input = scraper_input
        job_list = []
        page = 1
        max_pages = 30
        while len(job_list) < scraper_input.results_wanted and page <= max_pages:
            log.info(f"Searching page {page}...")
            jobs = self._scrape_page(page)
            if not jobs:
                log.info(f"No jobs found on page {page}")
                break
            existing_ids = {j.id for j in job_list}
            for job in jobs:
                if job.id not in existing_ids:
                    job_list.append(job)
                    if len(job_list) >= scraper_input.results_wanted:
                        break
            page += 1
        paginated_jobs = job_list[:scraper_input.results_wanted]
        log.info(f"Total jobs collected: {len(paginated_jobs)}")
        return JobResponse(jobs=paginated_jobs)

    def _scrape_page(self, page: int) -> List[JobPost]:
        jobs = []
        params = {
            "q": self.scraper_input.search_term or "",
            "countryCode": "US",
            "radius": "30",
            "radiusUnit": "mi",
            "pageSize": str(self.jobs_per_page),
            "page": str(page),
            "language": "en",
        }
        if self.scraper_input.hours_old:
            if self.scraper_input.hours_old <= 24:
                params["filters.postedDate"] = "ONE"
            elif self.scraper_input.hours_old <= 72:
                params["filters.postedDate"] = "THREE"
            elif self.scraper_input.hours_old <= 168:
                params["filters.postedDate"] = "SEVEN"
            else:
                params["filters.postedDate"] = "THIRTY"
        if self.scraper_input.is_remote:
            params["filters.isRemote"] = "true"
        if self.scraper_input.location:
            params["location"] = self.scraper_input.location

        url = f"{self.base_url}/jobs?" + urllib.parse.urlencode(params)
        log.debug(f"Search URL: {url}")
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
        except Exception as e:
            log.error(f"Error fetching page {page}: {e}")
            return jobs

        soup = BeautifulSoup(response.text, 'html.parser')
        job_ids = []
        seen_ids = set()

        for script in soup.find_all('script'):
            text = script.string or ''
            for m in re.finditer(r'"id"\s*:\s*"([a-f0-9-]{36})"', text):
                jid = m.group(1)
                if jid not in seen_ids:
                    seen_ids.add(jid)
                    job_ids.append(jid)

        if not job_ids:
            for a_tag in soup.find_all('a', href=re.compile(r'/job-detail/')):
                href = a_tag.get('href', '')
                m = re.search(r'/job-detail/([a-f0-9-]{36})', href)
                if m:
                    jid = m.group(1)
                    if jid not in seen_ids:
                        seen_ids.add(jid)
                        job_ids.append(jid)

        log.info(f"Found {len(job_ids)} job IDs on page {page}")
        ids_to_fetch = job_ids[:self.jobs_per_page]
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self._fetch_job_details, jid): jid for jid in ids_to_fetch}
            for future in as_completed(futures):
                try:
                    job = future.result()
                    if job:
                        jobs.append(job)
                except Exception as e:
                    log.error(f"Error fetching job {futures[future]}: {e}")
        return jobs

    def _fetch_job_details(self, job_id: str) -> Optional[JobPost]:
        job_url = f"{self.base_url}/job-detail/{job_id}"
        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)
        log.debug(f"Fetching job details: {job_url}")
        try:
            response = self.session.get(job_url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            log.error(f"Error fetching job details for {job_id}: {e}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        raw_html = response.text
        employment_type_raw = util.extract_employment_type_from_raw_html(raw_html)

        # Primary: __NEXT_DATA__
        job_data_from_next = util.extract_from_next_data(soup)
        if job_data_from_next:
            job_post = self._process_next_data_job(job_data_from_next, job_id, job_url, raw_html=raw_html, employment_type_raw=employment_type_raw)
            if job_post:
                self._apply_w2_c2c_and_link(job_post, soup, raw_html, job_id)
                if not job_post.compensation or not getattr(job_post, 'experience', None) or not getattr(job_post, 'w2_c2c_type', None):
                    self._enrich_with_headless(job_post, job_url)
                return job_post

        # Fallback: JSON-LD
        job_data_from_jsonld = util.extract_structured_data(soup)
        if job_data_from_jsonld:
            job_post = self._process_structured_job(job_data_from_jsonld, job_id, job_url, soup, raw_html=raw_html, employment_type_raw=employment_type_raw)
            if job_post:
                self._apply_w2_c2c_and_link(job_post, soup, raw_html, job_id)
                if not job_post.compensation or not getattr(job_post, 'experience', None) or not getattr(job_post, 'w2_c2c_type', None):
                    self._enrich_with_headless(job_post, job_url)
                return job_post

        # Final fallback: HTML parse
        log.warning(f"Falling back to HTML parsing for job {job_id}")
        job_post = self._parse_job_detail_page(soup, job_id, job_url, raw_html=raw_html, employment_type_raw=employment_type_raw)
        if job_post:
            self._apply_w2_c2c_and_link(job_post, soup, raw_html, job_id)
            if not job_post.compensation or not getattr(job_post, 'experience', None) or not getattr(job_post, 'w2_c2c_type', None):
                self._enrich_with_headless(job_post, job_url)
        return job_post

    def _process_next_data_job(self, job_data: Dict[str, Any], job_id: str, job_url: str, raw_html: str = "", employment_type_raw: str = None) -> JobPost:
        title = job_data.get('title')
        company = job_data.get('companyName') or job_data.get('company', {}).get('name')
        location_raw = job_data.get('location') or job_data.get('jobLocation', {}).get('address', {}).get('addressLocality')
        location = util.parse_location(location_raw) if isinstance(location_raw, str) else None
        description = util.clean_description(job_data.get('description', ''))
        date_posted = util.parse_posted_date(job_data.get('postedDate') or job_data.get('datePosted'))
        
        job_type = []
        jt = util.map_job_type(employment_type_raw or job_data.get('employmentType'))
        if jt: job_type.append(jt)

        compensation = util.extract_salary_from_json(job_data)
        if not compensation:
            compensation = util.extract_salary_from_description(description)
        if not compensation and raw_html:
            compensation = util.extract_base_salary_from_raw_html(raw_html)

        experience = util.extract_experience_from_description(description)
        skills = util.extract_skills_from_json(job_data)
        if not skills:
            skills = util.extract_skills_from_text(description)
        
        external_url = util.extract_apply_url_from_raw_html(raw_html)
        if not external_url:
            external_url = util.find_career_url_in_dict(job_data)

        job_post = JobPost(
            id=job_id, title=title, company_name=company, location=location, job_url=job_url,
            job_url_direct=external_url, description=description, date_posted=date_posted,
            job_type=job_type, compensation=compensation
        )
        object.__setattr__(job_post, 'employment_type', employment_type_raw)
        if experience: object.__setattr__(job_post, 'experience', experience)
        if skills: object.__setattr__(job_post, 'skills', skills)
        
        w2_c2c = util.classify_w2_c2c(title or "", description or "", raw_html or "")
        if w2_c2c: object.__setattr__(job_post, 'w2_c2c_type', w2_c2c)
        
        return job_post

    def _process_structured_job(self, job_data: Dict[str, Any], job_id: str, job_url: str, soup: BeautifulSoup, raw_html: str = "", employment_type_raw: str = None) -> JobPost:
        title = job_data.get('title')
        company = job_data.get('hiringOrganization', {}).get('name')
        loc = job_data.get('jobLocation', {}).get('address', {})
        location = Location(city=loc.get('addressLocality'), state=loc.get('addressRegion'), country=loc.get('addressCountry') or 'USA')
        description = util.clean_description(job_data.get('description', ''))
        date_posted = job_data.get('datePosted', '').split('T')[0] if job_data.get('datePosted') else None
        
        job_type = []
        jt = util.map_job_type(employment_type_raw or job_data.get('employmentType'))
        if jt: job_type.append(jt)

        compensation = util.extract_salary_from_json(job_data)
        if not compensation:
            compensation = util.extract_salary_from_description(description)

        experience = util.extract_experience_from_description(description)
        skills = util.extract_skills_from_html(soup)
        if not skills:
            skills = util.extract_skills_from_text(description)
            
        external_url = util.extract_apply_url_from_raw_html(raw_html)

        job_post = JobPost(
            id=job_id, title=title, company_name=company, location=location, job_url=job_url,
            job_url_direct=external_url, description=description, date_posted=date_posted,
            job_type=job_type, compensation=compensation
        )
        object.__setattr__(job_post, 'employment_type', employment_type_raw)
        if experience: object.__setattr__(job_post, 'experience', experience)
        if skills: object.__setattr__(job_post, 'skills', skills)
        
        w2_c2c = util.classify_w2_c2c(title or "", description or "", raw_html or "")
        if w2_c2c: object.__setattr__(job_post, 'w2_c2c_type', w2_c2c)
        
        return job_post

    def _parse_job_detail_page(self, soup: BeautifulSoup, job_id: str, job_url: str, raw_html: str = "", employment_type_raw: str = None) -> JobPost:
        title_elem = soup.find('h1') or soup.find(attrs={'data-testid': 'job-header-title'})
        title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"
        
        company_elem = soup.find(attrs={'data-testid': 'company-name'}) or soup.find('a', href=re.compile(r'/company/'))
        company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"
        
        location_elem = soup.find(attrs={'data-testid': 'job-header-location'})
        location = util.parse_location(location_elem.get_text(strip=True)) if location_elem else None
        
        desc_elem = soup.find(id='jobDescription') or soup.find(attrs={'data-testid': 'job-description'})
        description = util.clean_description(str(desc_elem)) if desc_elem else ""
        
        date_elem = soup.find(attrs={'data-testid': 'job-header-posted-date'})
        date_posted = util.parse_posted_date(date_elem.get_text(strip=True)) if date_elem else None
        
        job_type = []
        jt = util.map_job_type(employment_type_raw)
        if jt: job_type.append(jt)

        compensation = util.extract_salary_from_description(description)
        if not compensation:
            compensation = util.extract_base_salary_from_raw_html(raw_html)

        experience = util.extract_experience_from_description(description)
        skills = util.extract_skills_from_html(soup)
        if not skills:
            skills = util.extract_skills_from_text(description)
            
        external_url = util.extract_apply_url_from_raw_html(raw_html)
        if not external_url:
            external_url = util.extract_external_apply_url_fallback(soup)

        job_post = JobPost(
            id=job_id, title=title, company_name=company, location=location, job_url=job_url,
            job_url_direct=external_url, description=description, date_posted=date_posted,
            job_type=job_type, compensation=compensation
        )
        object.__setattr__(job_post, 'employment_type', employment_type_raw)
        if experience: object.__setattr__(job_post, 'experience', experience)
        if skills: object.__setattr__(job_post, 'skills', skills)
        
        w2_c2c = util.classify_w2_c2c(title or "", description or "", raw_html or "")
        if w2_c2c: object.__setattr__(job_post, 'w2_c2c_type', w2_c2c)
        
        return job_post

    def _extract_w2_c2c_from_header_card(self, soup: BeautifulSoup) -> Optional[str]:
        header_card = soup.find(attrs={'data-testid': 'job-detail-header-card'})
        if not header_card: return None
        badge_texts = [elem.get_text(strip=True).lower() for elem in header_card.find_all(class_=re.compile(r'SeuiInfoBadge', re.I)) if elem.get_text(strip=True)]
        if not badge_texts: return None
        badge_combined = ' | '.join(badge_texts)
        if any(k in badge_combined for k in ['w2', 'w-2']): return 'W2'
        if any(k in badge_combined for k in ['c2c', 'c-2-c', 'corp to corp', 'corp-to-corp']): return 'C2C'
        if any(k in badge_combined for k in ['contract independent', 'independent contractor', 'contract to hire']): return 'W2'
        return None

    def _extract_dice_apply_url(self, raw_html: str, job_id: str) -> Optional[str]:
        m = re.search(r'job-applications/([a-f0-9-]{36})/start-apply', raw_html)
        if m: return f"https://www.dice.com/job-applications/{m.group(1)}/start-apply"
        return f"https://www.dice.com/job-applications/{job_id}/start-apply"

    def _apply_w2_c2c_and_link(self, job_post: JobPost, soup: BeautifulSoup, raw_html: str, job_id: str) -> None:
        if not getattr(job_post, 'w2_c2c_type', None):
            w2_c2c = self._extract_w2_c2c_from_header_card(soup)
            if w2_c2c: object.__setattr__(job_post, 'w2_c2c_type', w2_c2c)
        if not job_post.job_url_direct:
            dice_apply_url = self._extract_dice_apply_url(raw_html, job_id)
            if dice_apply_url: job_post.job_url_direct = dice_apply_url
        if not getattr(job_post, 'company_logo', None):
            logo_url = self._extract_company_logo(soup, raw_html)
            if logo_url: object.__setattr__(job_post, 'company_logo', logo_url)

    def _extract_company_logo(self, soup: BeautifulSoup, raw_html: str) -> Optional[str]:
        header_card = soup.find(attrs={'data-testid': 'job-detail-header-card'})
        if header_card:
            img = header_card.find('img')
            if img and img.get('src') and str(img['src']).startswith('http'): return img['src']
        m = re.search(r'"logo"\s*:\s*"(https?://[^"]+)"', raw_html)
        return m.group(1) if m else None

    def _enrich_with_headless(self, job_post: JobPost, job_url: str) -> None:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            log.warning("Playwright not installed – skipping headless enrichment.")
            return

        log.info(f"[Headless] Fetching rendered page: {job_url}")
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                page = context.new_page()
                try:
                    page.goto(job_url, wait_until="domcontentloaded", timeout=20_000)
                    try:
                        page.wait_for_selector("[data-testid='job-description'], .job-description, #jobdescSec, [data-cy='job-description']", timeout=8_000)
                    except PWTimeout: pass
                    rendered_html = page.content()
                except PWTimeout:
                    log.warning(f"[Headless] Timed out: {job_url}")
                    browser.close()
                    return
                finally:
                    browser.close()
        except Exception as e:
            log.error(f"[Headless] Browser error: {e}")
            return

        soup = BeautifulSoup(rendered_html, "html.parser")
        salary_text = self._headless_extract_salary_text(soup)
        experience_text = self._headless_extract_experience_text(soup)
        description_text = self._headless_get_description_text(soup)

        if not job_post.compensation:
            compensation = util.extract_salary_from_description(salary_text) if salary_text else None
            if not compensation and description_text:
                compensation = util.extract_salary_from_description(description_text)
            if compensation:
                job_post.compensation = compensation
                log.info(f"[Headless] ✓ Salary enriched")

        if not getattr(job_post, 'experience', None):
            experience = util.extract_experience_from_description(experience_text) if experience_text else None
            if not experience and description_text:
                experience = util.extract_experience_from_description(description_text)
            if experience:
                object.__setattr__(job_post, 'experience', experience)
                log.info(f"[Headless] ✓ Experience enriched: {experience}")

        headless_w2_c2c = self._headless_extract_w2_c2c(soup)
        if headless_w2_c2c:
            object.__setattr__(job_post, 'w2_c2c_type', headless_w2_c2c)
            log.info(f"[Headless] ✓ W2/C2C enriched: {headless_w2_c2c}")

    def _headless_extract_w2_c2c(self, soup: BeautifulSoup) -> Optional[str]:
        badge_texts: List[str] = []
        for cls_re in [re.compile(r'chip|badge|tag|pill|label|employment.?type|job.?type', re.I)]:
            for elem in soup.find_all(class_=cls_re):
                t = elem.get_text(separator=" ", strip=True)
                if t: badge_texts.append(t)
        for attr_name in ("data-testid", "data-cy", "data-id"):
            for elem in soup.find_all(attrs={attr_name: re.compile(r'chip|badge|tag|pill|employment|job.?type|contract.?type', re.I)}):
                t = elem.get_text(separator=" ", strip=True)
                if t: badge_texts.append(t)
        for elem in soup.find_all(['span', 'div', 'li', 'button']):
            if len(elem.find_all()) <= 2:
                t = elem.get_text(separator=" ", strip=True)
                if t and len(t) <= 80: badge_texts.append(t)
        seen: set = set()
        unique_badges: List[str] = []
        for bt in badge_texts:
            key = bt.strip().lower()
            if key and key not in seen:
                seen.add(key); unique_badges.append(bt.strip())
        
        W2_PATTERNS = [re.compile(r'\bw-?2\b', re.I), re.compile(r'\bw\s+2\b', re.I)]
        C2C_PATTERNS = [re.compile(r'\bc2c\b', re.I), re.compile(r'\bc-2-c\b', re.I), re.compile(r'\bc\s+2\s+c\b', re.I), re.compile(r'\bcorp\s*[-–]\s*to\s*[-–]\s*corp\b', re.I), re.compile(r'\bcorp\s+to\s+corp\b', re.I), re.compile(r'\bcorporation\s+to\s+corporation\b', re.I)]
        CONTRACT_PATTERNS = [re.compile(r'\bcontract\s+w-?2\b', re.I), re.compile(r'\bcontract\s+to\s+hire\b', re.I), re.compile(r'\bcontract\s+position\b', re.I), re.compile(r'\bcontract\s+role\b', re.I), re.compile(r'\bcontract\s+opportunity\b', re.I), re.compile(r'\bcontract\s+employment\b', re.I), re.compile(r'\bcontract\s+worker\b', re.I), re.compile(r'\bcontract\s+job\b', re.I), re.compile(r'\bindependent\s+contractor\b', re.I), re.compile(r'\bcontract\s+independent\b', re.I), re.compile(r'\b1099\b', re.I), re.compile(r'\bcontractual\s+(?:position|role|employment)\b', re.I)]
        
        has_w2 = any(any(p.search(t) for p in W2_PATTERNS) for t in unique_badges)
        has_c2c = any(any(p.search(t) for p in C2C_PATTERNS) for t in unique_badges)
        has_contract = any(any(p.search(t) for p in CONTRACT_PATTERNS) for t in unique_badges)
        if has_w2: return 'W2'
        if has_c2c: return 'C2C'
        if has_contract: return 'W2'
        return None

    def _headless_extract_salary_text(self, soup: BeautifulSoup) -> Optional[str]:
        for selector in [{'data-testid': 'job-salary'}, {'data-testid': 'estimated-salary'}, {'class': re.compile(r'salary|compensation|pay', re.I)}]:
            elem = soup.find(attrs=selector) if 'data-testid' in selector else soup.find(class_=selector['class'])
            if elem:
                t = elem.get_text(separator=" ", strip=True)
                if '$' in t: return t
        return None

    def _headless_extract_experience_text(self, soup: BeautifulSoup) -> Optional[str]:
        for selector in [{'data-testid': 'job-experience'}, {'class': re.compile(r'experience|requirements', re.I)}]:
            elem = soup.find(attrs=selector) if 'data-testid' in selector else soup.find(class_=selector['class'])
            if elem:
                t = elem.get_text(separator=" ", strip=True)
                if any(k in t.lower() for k in ['year', 'yr', 'exp']): return t
        return None

    def _headless_get_description_text(self, soup: BeautifulSoup) -> Optional[str]:
        desc_elem = soup.find(attrs={'data-testid': 'job-description'}) or soup.find(id='jobDescription') or soup.find(class_=re.compile(r'description', re.I))
        return desc_elem.get_text(separator=" ", strip=True) if desc_elem else None

    def format_job_for_display(self, job: JobPost) -> dict:
        output = {
            "ID": job.id, "Title": job.title, "Company": job.company_name,
            "Location": job.location.display_location() if job.location else "Remote/Not Specified",
            "Date Posted": job.date_posted or "Not Specified",
            "Is Remote": "Yes" if job.is_remote else "No",
            "Job URL": job.job_url,
            "Apply Type": getattr(job, 'w2_c2c_type', "Default (W2/Contract)") or "Default (W2/Contract)",
            "Employment": getattr(job, 'employment_type', "Not Specified") or "Not Specified",
            "Experience": getattr(job, 'experience', "Not Specified") or "Not Specified",
            "Skills": ", ".join(job.skills) if hasattr(job, 'skills') and job.skills else "Not Specified",
        }
        if job.compensation:
            comp = job.compensation
            comp_text = f"{comp.currency} ${comp.min_amount:,}"
            if comp.max_amount and comp.max_amount != comp.min_amount:
                comp_text += f" - ${comp.max_amount:,}"
            if comp.interval: comp_text += f" {comp.interval.value}"
            output["Compensation"] = comp_text
        output["Description"] = job.description or ""
        return output

    def format_jobs_for_json(self, jobs: list[JobPost]) -> list[dict]:
        jobs_data = []
        for job in jobs:
            job_dict = job.model_dump()
            if job.location:
                country_value = job.location.country if isinstance(job.location.country, str) else job.location.country.value[0]
                job_dict['location'] = {'city': job.location.city, 'state': job.location.state, 'country': country_value, 'display': job.location.display_location()}
            if job.job_type: job_dict['job_type'] = [jt.value[0] for jt in job.job_type]
            for attr in ['employment_type', 'experience', 'skills', 'w2_c2c_type']:
                if hasattr(job, attr) and getattr(job, attr): job_dict[attr] = getattr(job, attr)
            if job.compensation:
                comp = job.compensation
                job_dict['compensation'] = {'interval': comp.interval.value if comp.interval else None, 'min_amount': comp.min_amount, 'max_amount': comp.max_amount, 'currency': comp.currency}
            jobs_data.append(job_dict)
        return jobs_data

    def get_summary_statistics(self, jobs: list[JobPost]) -> dict:
        if not jobs: return {"total_jobs": 0, "jobs_with_external_apply": 0, "jobs_with_external_apply_percent": 0.0, "remote_jobs": 0, "remote_jobs_percent": 0.0, "jobs_with_skills": 0, "jobs_with_skills_percent": 0.0, "jobs_with_salary": 0, "jobs_with_salary_percent": 0.0, "jobs_with_experience": 0, "jobs_with_experience_percent": 0.0, "jobs_with_employment_type": 0, "jobs_with_employment_type_percent": 0.0}
        total = len(jobs)
        ext = sum(1 for j in jobs if j.job_url_direct)
        rem = sum(1 for j in jobs if j.is_remote)
        skl = sum(1 for j in jobs if hasattr(j, 'skills') and j.skills)
        sal = sum(1 for j in jobs if j.compensation)
        exp = sum(1 for j in jobs if hasattr(j, 'experience') and j.experience)
        emt = sum(1 for j in jobs if hasattr(j, 'employment_type') and j.employment_type)
        return {"total_jobs": total, "jobs_with_external_apply": ext, "jobs_with_external_apply_percent": ext/total*100, "remote_jobs": rem, "remote_jobs_percent": rem/total*100, "jobs_with_skills": skl, "jobs_with_skills_percent": skl/total*100, "jobs_with_salary": sal, "jobs_with_salary_percent": sal/total*100, "jobs_with_experience": exp, "jobs_with_experience_percent": exp/total*100, "jobs_with_employment_type": emt, "jobs_with_employment_type_percent": emt/total*100}

    def format_external_apply_urls(self, jobs: list[JobPost], title: str = "External Apply URLs") -> str:
        lines = [f"{title}\n", "=" * 80 + "\n\n"]
        for idx, job in enumerate(jobs, 1):
            if job.job_url_direct: lines.append(f"{idx}. {job.title} - {job.company_name}\n   External URL: {job.job_url_direct}\n   Dice URL: {job.job_url}\n\n")
            else: lines.append(f"{idx}. {job.title} - {job.company_name}\n   (No external apply URL - apply through Dice: {job.job_url})\n\n")
        return "".join(lines)

    def _format_salary_for_csv(self, compensation: Optional[util.Compensation]) -> str:
        if not compensation: return ""
        comp_text = f"{compensation.currency} "
        if compensation.interval == util.CompensationInterval.HOURLY:
            comp_text += f"${compensation.min_amount:.2f}"
            if compensation.max_amount and compensation.max_amount != compensation.min_amount: comp_text += f" - ${compensation.max_amount:.2f}"
        else:
            comp_text += f"${int(compensation.min_amount):,}"
            if compensation.max_amount and compensation.max_amount != compensation.min_amount: comp_text += f" - ${int(compensation.max_amount):,}"
        if compensation.interval: comp_text += f" {compensation.interval.value}"
        return comp_text

    def _format_job_type_for_csv(self, job: JobPost) -> str:
        return getattr(job, 'w2_c2c_type', '') or ''

    def save_to_csv(self, jobs: list[JobPost], filename: str = "dice_jobs.csv") -> str:
        import csv
        if not jobs: log.warning("No jobs to save to CSV"); return filename
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Job ID', 'Title', 'Company', 'Location City', 'Location State', 'Location Country', 'Is Remote', 'Job Type', 'Employment Type', 'Date Posted', 'Salary', 'Experience', 'Job URL (Dice)', 'External Apply URL', 'Skills', 'Description (Full)']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for job in jobs:
                writer.writerow({
                    'Job ID': job.id, 'Title': job.title, 'Company': job.company_name,
                    'Location City': job.location.city if job.location else '',
                    'Location State': job.location.state if job.location else '',
                    'Location Country': job.location.country if job.location else '',
                    'Is Remote': 'Yes' if job.is_remote else 'No',
                    'Job Type': self._format_job_type_for_csv(job),
                    'Employment Type': getattr(job, 'employment_type', '') or '',
                    'Date Posted': job.date_posted or '',
                    'Job URL (Dice)': job.job_url,
                    'External Apply URL': job.job_url_direct or '',
                    'Salary': self._format_salary_for_csv(job.compensation),
                    'Experience': getattr(job, 'experience', '') or '',
                    'Skills': ', '.join(job.skills) if hasattr(job, 'skills') and job.skills else '',
                    'Description (Full)': job.description or '',
                })
        log.info(f"Saved {len(jobs)} jobs to {filename}")
        return filename

    def save_results(self, jobs: list[JobPost], output_prefix: str = "dice_jobs") -> dict[str, str]:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file, csv_w2_file, csv_c2c_file, txt_file = f"{output_prefix}_{timestamp}.json", f"dice_w2_{timestamp}.csv", f"dice_c2c_{timestamp}.csv", f"{output_prefix}_external_apply_urls_{timestamp}.txt"
        with open(json_file, 'w', encoding='utf-8') as f: json.dump(self.format_jobs_for_json(jobs), f, indent=2, default=str, ensure_ascii=False)
        w2_jobs, c2c_jobs = [j for j in jobs if getattr(j, 'w2_c2c_type', '') == 'W2'], [j for j in jobs if getattr(j, 'w2_c2c_type', '') == 'C2C']
        self.save_to_csv(w2_jobs, csv_w2_file); self.save_to_csv(c2c_jobs, csv_c2c_file)
        log.info(f"Split {len(jobs)} jobs: {len(w2_jobs)} W2/Contract → {csv_w2_file}, {len(c2c_jobs)} pure C2C → {csv_c2c_file}")
        with open(txt_file, 'w', encoding='utf-8') as f: f.write(self.format_external_apply_urls(jobs, title=f"External Apply URLs ({len(jobs)} jobs)"))
        return {'json_file': json_file, 'csv_w2_file': csv_w2_file, 'csv_c2c_file': csv_c2c_file, 'txt_file': txt_file}

    def display_results(self, jobs: list[JobPost], search_term: str = None) -> None:
        if not jobs: print("No jobs found matching the criteria."); return
        for idx, job in enumerate(jobs, 1):
            print(f"\n{'='*80}\nJob #{idx}\n{'='*80}")
            for key, value in self.format_job_for_display(job).items():
                if key != "Description": print(f"{key:30}: {value}")
            if job.job_url_direct: print(f"\n{'='*40}\nEXTERNAL APPLY URL:\n{job.job_url_direct}\n{'='*40}")
            else: print(f"\n[NOTE] Apply through Dice: {job.job_url}")
        stats = self.get_summary_statistics(jobs)
        print(f"\n\n{'='*80}\nSUMMARY STATISTICS\n{'='*80}")
        print(f"Total Jobs Found: {stats['total_jobs']}\nJobs with External Apply URL: {stats['jobs_with_external_apply']} ({stats['jobs_with_external_apply_percent']:.1f}%)\nRemote Jobs: {stats['remote_jobs']} ({stats['remote_jobs_percent']:.1f}%)\nJobs with Skills Listed: {stats['jobs_with_skills']} ({stats['jobs_with_skills_percent']:.1f}%)\nJobs with Salary Info: {stats['jobs_with_salary']} ({stats['jobs_with_salary_percent']:.1f}%)\nJobs with Experience Info: {stats['jobs_with_experience']} ({stats['jobs_with_experience_percent']:.1f}%)\nJobs with Employment Type: {stats['jobs_with_employment_type']} ({stats['jobs_with_employment_type_percent']:.1f}%)")

def run_dice_scraper():
    """Main runner function integrated from run_dice.py."""
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if not supabase_url or not supabase_key or "your-" in supabase_url:
        print("⚠ ERROR: Set SUPABASE_URL and SUPABASE_KEY in your .env file")
        return

    supabase = create_client(supabase_url, supabase_key)
    print(f"✓ Connected to Supabase: {supabase_url}")
    print(f"\nInitializing Dice scraper for {len(SEARCH_TERMS)} search domains...")
    
    scraper = Dice()
    total_inserted = 0
    total_scraped = 0
    seen_job_ids = set()

    for idx, term in enumerate(SEARCH_TERMS, 1):
        print(f"\n{'='*80}")
        print(f"[{idx}/{len(SEARCH_TERMS)}] Scraping: '{term}'")
        print(f"{'='*80}")

        try:
            scraper_input = ScraperInput(
                site_type=[Site.DICE],
                search_term=term,
                location="United States",
                results_wanted=100,
                hours_old=24,
                country=Country.USA
            )

            job_response = scraper.scrape(scraper_input)
            new_jobs = []
            for job in job_response.jobs:
                if job.id not in seen_job_ids:
                    seen_job_ids.add(job.id)
                    new_jobs.append(job)

            total_scraped += len(new_jobs)

            if new_jobs:
                inserted = util.insert_to_supabase(supabase, new_jobs, term)
                total_inserted += inserted
                print(f"  → {len(job_response.jobs)} found, {len(new_jobs)} new, {inserted} inserted to Supabase")
            else:
                print(f"  → {len(job_response.jobs)} found, 0 new (all duplicates)")

        except Exception as e:
            print(f"  → Error scraping '{term}': {e}")
            traceback.print_exc()
            continue

    print(f"\n\n{'='*80}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*80}")
    print(f"Total unique jobs scraped: {total_scraped}")
    print(f"Total inserted to Supabase: {total_inserted}")
