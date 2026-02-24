from __future__ import annotations

import math
import random
import time
import threading
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse, urlunparse, unquote
from urllib.parse import parse_qs
import requests

import regex as re
from bs4 import BeautifulSoup
from bs4.element import Tag

from jobspy_enhanced.exception import LinkedInException
from jobspy_enhanced.linkedin.constant import headers
from jobspy_enhanced.linkedin.util import (
    is_job_remote,
    job_type_code,
    parse_job_type,
    parse_job_level,
    parse_company_industry
)
from jobspy_enhanced.model import (
    JobPost,
    Location,
    JobResponse,
    Country,
    Compensation,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)
from jobspy_enhanced.util import (
    extract_emails_from_text,
    currency_parser,
    markdown_converter,
    plain_converter,
    create_session,
    remove_attributes,
    create_logger,
)

log = create_logger("LinkedIn")


class LinkedIn(Scraper):
    base_url = "https://www.linkedin.com"
    base_delay = 2.0  # Base delay between requests
    jobs_per_page = 25  # Standard page size
    max_retries = 5  # Increased number of retries
    backoff_multiplier = 1.5  # Increased backoff multiplier
    
    # High volume scraping settings
    min_delay = 2.0  # Increased minimum delay
    max_delay = 5.0  # Increased maximum delay
    batch_size = 50  # Smaller batch size to avoid detection
    max_consecutive_errors = 3  # Max errors before increasing delay
    error_backoff_factor = 2.0  # Increased error backoff
    batch_cooldown = 30  # Cooldown period between batches in seconds
    
    # Thread pool settings
    min_worker_threads = 4  # Minimum number of worker threads
    max_worker_threads = 8  # Maximum number of worker threads
    max_jobs_per_thread = 10  # Maximum number of jobs per thread
    
    def __init__(self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None):
        """
        Initializes LinkedInScraper with optimized settings for high throughput
        """
        super().__init__(Site.LINKEDIN, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=3,
            clear_cookies=True,
        )
        self.session.headers.update(headers)
        self.scraper_input = None
        self.country = "worldwide"
        self.job_url_direct_regex = re.compile(r'(?<=\?url=)[^"]+')
        self._current_proxy: str | None = None
        self._ext_apply_calls: list[float] = []
        self._success_rate = 1.0
        
        # Thread-safe components
        self._results_lock = Lock()
        self._rate_limit_lock = Lock()
        self._job_queue = Queue()
        # dynamic proxy state
        self._current_proxy: str | None = None
        # externalApply rate limiter timestamps
        self._ext_apply_calls: list[float] = []

    def _prepare_base_params(self, scraper_input: ScraperInput) -> dict:
        """Prepare base parameters for LinkedIn API requests with enhanced parameter handling"""
        params = {
            "keywords": scraper_input.search_term,
            "location": scraper_input.location,
            "distance": "100",  # Increased search radius
            "f_TPR": f"r{scraper_input.hours_old * 3600}" if scraper_input.hours_old else None,
            "position": 1,
            "geoId": "103644278",  # United States
            "pageSize": str(self.jobs_per_page),
            "sortBy": "DD",  # Sort by date for more complete results
            "f_WT": "2" if scraper_input.is_remote else None,
            "f_JT": "F,C,I",  # Include full-time, contract, internship
            "f_E": scraper_input.experience_level if scraper_input.experience_level else None,
            "start": 0,
            "refresh": True,
            "f_AL": "true",  # Include Easy Apply jobs
        }
        
        # Handle additional filters
        if scraper_input.job_type:
            params["f_JT"] = job_type_code(scraper_input.job_type)
        if scraper_input.easy_apply is not None:
            params["f_AL"] = "true" if scraper_input.easy_apply else "false"
        if hasattr(scraper_input, 'linkedin_company_ids') and scraper_input.linkedin_company_ids:
            params["f_C"] = ",".join(map(str, scraper_input.linkedin_company_ids))
            
        # Remove None values and ensure clean params
        return {k: v for k, v in params.items() if v is not None}

    def _fetch_job_details(self, job: JobPost) -> Optional[JobPost]:
        """
        Fetch detailed information for a job posting with retries and rate limiting
        """
        if not hasattr(job, 'job_id'):
            return None
            
        with self._rate_limit_lock:
            time.sleep(self.delay * 0.5)  # Shorter delay for details
            
        try:
            # Add details fetching logic here
            # You can move the existing detail fetching code here
            return job
        except Exception as e:
            log.warning(f"Failed to fetch details for job {job.job_id}: {str(e)}")
            return None

    def _rotate_proxy(self) -> None:
        """Simple proxy rotation with fallback."""
        if not self.proxies:
            self.session.proxies = None
            return

        try:
            # If specific proxies were provided, use those
            if isinstance(self.proxies, list) and self.proxies:
                proxy = self.proxies[0]  # Use first available proxy
                self.session.proxies = {"http": proxy, "https": proxy}
                log.info("Using provided proxy")
                return
            elif isinstance(self.proxies, str):
                self.session.proxies = {"http": self.proxies, "https": self.proxies}
                log.info("Using provided proxy")
                return
        except Exception as e:
            log.warning(f"Error setting proxy: {e}")
            
        # If no valid proxies, proceed without
        self.session.proxies = None
        log.info("Proceeding without proxy")

    def _handle_rate_limit(self, attempt: int, max_retries: int = None) -> bool:
        """
        Handle rate limiting with exponential backoff and jitter
        Returns True if should retry, False if max retries exceeded
        """
        if max_retries is None:
            max_retries = self.max_retries
            
        if attempt >= max_retries:
            log.error(f"Max retries ({max_retries}) exceeded for rate limiting")
            return False
        
        # Adjust delays based on rate limit mode
        rate_limit_mode = getattr(self.scraper_input, 'rate_limit_mode', 'normal') if self.scraper_input else 'normal'
        
        if rate_limit_mode == "fast":
            # Ultra-fast mode for maximum speed
            base_delay = self.delay * 0.8 * (self.backoff_multiplier ** attempt)
            jitter = random.uniform(0.9, 1.1)  # Minimal jitter
        elif rate_limit_mode == "aggressive":
            # Fast but smart delays for large requests
            base_delay = self.delay * 1.2 * (self.backoff_multiplier ** attempt)
            jitter = random.uniform(0.8, 1.2)  # Less jitter for speed
        elif rate_limit_mode == "conservative":
            # Conservative delays
            base_delay = self.delay * 1.5 * (self.backoff_multiplier ** attempt)
            jitter = random.uniform(0.8, 1.2)
        else:  # normal
            base_delay = self.delay * (self.backoff_multiplier ** attempt)
            jitter = random.uniform(0.7, 1.3)
            
        delay = base_delay * jitter
        
        # Much lower caps for faster processing
        if rate_limit_mode == "fast":
            max_delay = 10  # Very short delays for fast mode
        elif rate_limit_mode == "aggressive":
            max_delay = 20
        else:
            max_delay = 30
        delay = min(delay, max_delay)
        
        log.warning(f"Rate limited (429). Waiting {delay:.1f} seconds before retry {attempt + 1}/{max_retries} (mode: {rate_limit_mode})")
        print(f"⏳ Rate limited! Waiting {delay:.1f} seconds before retry {attempt + 1}/{max_retries} (mode: {rate_limit_mode})")
        
        time.sleep(delay)
        return True

    def _make_request_with_retry(self, url: str, params: dict, timeout: int = 10, max_retries: Optional[int] = None) -> Optional[requests.Response]:
        """
        Make a request with automatic retry for rate limiting
        """
        retries = self.max_retries if max_retries is None else max_retries
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                
                if response.status_code == 429:
                    if not self._handle_rate_limit(attempt, max_retries=retries):
                        return None
                    continue
                elif response.status_code not in range(200, 400):
                    log.error(f"LinkedIn response status code {response.status_code}: {response.text}")
                    if attempt < retries:
                        time.sleep(random.uniform(2, 5))  # Short delay for other errors
                        continue
                    return None
                else:
                    return response
                    
            except Exception as e:
                log.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt < retries:
                    time.sleep(random.uniform(1, 3))
                    continue
                else:
                    log.error(f"All request attempts failed: {str(e)}")
                    return None
        
        return None

    def _throttle(self, bucket: list[float], max_per_sec: int) -> None:
        """Simple token-bucket-like throttle: sleep if over max_per_sec in last 1s."""
        now = time.time()
        # keep only last 1s
        while bucket and now - bucket[0] > 1.0:
            bucket.pop(0)
        if len(bucket) >= max_per_sec:
            time.sleep(0.08)
        bucket.append(time.time())

    def _fetch_page_cards(self, base_params: dict, start: int) -> list[tuple[Tag, str]]:
        """Optimized fetch of job cards with enhanced reliability"""
        # Prepare request parameters
        params = dict(base_params)
        params.update({
            "start": start,
            "pageNum": start // self.jobs_per_page,
            "origin": "JOB_SEARCH_RESULTS",
            "refresh": True,
        })
        
        # Update session headers for better reliability
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'www.linkedin.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0'
        })
        
        # Initialize state
        attempts = 2
        out: list[tuple[Tag, str]] = []
        
        for att in range(attempts):
            try:
                # Make request with highly optimized settings
                resp = self._make_request_with_retry(
                    f"{self.base_url}/jobs-guest/jobs/api/seeMoreJobPostings/search?",
                    params=params,
                    timeout=8,  # Balanced timeout
                    max_retries=2
                )
                
                if not resp:
                    log.warning(f"Request failed on attempt {att + 1}")
                    if att < attempts - 1:
                        time.sleep(0.5)  # Shorter pause before retry
                    continue
                    
                # Parse response with optimized BeautifulSoup settings
                soup = BeautifulSoup(resp.text, "html.parser", parse_only=None)
                
                # Find all job cards with multiple selectors
                job_cards = soup.find_all("div", class_="base-card")
                if not job_cards:
                    job_cards = soup.find_all("div", {"class": lambda x: x and any(c in x for c in ['job-search-card', 'base-card'])})
                
                log.info(f"Found {len(job_cards)} job cards")
                
                # Process job cards
                page_ids = set()
                for job_card in job_cards:
                    try:
                        # Multiple selectors for job links
                        href_tag = (
                            job_card.find("a", class_="base-card__full-link") or
                            job_card.find("a", class_="job-card-container__link") or
                            job_card.find("a", {"class": lambda x: x and ('link' in x or 'url' in x)})
                        )
                        
                        if href_tag and (href := href_tag.get("href")):
                            job_id = href.split("?")[0].split("-")[-1]
                            if job_id not in page_ids:
                                page_ids.add(job_id)
                                out.append((job_card, job_id))
                    except Exception as e:
                        log.warning(f"Error processing job card: {str(e)}")
                        continue
                
                # Return all found jobs for this page
                if out:
                    return out
                    
                # Retry if insufficient results
                if att < attempts - 1:
                    log.warning("Insufficient results, retrying...")
                    time.sleep(1)
                
            except Exception as e:
                log.warning(f"Error fetching/parsing page: {str(e)}")
                if att < attempts - 1:
                    time.sleep(1)
                continue
        
        return out  # Return all found jobs

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """Scrapes job listings from LinkedIn with high-volume optimizations"""
        self.scraper_input = scraper_input
        jobs = []
        seen_ids = set()
        current_page = scraper_input.offset or 0
        consecutive_errors = 0
        current_delay = self.min_delay
        
        start_time = time.time()
        log.info(f"Starting job scraping for {scraper_input.results_wanted} jobs")
        
        # Prepare request parameters
        base_params = self._prepare_base_params(scraper_input)
        
        # Calculate optimal batch size and number of batches
        batch_size = min(self.batch_size, scraper_input.results_wanted)
        jobs_remaining = scraper_input.results_wanted
        current_batch = 0
        jobs_in_batch = 0
        
        while jobs_remaining > 0:
            try:
                # Check if we need a batch cooldown
                if jobs_in_batch >= batch_size:
                    log.info(f"Batch {current_batch} complete. Cooling down for {self.batch_cooldown} seconds...")
                    time.sleep(self.batch_cooldown)
                    current_batch += 1
                    jobs_in_batch = 0
                    current_delay = self.min_delay  # Reset delay after cooldown
                
                # Add random jitter to delay
                jitter = random.uniform(0, 1.0)
                effective_delay = current_delay + jitter
                
                # Get start position for current page
                start = current_page * self.jobs_per_page
                
                # Fetch job cards from the page with rate limiting
                with self._rate_limit_lock:
                    page_jobs = self._fetch_page_cards(base_params, start)
                    time.sleep(effective_delay)  # Apply rate limiting with jitter
                
                # Handle rate limiting response
                if not page_jobs:
                    consecutive_errors += 1
                    if consecutive_errors >= self.max_consecutive_errors:
                        log.info(f"Hit rate limit, increasing delay to {current_delay * self.error_backoff_factor:.1f}s")
                        current_delay = min(current_delay * self.error_backoff_factor, self.max_delay)
                        consecutive_errors = 0
                        time.sleep(self.batch_cooldown)  # Take a longer break
                        continue
                    else:
                        log.info("No jobs found on this page, trying next page")
                
                log.info(f"Found {len(page_jobs)} job cards on page {current_page + 1}")
                log.info(f"Progress: {len(jobs)}/{scraper_input.results_wanted} jobs collected")
                
                # Process jobs in current batch using thread pool
                new_jobs = 0
                if jobs_remaining <= 0:
                    break
                
                # Filter out jobs we've already seen
                new_job_cards = [(card, jid) for card, jid in page_jobs if jid not in seen_ids]
                
                if new_job_cards:
                    # Calculate optimal number of threads
                    batch_size = len(new_job_cards)
                    num_threads = min(
                        max(self.min_worker_threads, batch_size // self.max_jobs_per_thread),
                        self.max_worker_threads
                    )
                    
                    # Process jobs in parallel
                    with ThreadPoolExecutor(max_workers=num_threads) as executor:
                        # Submit jobs to thread pool
                        future_to_job = {
                            executor.submit(
                                self._process_job, 
                                job_card, 
                                job_id, 
                                scraper_input.linkedin_fetch_description
                            ): (job_card, job_id) 
                            for job_card, job_id in new_job_cards
                        }
                        
                        # Process completed jobs as they finish
                        for future in as_completed(future_to_job):
                            job_card, job_id = future_to_job[future]
                            try:
                                job = future.result()
                                if job:
                                    seen_ids.add(job_id)
                                    jobs.append(job)
                                    new_jobs += 1
                                    jobs_remaining -= 1
                                    
                                    # Reset error counter on success
                                    consecutive_errors = 0
                                    
                                    # Gradually decrease delay on sustained success
                                    if current_delay > self.min_delay:
                                        current_delay = max(self.min_delay, 
                                                         current_delay / self.backoff_multiplier)
                            except Exception as e:
                                log.warning(f"Error processing job {job_id}: {str(e)}")
                                consecutive_errors += 1
                                
                                # Increase delay if too many consecutive errors
                                if consecutive_errors >= self.max_consecutive_errors:
                                    current_delay = min(self.max_delay,
                                                     current_delay * self.error_backoff_factor)
                                    log.warning(f"Increasing delay to {current_delay}s due to errors")
                                    consecutive_errors = 0  # Reset counter
                
                # If we got no new jobs from this page, increase delay
                if new_jobs == 0:
                    current_delay = min(self.max_delay, current_delay * self.backoff_multiplier)
                    log.info(f"No new jobs found, increasing delay to {current_delay}s")
                
                # Move to next page
                current_page += 1
                
                # Log timing info every 100 jobs
                if len(jobs) % 100 == 0 and len(jobs) > 0:
                    elapsed = time.time() - start_time
                    rate = len(jobs) / elapsed
                    eta = (scraper_input.results_wanted - len(jobs)) / rate if rate > 0 else 0
                    log.info(f"Progress: {len(jobs)}/{scraper_input.results_wanted} jobs")
                    log.info(f"Rate: {rate:.1f} jobs/sec")
                    log.info(f"ETA: {eta/60:.1f} minutes")
                
            except Exception as e:
                log.error(f"Error processing page: {str(e)}")
                consecutive_errors += 1
                if consecutive_errors >= self.max_consecutive_errors:
                    current_delay = min(self.max_delay, current_delay * self.error_backoff_factor)
                    consecutive_errors = 0
                time.sleep(current_delay)  # Add delay after error
            
            # Add delay unless we have enough jobs
            if len(jobs) < scraper_input.results_wanted:
                time.sleep(self.base_delay)
                
        return JobResponse(jobs=jobs)
        # Prepare base parameters
        base_params = self._prepare_base_params(scraper_input)
        
        # Queue for collecting results
        results_queue = Queue()
        
        def process_chunk(chunk_idx: int) -> None:
            """Process a chunk of jobs with optimized fetching and processing"""
            chunk_start = chunk_idx * self.chunk_size
            chunk_jobs = []
            
            try:
                # Progressive rate limiting based on chunk index
                with self._rate_limit_lock:
                    # Increase delay as we process more chunks
                    progressive_delay = self.base_delay * (1 + (chunk_idx // 20) * 0.2)
                    time.sleep(progressive_delay)
                
                # Fetch job cards
                page_jobs = self._fetch_page_cards(base_params, chunk_start)
                
                if not page_jobs:
                    return
                    
                # Extract basic job info from cards
                for job_card, job_id in page_jobs:
                    try:
                        # Get job link
                        href = ""
                        for link_class in ["base-card__full-link", "job-card-container__link"]:
                            if href_tag := job_card.find("a", class_=link_class):
                                href = href_tag.get("href", "")
                                if href:
                                    break
                        
                        if not href:
                            continue
                            
                        # Get title and company
                        # Get title, company and location with fallbacks
                        title_tag = job_card.find("h3", class_="base-search-card__title") or \
                                  job_card.find("h3", class_="job-card-list__title")
                        company_tag = job_card.find("h4", class_="base-search-card__subtitle") or \
                                    job_card.find("span", class_="job-card-container__company-name")
                        location_tag = job_card.find("span", class_="job-search-card__location") or \
                                     job_card.find("div", class_="job-card-container__metadata-wrapper")
                        
                        if title_tag and company_tag:
                            title = title_tag.get_text(strip=True)
                            company = company_tag.get_text(strip=True)
                            location_str = location_tag.get_text(strip=True) if location_tag else None
                            
                            # Parse location into Location object
                            if location_str:
                                if ", " in location_str:
                                    city, state = location_str.split(", ", 1)
                                    state = state.split(" Metropolitan Area")[0]  # Handle special cases
                                    location = Location(city=city, state=state, country=Country.USA)
                                else:
                                    location = Location(state=location_str, country=Country.USA)
                            else:
                                location = None
                            
                            # Create job post
                            job = JobPost(
                                id=job_id,
                                job_url=href,
                                title=title,
                                company_name=company,
                                job_type=None,
                                location=location,
                                has_easy_apply=None,
                                description=None,
                                description_format=DescriptionFormat.HTML
                            )
                            
                            if scraper_input.linkedin_fetch_description:
                                # Fetch details in parallel later
                                job_details = self._get_job_details(job_id)
                                self._process_job_card_with_details(job_card, job_id, job_details)
                            
                            return job
                            
                    except Exception as e:
                        log.warning(f"Error processing job: {str(e)}")
                        return None
                
            except Exception as e:
                log.error(f"Chunk {chunk_idx} failed: {str(e)}")
                return []
                
        self.scraper_input = scraper_input
        
        # Initialize state
        seen_jobs = set()
        jobs_list = []
        
        # Get jobs sequentially
        page = 0
        start = scraper_input.offset or 0
        
        while len(jobs_list) < scraper_input.results_wanted:
            # Get jobs for current page
            start_idx = page * self.jobs_per_page
            page_jobs = self._fetch_page_cards(self._prepare_base_params(scraper_input), start_idx)
            
            if not page_jobs:
                break
                
            # Process job cards
            for job_card, job_id in page_jobs:
                try:
                    # Process job
                    job = self._process_job(job_card, job_id, scraper_input.linkedin_fetch_description)
                    if job and job_id not in seen_jobs:
                        seen_jobs.add(job_id)
                        jobs_list.append(job)
                except Exception as e:
                    log.warning(f"Error processing job: {str(e)}")
            
            # Increment page and add delay
            page += 1
            if len(jobs_list) < scraper_input.results_wanted:
                time.sleep(self.base_delay)
        
        # Return results
        return JobResponse(jobs=jobs_list[:scraper_input.results_wanted])

    def _process_job(
        self, job_card: Tag, job_id: str, full_descr: bool
    ) -> Optional[JobPost]:
        # Note: Easy apply detection is now done at the job page level in _get_job_details
        # This ensures more accurate detection since the information is only available there
        salary_tag = job_card.find("span", class_="job-search-card__salary-info")

        compensation = description = None
        job_url = None
        
        # Extract job URL
        url_tag = job_card.find("a", class_="base-card__full-link") or \
                 job_card.find("a", class_="job-card-container__link") or \
                 job_card.select_one("a[data-tracking-control-name='public_jobs_jserp-result_search-card']")
                 
        if url_tag and (href := url_tag.get("href")):
            job_url = href.split("?")[0]  # Remove query parameters
        
        # Extract basic job info from card
        title_tag = job_card.find("h3", class_="base-search-card__title") or \
                  job_card.find("h3", class_="job-card-list__title")
        company_tag = job_card.find("h4", class_="base-search-card__subtitle") or \
                    job_card.find("span", class_="job-card-container__company-name")
        location_tag = job_card.find("span", class_="job-search-card__location") or \
                     job_card.find("div", class_="job-card-container__metadata-wrapper")
        
        if not (title_tag and company_tag):
            return None
            
        title = title_tag.get_text(strip=True)
        company = company_tag.get_text(strip=True)
        location_str = location_tag.get_text(strip=True) if location_tag else None
        
        # Parse location
        location = None
        if location_str:
            if ", " in location_str:
                city, state = location_str.split(", ", 1)
                if " Metropolitan Area" in state:
                    state = state.split(" Metropolitan Area")[0]
                location = Location(city=city, state=state, country=Country.USA)
            else:
                location = Location(state=location_str, country=Country.USA)

        # Parse salary if available
        if salary_tag:
            salary_text = salary_tag.get_text(separator=" ").strip()
            salary_values = [currency_parser(value) for value in salary_text.split("-")]
            salary_min = salary_values[0]
            salary_max = salary_values[1]
            currency = salary_text[0] if salary_text[0] != "$" else "USD"

            compensation = Compensation(
                min_amount=int(salary_min),
                max_amount=int(salary_max),
                currency=currency,
            )
            
        # Create and return job post
        job = JobPost(
            job_id=job_id,
            job_url=self.base_url + f"/jobs/view/{job_id}",
            title=title,
            company_name=company,
            location=location,
            has_easy_apply=None,  # Will be determined if full description is fetched
            description=None,  # Will be fetched if requested
            description_format=DescriptionFormat.HTML,
            compensation=compensation
        )
        return job
        location = self._get_location(metadata_card)

        # Try multiple selectors for date posted information
        date_posted = None
        
        if metadata_card:
            # Try different possible selectors for date information
            datetime_tag = None
            
            # First try: job-search-card__listdate class
            datetime_tag = metadata_card.find("time", class_="job-search-card__listdate")
            
            # Second try: any time tag with datetime attribute
            if not datetime_tag:
                datetime_tag = metadata_card.find("time", attrs={"datetime": True})
            
            # Third try: look for relative time text and parse it
            if not datetime_tag:
                time_elements = metadata_card.find_all("time")
                for time_elem in time_elements:
                    if time_elem.get("datetime"):
                        datetime_tag = time_elem
                        break
            
            # Fourth try: look for any element with datetime attribute
            if not datetime_tag:
                datetime_tag = metadata_card.find(attrs={"datetime": True})
            
            # Parse the datetime if found
            if datetime_tag and "datetime" in datetime_tag.attrs:
                datetime_str = datetime_tag["datetime"]
                try:
                    # Try different datetime formats
                    for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ"]:
                        try:
                            date_posted = datetime.strptime(datetime_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except:
                    date_posted = None
            
            # If still no date found, try to parse relative time text
            if not date_posted:
                date_posted = self._parse_relative_date(metadata_card)
        # Always try to get job details if requested
        if full_descr:
            log.info(f"Fetching details for job {job_id}...")
            try:
                job_details = self._get_job_details(job_id)
                if not job_details:
                    job_details = {}
                if "description" not in job_details or not job_details["description"]:
                    log.warning(f"No description found for job {job_id}")
                    job_details["description"] = "Description not available"
                log.info(f"Job details fetched successfully")
            except Exception as e:
                log.error(f"Error fetching job details: {str(e)}")
                job_details = {}
        is_remote = is_job_remote(title, description, location)

        return JobPost(
            id=f"li-{job_id}",
            title=title,
            company_name=company,
            company_url=company_url,
            location=location,
            is_remote=is_remote,
            date_posted=date_posted,
            job_url=f"{self.base_url}/jobs/view/{job_id}",
            compensation=compensation,
            job_type=job_details.get("job_type"),
            job_level=job_details.get("job_level", "").lower(),
            company_industry=job_details.get("company_industry"),
            description=job_details.get("description"),
            job_url_direct=job_details.get("job_url_direct"),
            emails=extract_emails_from_text(description),
            company_logo=job_details.get("company_logo"),
            job_function=job_details.get("job_function"),
        )

    def _get_job_details(self, job_id: str) -> dict:
        """
        Retrieves job description and other job details by going to the job page url.
        
        Phase 1: Fetches view-source:https://www.linkedin.com/jobs/view/{job_id} (raw HTML)
        Phase 2: Uses multiple extraction methods with retries
        
        :param job_id: LinkedIn job ID
        :return: dict
        """
        # Phase 1: Initial Page Fetch (view-source style - raw HTML)
        # Adjust retries based on rate_limit_mode
        rate_limit_mode = getattr(self.scraper_input, 'rate_limit_mode', 'normal') if self.scraper_input else 'normal'
        if rate_limit_mode == "fast":
            detail_max_retries = 0  # No retry for fast mode - fail fast
        else:
            detail_max_retries = 1
        job_url_direct_pre: str | None = None
        soup: BeautifulSoup | None = None
        response_text: str = ""
        
        for attempt in range(detail_max_retries + 1):
            try:
                # rotate proxy for each attempt (helps with 429 errors)
                self._rotate_proxy()
                
                # Fetch view-source style (raw HTML) - HTTP GET already returns raw HTML
                # Setting Accept header to ensure we get HTML content
                headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
                response = self.session.get(
                    f"{self.base_url}/jobs/view/{job_id}", 
                    timeout=8,
                    headers=headers
                )
                
                if response.status_code == 429:
                    # Only retry on 429 if we have attempts left and not in fast mode
                    if attempt < detail_max_retries:
                        # Shorter delays for fast mode (if retry allowed)
                        if rate_limit_mode == "fast":
                            time.sleep(random.uniform(0.3, 0.8))
                        elif rate_limit_mode == "aggressive":
                            time.sleep(random.uniform(0.4, 1.0))
                        else:
                            time.sleep(random.uniform(0.5, 1.5))
                        continue
                    # In fast mode, return empty on 429 to fail fast
                    return {}
                elif response.status_code not in range(200, 400):
                    log.warning(f"Job {job_id}: HTTP {response.status_code}")
                    if attempt < detail_max_retries:
                        time.sleep(random.uniform(0.3, 0.8))
                        continue
                    return {}
                
                if "linkedin.com/signup" in response.url:
                    log.warning(f"Job {job_id}: Redirected to signup page")
                    if attempt < detail_max_retries:
                        time.sleep(random.uniform(0.3, 0.8))
                    continue
                    return {}
                
                # Successfully fetched page - parse immediately
                response_text = response.text
                soup = BeautifulSoup(response_text, "html.parser")
                # Extract URLs using multiple methods
                job_url_direct = self._extract_job_url_direct_from_raw(response_text, job_id)
                
                # Try to find apply button if direct URL not found
                if not job_url_direct:
                    apply_button = soup.find('a', {'class': ['apply-button', 'jobs-apply-button']})
                    if apply_button and 'href' in apply_button.attrs:
                        job_url_direct = apply_button['href']
                
                if job_url_direct:
                    log.info(f"Found direct apply URL for job {job_id}")
                
                break
                
            except Exception as e:
                log.warning(f"Job {job_id}: Attempt {attempt + 1} failed - {str(e)}")
                if attempt < detail_max_retries:
                    time.sleep(random.uniform(0.3, 0.8))
                continue
        else:
            log.error(f"Job {job_id}: All attempts failed, returning empty details")
            return {}

        # If we couldn't fetch the page at all, return empty
        if soup is None:
            return {}

        # Extract job description and other details from HTML
        # Try multiple description container classes in order of preference
        description_classes = [
            "show-more-less-html__markup",
            "description__text",
            "jobs-description__content",
            "jobs-description",
            "description"
        ]
        
        description = None
        for class_name in description_classes:
            div_content = soup.find("div", class_=lambda x: x and class_name in x)
            if div_content:
                div_content = remove_attributes(div_content)
                description = div_content.prettify(formatter="html")
                if description:
                    break
        
        # Try to convert description based on format preference
        if description:
            try:
                format = getattr(self, 'description_format', DescriptionFormat.PLAIN)
                if format == DescriptionFormat.MARKDOWN:
                    description = markdown_converter(description)
                elif format == DescriptionFormat.PLAIN:
                    description = plain_converter(description)
            except Exception as e:
                log.warning(f"Error converting description format: {str(e)}")
                # Fall back to plain text if conversion fails
                description = plain_converter(description)
        h3_tag = soup.find(
            "h3", text=lambda text: text and "Job function" in text.strip()
        )

        job_function = None
        if h3_tag:
            job_function_span = h3_tag.find_next(
                "span", class_="description__job-criteria-text"
            )
            if job_function_span:
                job_function = job_function_span.text.strip()

        company_logo = (
            logo_image.get("data-delayed-url")
            if (logo_image := soup.find("img", {"class": "artdeco-entity-image"}))
            else None
        )
        
        # Phase 2: Extraction Methods (In Order)
        # Method 1: Raw HTML extraction using job posting API
        job_url_direct = job_url_direct_pre  # From Phase 1
        if not job_url_direct:
            # Try job posting API for raw HTML extraction
            api_html = self._fetch_job_posting_api(job_id)
            if api_html:
                job_url_direct = self._extract_job_url_direct_from_raw(api_html, job_id)
                if job_url_direct:
                    log.info(f"Job {job_id}: Direct URL found via job posting API")
        
        # Method 2: DOM parsing (fast, no network call)
        if not job_url_direct:
            job_url_direct = self._parse_job_url_direct(soup)
        
        # Method 3: Re-check raw HTML if DOM failed (safety check)
        if not job_url_direct:
            job_url_direct = self._extract_job_url_direct_from_raw(response_text, job_id)
        
        # Method 4: Page retry with proxy rotation (fewer retries for fast mode)
        if not job_url_direct:
            log.warning(f"Job {job_id}: No direct URL found, attempting retry...")
            # Adjust retries based on rate_limit_mode
            rate_limit_mode = getattr(self.scraper_input, 'rate_limit_mode', 'normal') if self.scraper_input else 'normal'
            if rate_limit_mode == "fast":
                max_retries = 1  # Only 1 retry for fast mode (2 total attempts)
            elif rate_limit_mode == "aggressive":
                max_retries = 2
            else:
                max_retries = 3
            for retry_attempt in range(max_retries):
                try:
                    # Shorter delays for fast mode
                    if rate_limit_mode == "fast":
                        time.sleep(random.uniform(0.1, 0.3))
                    elif rate_limit_mode == "aggressive":
                        time.sleep(random.uniform(0.2, 0.4))
                    else:
                        time.sleep(random.uniform(0.3, 0.5))
                    # Rotate proxy before retry
                    self._rotate_proxy()
                    # Fetch the page again (view-source style)
                    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
                    retry_response = self.session.get(
                        f"{self.base_url}/jobs/view/{job_id}", 
                        timeout=8,
                        headers=headers
                    )
                    if retry_response.status_code == 200 and "linkedin.com/signup" not in retry_response.url:
                        retry_soup = BeautifulSoup(retry_response.text, "html.parser")
                        # Try raw HTML extraction first
                        job_url_direct = self._extract_job_url_direct_from_raw(retry_response.text, job_id)
                        # Then try DOM parsing
                        if not job_url_direct:
                            job_url_direct = self._parse_job_url_direct(retry_soup)
                        
                        if job_url_direct:
                            log.info(f"Job {job_id}: Direct URL found on retry attempt {retry_attempt + 1}: {job_url_direct}")
                            break
                        else:
                            log.warning(f"Job {job_id}: Retry attempt {retry_attempt + 1} - still no direct URL found")
                    else:
                        log.warning(f"Job {job_id}: Retry attempt {retry_attempt + 1} - failed to fetch page")
                except Exception as e:
                    log.warning(f"Job {job_id}: Retry attempt {retry_attempt + 1} failed - {str(e)}")
        
        # Method 5: ExternalApply endpoint with view-source fetch (fewer retries for fast mode)
        if not job_url_direct or (job_url_direct and 'linkedin.com' in job_url_direct.lower()):
            # Adjust retries based on rate_limit_mode
            rate_limit_mode = getattr(self.scraper_input, 'rate_limit_mode', 'normal') if self.scraper_input else 'normal'
            if rate_limit_mode == "fast":
                max_ext_retries = 1  # Only 1 retry for fast mode (2 total attempts)
            elif rate_limit_mode == "aggressive":
                max_ext_retries = 2
            else:
                max_ext_retries = 3
            for ext_retry in range(max_ext_retries):
                try:
                    if ext_retry > 0:
                        # Shorter delays for fast mode
                        if rate_limit_mode == "fast":
                            time.sleep(random.uniform(0.3, 0.6))
                        elif rate_limit_mode == "aggressive":
                            time.sleep(random.uniform(0.4, 0.8))
                        else:
                            time.sleep(random.uniform(0.5, 1.0))
                        self._rotate_proxy()
                    
                    # Fetch view-source style for externalApply endpoint
                    ext_url = self._extract_job_url_direct_from_external_apply_with_view_source(job_id)
                    if ext_url and 'linkedin.com' not in ext_url.lower():
                        # Got a valid external URL - use it
                        job_url_direct = ext_url
                        log.info(f"Job {job_id}: Direct URL found via externalApply with view-source (attempt {ext_retry + 1}): {job_url_direct}")
                        break
                except Exception as e:
                    log.warning(f"Job {job_id}: externalApply attempt {ext_retry + 1} failed - {str(e)}")
                    if ext_retry < max_ext_retries - 1:
                        continue
        
        # Detect if this is an easy apply job from the job page
        is_easy_apply = self._is_easy_apply_job_from_page(soup)
        
        # Filter out easy apply jobs if requested
        if self.scraper_input.easy_apply is False and is_easy_apply:
            log.info(f"Job {job_id}: Filtering out easy apply job")
            return None  # Return None to skip this job
        
        return {
            "description": description,
            "job_level": parse_job_level(soup),
            "company_industry": parse_company_industry(soup),
            "job_type": parse_job_type(soup),
            "job_url_direct": job_url_direct,
            "company_logo": company_logo,
            "job_function": job_function,
            "is_easy_apply": is_easy_apply,
        }

    def _process_job_card_with_details(self, job_card: Tag, job_id: str, details: dict) -> Optional[JobPost]:
        """Build a JobPost from a card and a pre-fetched details dict."""
        # Get job URL first
        job_url = None
        url_tag = job_card.find("a", class_="base-card__full-link") or \
                 job_card.find("a", class_="job-card-container__link") or \
                 job_card.select_one("a[data-tracking-control-name='public_jobs_jserp-result_search-card']")
                 
        if url_tag and (href := url_tag.get("href")):
            job_url = href.split("?")[0]  # Remove query parameters
        else:
            job_url = f"https://www.linkedin.com/jobs/view/{job_id}"
            
        title_tag = job_card.find("span", class_="sr-only") or \
                   job_card.find("h3", class_="base-search-card__title")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company_url = (
            urlunparse(urlparse(company_a_tag.get("href"))._replace(query=""))
            if company_a_tag and company_a_tag.has_attr("href")
            else ""
        )
        company = company_a_tag.get_text(strip=True) if company_a_tag else "N/A"

        # Prefer page-derived company fields
        if details.get("company_name"):
            company = details["company_name"]
        if details.get("company_url"):
            company_url = details["company_url"]

        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        location = self._get_location(metadata_card)

        date_posted = None
        if metadata_card:
            datetime_tag = metadata_card.find("time", attrs={"datetime": True})
            if datetime_tag and "datetime" in datetime_tag.attrs:
                datetime_str = datetime_tag["datetime"]
                try:
                    for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ"]:
                        try:
                            date_posted = datetime.strptime(datetime_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except:
                    date_posted = None

        # Filter easy apply if requested
        if details.get("is_easy_apply") and self.scraper_input and self.scraper_input.easy_apply is False:
            return None
            
        # Ensure we have a description
        description = details.get("description")
        if not description:
            log.warning(f"No description found for job {job_id}")
            description = "Description not available"

        # Get job details if we don't have them
        if not details and full_descr:
            details = self._get_job_details(job_id)
        
        # Extract job details
        description = details.get("description") if details else None
        job_url_direct = details.get("job_url_direct") if details else None
        
        # Set default values if needed
        if full_descr and not description:
            description = "Description not available"
            
        is_remote = is_job_remote(title, description, location)

        # Get direct apply URL if available
        apply_url = details.get("job_url_direct") or job_url
        
        return JobPost(
            id=f"li-{job_id}",
            title=title,
            company_name=company,
            company_url=company_url,
            location=location,
            is_remote=is_remote,
            date_posted=date_posted,
            job_url=job_url,  # Use the extracted job URL
            apply_url=apply_url,  # Add direct apply URL
            compensation=None,
            job_type=details.get("job_type"),
            job_level=details.get("job_level", "").lower(),
            company_industry=details.get("company_industry"),
            description=description,
            job_url_direct=apply_url,  # For backward compatibility
            emails=extract_emails_from_text(description),
            company_logo=details.get("company_logo"),
            job_function=details.get("job_function"),
        )

    def _fetch_job_batch_parallel(self, job_cards: list[tuple[Tag, str]], max_workers: int = 8) -> list[JobPost]:
        """Fetch details for a batch of job cards in parallel with jitter and proxy rotation.
        Optimized: Reduced workers when using free proxies to avoid rate limits.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results: list[JobPost] = []

        # Reduce workers if using free proxies (they're slower and cause more 429s)
        # Original design was 16 workers, but free proxies can't handle that load
        if self._current_proxy is not None:  # Using free proxies
            max_workers = min(max_workers, 10)  # Cap at 10 for free proxies
        
        def worker(job_card: Tag, job_id: str) -> Optional[JobPost]:
            try:
                # Minimal jitter to avoid bursts
                time.sleep(random.uniform(0.02, 0.05))
                details = self._get_job_details(job_id)
                if details is None or not details:
                    return None
                return self._process_job_card_with_details(job_card, job_id, details)
            except Exception as e:
                log.debug(f"Worker exception for job {job_id}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {ex.submit(worker, jc, jid): (jc, jid) for jc, jid in job_cards}
            for fut in as_completed(future_map):
                jp = fut.result()
                if jp:
                    results.append(jp)
        return results

    def _get_location(self, metadata_card: Optional[Tag]) -> Location:
        """
        Extracts the location data from the job metadata card.
        :param metadata_card
        :return: location
        """
        location = Location(country=Country.from_string(self.country))
        if metadata_card is not None:
            location_tag = metadata_card.find(
                "span", class_="job-search-card__location"
            )
            location_string = location_tag.text.strip() if location_tag else "N/A"
            parts = location_string.split(", ")
            if len(parts) == 2:
                city, state = parts
                location = Location(
                    city=city,
                    state=state,
                    country=Country.from_string(self.country),
                )
            elif len(parts) == 3:
                city, state, country = parts
                country = Country.from_string(country)
                location = Location(city=city, state=state, country=country)
        return location

    def _parse_job_url_direct(self, soup: BeautifulSoup) -> str | None:
        """
        Gets the job url direct from job page
        :param soup:
        :return: str
        """
        job_url_direct = None
        
        # Method 1: Look for code element with id="applyUrl"
        job_url_direct_content = soup.find("code", id="applyUrl")
        if job_url_direct_content:
            # Handle both regular content and HTML comments
            content = job_url_direct_content.decode_contents().strip()
            
            # If content is empty, just whitespace, or contains only HTML comments, 
            # try to get the raw string including comments
            if not content or content.isspace() or content.startswith('<!--'):
                content = str(job_url_direct_content)
                # Extract content from HTML comments if present
                comment_pattern = re.compile(r'<!--(.*?)-->', re.DOTALL)
                comment_match = comment_pattern.search(content)
                if comment_match:
                    content = comment_match.group(1).strip()
            # Try multiple regex patterns for better URL extraction
            patterns = [
                self.job_url_direct_regex,  # Original pattern: (?<=\?url=)[^"]+
                re.compile(r'(?<=url=)[^"&\s]+'),  # Alternative pattern: (?<=url=)[^"&\s]+
                re.compile(r'"(https?://[^"]+)"'),  # Direct URL in quotes: "(https?://[^"]+)"
                re.compile(r'url=([^"&\s]+)'),  # Pattern for url=encoded_url format
                re.compile(r'https?://[^\s"<>]+'),  # Any HTTP/HTTPS URL
            ]
            
            for pattern in patterns:
                job_url_direct_match = pattern.search(content)
                if job_url_direct_match:
                    # Handle different group patterns
                    if pattern.groups > 0:
                        job_url_direct = job_url_direct_match.group(1)
                    else:
                        job_url_direct = job_url_direct_match.group()
                    
                    job_url_direct = unquote(job_url_direct)
                    
                    # Skip LinkedIn internal URLs
                    if any(x in job_url_direct.lower() for x in ['linkedin.com', 'signup', 'login']):
                        continue
                    
                    # Clean up the URL - remove any trailing parameters that might be LinkedIn-specific
                    if '&urlHash=' in job_url_direct:
                        job_url_direct = job_url_direct.split('&urlHash=')[0]
                    
                    return job_url_direct
        
        # Method 2: Look for script tags containing applyUrl
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string and "applyUrl" in script.string:
                patterns = [
                    self.job_url_direct_regex,  # Original pattern: (?<=\?url=)[^"]+
                    re.compile(r'(?<=url=)[^"&\s]+'),  # Alternative pattern: (?<=url=)[^"&\s]+
                    re.compile(r'"(https?://[^"]+)"'),  # Direct URL in quotes: "(https?://[^"]+)"
                    re.compile(r'url=([^"&\s]+)'),  # Pattern for url=encoded_url format
                    re.compile(r'https?://[^\s"<>]+'),  # Any HTTP/HTTPS URL
                ]
                
                for pattern in patterns:
                    job_url_direct_match = pattern.search(script.string)
                    if job_url_direct_match:
                        # Handle different group patterns
                        if pattern.groups > 0:
                            job_url_direct = job_url_direct_match.group(1)
                        else:
                            job_url_direct = job_url_direct_match.group()
                        
                        job_url_direct = unquote(job_url_direct)
                        
                        # Skip LinkedIn internal URLs
                        if any(x in job_url_direct.lower() for x in ['linkedin.com', 'signup', 'login']):
                            continue
                        
                        # Clean up the URL
                        if '&urlHash=' in job_url_direct:
                            job_url_direct = job_url_direct.split('&urlHash=')[0]
                        
                        return job_url_direct
        
        # Method 3: Look for any element containing applyUrl in data attributes
        elements_with_apply_url = soup.find_all(attrs={"data-apply-url": True})
        if elements_with_apply_url:
            job_url_direct = elements_with_apply_url[0].get("data-apply-url")
            if job_url_direct:
                return job_url_direct
        
        # Method 4: Look for external apply links in the page
        apply_links = soup.find_all("a", href=True)
        for link in apply_links:
            href = link.get("href", "")
            # Check if it's an external apply link (not LinkedIn)
            if (href and 
                not href.startswith("https://www.linkedin.com") and 
                not href.startswith("/legal/") and 
                not href.startswith("/jobs/") and
                not "linkedin.com" in href and
                not "user-agreement" in href and
                not "sign-in" in href and
                not "auth-button" in href and
                "apply" in href.lower()):
                job_url_direct = href
                return job_url_direct

        return job_url_direct

    def _fetch_job_posting_api(self, job_id: str) -> str | None:
        """
        Fetch job details from the LinkedIn job posting API endpoint.
        Returns raw HTML response text if successful, None otherwise.
        
        Endpoint: /jobs-guest/jobs/api/jobPosting/{job_id}
        :param job_id: LinkedIn job ID
        :return: Raw HTML string or None
        """
        try:
            # Try to fetch from job posting API
            api_url = f"{self.base_url}/jobs-guest/jobs/api/jobPosting/{job_id}"
            response = self.session.get(api_url, timeout=8)
            
            if response.status_code == 200:
                # Check if response is HTML (API might return HTML even though it's an API endpoint)
                content_type = response.headers.get('Content-Type', '').lower()
                if 'html' in content_type or response.text:
                    return response.text
            elif response.status_code == 429:
                log.warning(f"Job {job_id}: API rate limited (429)")
            else:
                log.debug(f"Job {job_id}: API returned status {response.status_code}")
        except Exception as e:
            log.debug(f"Job {job_id}: Error fetching API - {str(e)}")
        
        return None

    def _extract_job_url_direct_from_raw(self, raw_html: str, job_id: str) -> str | None:
        """
        Extract job_url_direct by scanning the raw HTML (equivalent to view-source) for
        the applyUrl code block and parsing the url=... value inside the comment.
        """
        try:
            # Prefer exact match with style="display: none"
            block_match = re.search(r'<code\s+id="applyUrl"\s+style="display:\s*none"[\s\S]*?</code>', raw_html, re.IGNORECASE)
            if not block_match:
                # Fallback: any applyUrl code block
                block_match = re.search(r'<code\s+id="applyUrl"[\s\S]*?</code>', raw_html, re.IGNORECASE)
            if not block_match:
                # Last-resort: search for externalApply/{job_id}?url=... directly
                ea = re.search(rf'externalApply/{job_id}\?url=([^&"\s>]+)', raw_html, re.IGNORECASE)
                if ea:
                    return unquote(ea.group(1)).split('&urlHash=')[0]
                return None
            block = block_match.group(0)
            # Pull out the commented string contents if present
            comment_match = re.search(r'<!--\s*"(https?://[^"]+)"\s*-->', block)
            if comment_match:
                commented_url = comment_match.group(1)
                # From the commented externalApply URL, prefer the url= param if present
                # e.g. ...externalApply/{id}?url=<ENCODED>&urlHash=...
                url_param_match = re.search(r'url=([^&\s]+)', commented_url)
                if url_param_match:
                    encoded = url_param_match.group(1)
                    direct = unquote(encoded)
                    # skip linkedin internal
                    # direct target can be any external domain; if it happens to include linkedin.com (rare), still prefer decoding
                    return direct.split('&urlHash=')[0]
                # If no url= param, and the commented_url itself is non-LinkedIn, accept it
                if not re.search(r'linkedin\.com', commented_url, re.IGNORECASE):
                    return commented_url
            # Fallback: search raw for url=... pattern near applyUrl
            url_near_match = re.search(r'applyUrl[\s\S]{0,400}url=([^"&\s>]+)', block, re.IGNORECASE)
            if url_near_match:
                return unquote(url_near_match.group(1)).split('&urlHash=')[0]
        except Exception:
            pass
        return None

    def _extract_job_url_direct_from_external_apply_with_view_source(self, job_id: str) -> str | None:
        """
        Fetch view-source:https://www.linkedin.com/jobs/view/{job_id} and extract direct URL.
        Searches for <code id="applyUrl" style="display: none"> in raw HTML.
        Extracts URL from HTML comments: <!--"externalApply/123?url=ENCODED"-->
        Up to 2 attempts (1 retry) with proxy rotation.
        """
        detail_max_retries = 1
        for attempt in range(detail_max_retries + 1):
            try:
                if attempt > 0:
                    time.sleep(random.uniform(0.3, 0.5))
                    self._rotate_proxy()
                
                # Fetch view-source style (raw HTML)
                headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
                response = self.session.get(
                    f"{self.base_url}/jobs/view/{job_id}", 
                    timeout=8,
                    headers=headers
                )
                
                if response.status_code == 200 and "linkedin.com/signup" not in response.url:
                    # Extract direct URL from raw HTML
                    job_url_direct = self._extract_job_url_direct_from_raw(response.text, job_id)
                    if job_url_direct:
                        return job_url_direct
                elif response.status_code == 429:
                    if attempt < detail_max_retries:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        log.warning(f"Job {job_id}: View-source fetch returned {response.status_code}")
                    
            except Exception as e:
                log.warning(f"Job {job_id}: View-source fetch attempt {attempt + 1} failed - {str(e)}")
                if attempt < detail_max_retries:
                    continue
        
        return None

    def _extract_job_url_direct_from_external_apply(self, job_id: str) -> str | None:
        """
        Call /jobs/view/externalApply/{job_id} and extract the url= param from
        the effective URL or Location header (without requiring a non-LinkedIn redirect).
        Enhanced: Also check if final URL is external (non-LinkedIn) as direct URL.
        """
        try:
            # Global throttle: ≤ 8 calls/sec
            now = time.time()
            self._ext_apply_calls = [t for t in self._ext_apply_calls if now - t < 1.0]
            if len(self._ext_apply_calls) >= 8:
                time.sleep(0.15)
            self._ext_apply_calls.append(time.time())
            url = f"{self.base_url}/jobs/view/externalApply/{job_id}"
            resp = self.session.get(url, timeout=10, allow_redirects=True)
            
            # Check 1: If final URL is external (not LinkedIn), use it directly
            final_url = resp.url
            if final_url and 'linkedin.com' not in final_url.lower():
                # It's an external URL - return it (might have query params, that's fine)
                return final_url
            
            # Check 2: Parse url= param from redirect chain
            candidate = final_url
            # Inspect redirect history for a Location with url= param
            if resp.history:
                for h in resp.history:
                    loc = h.headers.get('Location') or ''
                    if loc and 'url=' in loc:
                        candidate = loc
                        # If Location is external, use it
                        if 'linkedin.com' not in loc.lower():
                            parsed_loc = urlparse(loc)
                            if parsed_loc.scheme and parsed_loc.netloc:
                                return loc.split('?')[0] if '?' in loc else loc
            
            # Check 3: Parse url= param from query string
            qs = urlparse(candidate).query
            url_param = parse_qs(qs).get('url', [None])[0]
            if url_param:
                decoded_url = unquote(url_param)
                # Handle multiple levels of encoding
                while '%' in decoded_url:
                    try:
                        new_decoded = unquote(decoded_url)
                        if new_decoded == decoded_url:
                            break
                        decoded_url = new_decoded
                    except Exception:
                        break
                decoded_url = decoded_url.split('&urlHash=')[0]
                # Verify it's not a LinkedIn URL
                if decoded_url and 'linkedin.com' not in decoded_url.lower():
                    return decoded_url
            
            # Check 4: Check the actual response body for embedded URLs
            try:
                if resp.text:
                    # Look for common URL patterns in response
                    url_patterns = [
                        r'url=([^"&\s<>]+)',  # url= parameter
                        r'"(https?://[^"]+)"',  # Quoted URLs
                        r"'(https?://[^']+)'",  # Single-quoted URLs
                        r'location\.href\s*=\s*["\']([^"\']+)["\']',  # JavaScript redirects
                        r'window\.open\(["\']([^"\']+)["\']',  # Window.open calls
                    ]
                    for pattern in url_patterns:
                        matches = re.finditer(pattern, resp.text, re.IGNORECASE)
                        for match in matches:
                            potential_url = match.group(1) if match.groups() else match.group(0)
                            # Decode if needed
                            if '%' in potential_url:
                                potential_url = unquote(potential_url)
                            # Remove urlHash and other tracking params
                            potential_url = potential_url.split('&urlHash=')[0]
                            # Verify it's external and valid
                            if (potential_url and 
                                potential_url.startswith('http') and 
                                'linkedin.com' not in potential_url.lower() and
                                len(potential_url) > 10):  # Minimum URL length check
                                return potential_url
            except Exception:
                pass
            
            # Check 5: Check redirect history response bodies
            if resp.history:
                for redirect_resp in resp.history:
                    try:
                        if redirect_resp.text:
                            # Look for url= patterns in redirect response
                            url_match = re.search(r'url=([^"&\s<>]+)', redirect_resp.text, re.IGNORECASE)
                            if url_match:
                                potential_url = unquote(url_match.group(1)).split('&urlHash=')[0]
                                if (potential_url and 
                                    'linkedin.com' not in potential_url.lower() and
                                    len(potential_url) > 10):
                                    return potential_url
                    except Exception:
                        continue
                        
        except Exception as e:
            log.debug(f"externalApply extraction error for {job_id}: {e}")
            pass
        return None

    def _is_easy_apply_job_from_page(self, soup: BeautifulSoup) -> bool:
        """
        Detects if a job is an easy apply job by looking for specific indicators in the job page
        :param soup: BeautifulSoup object of the job page
        :return: bool
        """
        # Method 1: Look for explicit easy apply button in the job page
        easy_apply_button = soup.find("button", class_=lambda x: x and "easy-apply" in " ".join(x).lower())
        if easy_apply_button:
            return True
            
        # Method 2: Look for explicit easy apply text in buttons
        buttons = soup.find_all("button")
        for button in buttons:
            text = button.get_text().lower().strip()
            if any(indicator in text for indicator in ["easy apply", "quick apply"]):
                return True
                
        # Method 3: Look for easy apply in data attributes
        if soup.find(attrs={"data-easy-apply": True}):
            return True
            
        # Method 4: Look for easy apply in class names
        if soup.find(class_=lambda x: x and "easy-apply" in " ".join(x).lower()):
            return True
            
        # Method 5: Check if there's an external apply URL - if yes, it's NOT easy apply
        apply_url_element = soup.find("code", id="applyUrl")
        if apply_url_element:
            content = apply_url_element.decode_contents().strip()
            if content and not any(x in content.lower() for x in ['linkedin.com', 'signup', 'login']):
                # Has external URL, so it's NOT easy apply
                return False
        
        # Method 5b: Also check script tags for applyUrl
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string and "applyUrl" in script.string:
                # Look for external URLs in the script
                import re
                url_patterns = [
                    r'"(https?://[^"]+)"',
                    r'url=([^"&\s]+)',
                    r'https?://[^\s"<>]+'
                ]
                for pattern in url_patterns:
                    matches = re.findall(pattern, script.string)
                    for match in matches:
                        if isinstance(match, tuple):
                            match = match[0] if match else ""
                        if match and not any(x in match.lower() for x in ['linkedin.com', 'signup', 'login']):
                            # Found external URL, so it's NOT easy apply
                            return False
        
        # Method 6: Look for external apply links in the page
        apply_links = soup.find_all("a", href=True)
        for link in apply_links:
            href = link.get("href", "")
            # Check if it's an external apply link (not LinkedIn)
            if (href and 
                not href.startswith("https://www.linkedin.com") and 
                not href.startswith("/legal/") and 
                not href.startswith("/jobs/") and
                not "linkedin.com" in href and
                not "user-agreement" in href and
                not "sign-in" in href and
                not "auth-button" in href and
                "apply" in href.lower()):
                # Found external apply link, so it's NOT easy apply
                return False
        
        # If we get here, we couldn't find explicit easy apply indicators
        # and we couldn't find external apply links
        # Be more conservative - only filter out if we have strong evidence it's easy apply
        # Look for additional indicators that suggest it's definitely an easy apply job
        
        # Check for LinkedIn-specific apply patterns that indicate easy apply
        page_text = soup.get_text().lower()
        
        # First check for explicit easy apply indicators
        explicit_easy_apply_indicators = [
            'easy apply',
            'quick apply', 
            'one-click apply',
            'apply with linkedin',
            'linkedin apply'
        ]
        
        if any(indicator in page_text for indicator in explicit_easy_apply_indicators):
            return True
            
        # For sign-in required patterns, be more careful - only filter out if we also
        # can't find any external apply URLs
        signin_indicators = [
            'join or sign in to find your next job',
            'sign in to find your next job',
            'join to apply for',
            'security verification',
            'already on linkedin? sign in'
        ]
        
        has_signin_indicators = any(indicator in page_text for indicator in signin_indicators)
        
        if has_signin_indicators:
            # Look for external apply URLs more thoroughly before deciding
            external_urls_found = False
            
            # Import re if not already imported
            import re
            
            # First, check if we already found external apply links in the earlier methods
            # (This is the most reliable indicator)
            apply_links = soup.find_all("a", href=True)
            for link in apply_links:
                href = link.get("href", "")
                if (href and
                    not href.startswith("https://www.linkedin.com") and
                    not href.startswith("/legal/") and
                    not href.startswith("/jobs/") and
                    not "linkedin.com" in href and
                    not "user-agreement" in href and
                    not "sign-in" in href and
                    not "auth-button" in href and
                    "apply" in href.lower()):
                    external_urls_found = True
                    break
            
            # If no external apply links found, check for any external URLs that might be apply URLs
            if not external_urls_found:
                # Check for external URLs in the page
                external_url_pattern = r'https://(?!www\.linkedin\.com)[^"\s<>]+'
                external_urls = re.findall(external_url_pattern, str(soup))
                
                # Filter out common non-apply URLs (be more specific to avoid filtering legitimate job URLs)
                non_apply_patterns = [
                    # LinkedIn non-job URLs
                    r'linkedin\.com/in/',
                    r'linkedin\.com/feed/',
                    r'linkedin\.com/messaging/',
                    r'linkedin\.com/notifications/',
                    r'media\.licdn\.com',
                    r'static\.licdn\.com',
                    r'cdn\.linkedin\.com',
                    
                    # Social media non-job URLs (but allow careers/jobs pages)
                    r'facebook\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                    r'twitter\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                    r'youtube\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                    r'instagram\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                    
                    # File extensions (definitely not job URLs)
                    r'\.css$',
                    r'\.js$',
                    r'\.png$',
                    r'\.jpg$',
                    r'\.jpeg$',
                    r'\.gif$',
                    r'\.svg$',
                    r'\.ico$',
                    r'\.pdf$',
                    r'\.zip$',
                    r'\.mp4$',
                    r'\.mp3$',
                    
                    # Common non-job page patterns
                    r'/legal/',
                    r'/privacy',
                    r'/terms',
                    r'/about$',
                    r'/contact$',
                    r'/news$',
                    r'/blog$',
                    r'/press$',
                    r'/investors$',
                    r'/help$',
                    r'/support$'
                ]
                
                for url in external_urls:
                    # Skip if it matches non-apply patterns
                    if any(re.search(pattern, url, re.IGNORECASE) for pattern in non_apply_patterns):
                        continue
                    
                    # If it's an external URL that doesn't match non-apply patterns,
                    # it could be an apply URL
                    external_urls_found = True
                    break
            
            # Only filter out if we have sign-in indicators AND no external URLs found
            if not external_urls_found:
                return True
            
        # Method 8: NEW - Check for jobs with Apply buttons but no external apply URLs
        # This is a strong indicator of Easy Apply jobs
        import re
        apply_buttons = soup.find_all(['button', 'a'], string=re.compile(r'apply', re.I))
        if apply_buttons and not apply_url_element:
            # If there are apply buttons but no applyUrl element, it's likely Easy Apply
            # unless we can find external apply URLs elsewhere
            external_urls_found = False
            
            # Check for any external URLs that might be apply URLs
            external_url_pattern = r'https://(?!www\.linkedin\.com)[^"\s<>]+'
            external_urls = re.findall(external_url_pattern, str(soup))

            # Filter out common non-apply URLs
            non_apply_patterns = [
                # LinkedIn non-job URLs
                r'linkedin\.com/in/',
                r'linkedin\.com/feed/',
                r'linkedin\.com/messaging/',
                r'linkedin\.com/notifications/',
                r'media\.licdn\.com',
                r'static\.licdn\.com',
                r'cdn\.linkedin\.com',
                
                # Social media non-job URLs (but allow careers/jobs pages)
                r'facebook\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                r'twitter\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                r'youtube\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                r'instagram\.com/(?!.*careers?)(?!.*jobs?)(?!.*work)',
                
                # File extensions (definitely not job URLs)
                r'\.css$',
                r'\.js$',
                r'\.png$',
                r'\.jpg$',
                r'\.jpeg$',
                r'\.gif$',
                r'\.svg$',
                r'\.ico$',
                r'\.pdf$',
                r'\.zip$',
                r'\.mp4$',
                r'\.mp3$',
                
                # Common non-job page patterns
                r'/legal/',
                r'/privacy',
                r'/terms',
                r'/about$',
                r'/contact$',
                r'/news$',
                r'/blog$',
                r'/press$',
                r'/investors$',
                r'/help$',
                r'/support$'
            ]
            
            for url in external_urls:
                # Skip if it matches non-apply patterns
                if any(re.search(pattern, url, re.IGNORECASE) for pattern in non_apply_patterns):
                    continue
                external_urls_found = True
                break

            # If no external URLs found, it's likely Easy Apply
            if not external_urls_found:
                return True
            
        # If no clear indicators either way, be conservative and don't filter
        return False

    def _is_easy_apply_job(self, job_card: Tag) -> bool:
        """
        Legacy method - kept for backward compatibility but no longer used for filtering
        Detects if a job is an easy apply job by looking for specific indicators in the job card
        :param job_card: BeautifulSoup element containing job card
        :return: bool
        """
        # This method is no longer used for filtering since easy apply detection
        # is now done at the job page level for better accuracy
        return False

    def _parse_relative_date(self, metadata_card) -> Optional[date]:
        """
        Parse relative date strings like "2 days ago", "1 week ago", etc.
        :param metadata_card: BeautifulSoup element containing metadata
        :return: date object or None
        """
        if not metadata_card:
            return None
            
        # Look for text that might contain relative dates
        text_content = metadata_card.get_text().lower()
        
        # Common relative date patterns
        import re
        from datetime import timedelta
        
        today = datetime.now().date()
        
        # Pattern for "X days ago"
        days_match = re.search(r'(\d+)\s+days?\s+ago', text_content)
        if days_match:
            days = int(days_match.group(1))
            return today - timedelta(days=days)
        
        # Pattern for "X weeks ago"
        weeks_match = re.search(r'(\d+)\s+weeks?\s+ago', text_content)
        if weeks_match:
            weeks = int(weeks_match.group(1))
            return today - timedelta(weeks=weeks)
        
        # Pattern for "X months ago"
        months_match = re.search(r'(\d+)\s+months?\s+ago', text_content)
        if months_match:
            months = int(months_match.group(1))
            # Approximate months as 30 days
            return today - timedelta(days=months * 30)
        
        # Pattern for "X years ago"
        years_match = re.search(r'(\d+)\s+years?\s+ago', text_content)
        if years_match:
            years = int(years_match.group(1))
            # Approximate years as 365 days
            return today - timedelta(days=years * 365)
        
        # Pattern for "yesterday"
        if 'yesterday' in text_content:
            return today - timedelta(days=1)
        
        # Pattern for "today"
        if 'today' in text_content:
            return today
        
        # Look for any time element that might have relative text
        time_elements = metadata_card.find_all("time")
        for time_elem in time_elements:
            time_text = time_elem.get_text().lower()
            if any(keyword in time_text for keyword in ['ago', 'yesterday', 'today']):
                # Try to parse this specific time element
                return self._parse_relative_date_from_text(time_text)
        
        return None
    
    def _parse_relative_date_from_text(self, text: str) -> Optional[date]:
        """
        Parse relative date from a specific text string
        :param text: text containing relative date
        :return: date object or None
        """
        import re
        from datetime import timedelta
        
        today = datetime.now().date()
        text = text.lower().strip()
        
        # Pattern for "X days ago"
        days_match = re.search(r'(\d+)\s+days?\s+ago', text)
        if days_match:
            days = int(days_match.group(1))
            return today - timedelta(days=days)
        
        # Pattern for "X weeks ago"
        weeks_match = re.search(r'(\d+)\s+weeks?\s+ago', text)
        if weeks_match:
            weeks = int(weeks_match.group(1))
            return today - timedelta(weeks=weeks)
        
        # Pattern for "X months ago"
        months_match = re.search(r'(\d+)\s+months?\s+ago', text)
        if months_match:
            months = int(months_match.group(1))
            return today - timedelta(days=months * 30)
        
        # Pattern for "X years ago"
        years_match = re.search(r'(\d+)\s+years?\s+ago', text)
        if years_match:
            years = int(years_match.group(1))
            return today - timedelta(days=years * 365)
        
        # Pattern for "yesterday"
        if 'yesterday' in text:
            return today - timedelta(days=1)
        
        # Pattern for "today"
        if 'today' in text:
            return today
        
        return None

