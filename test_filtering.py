import unittest
from jobspy_enhanced.model import JobPost, Location
from filtering import (
    calculate_max_experience,
    resolve_allowed_roles,
    matches_job_role,
    matches_location_preference,
    filter_jobs_for_client,
    extract_job_experience,
    is_internship_or_trainee_role,
    is_seniority_compatible,
    matches_employment_type
)

class TestDiceScraperFiltering(unittest.TestCase):
    
    def test_calculate_max_experience(self):
        # Rule 1: whole number -> exactly that number
        self.assertEqual(calculate_max_experience(5.0, "5"), 5.0)
        self.assertEqual(calculate_max_experience(5.0, "5.0"), 5.0)
        self.assertEqual(calculate_max_experience(4.0, "4"), 4.0)
        self.assertEqual(calculate_max_experience(7.0, "7"), 7.0)
        
        # Rule 2: fractional number (e.g. 5.5) -> ceil(5.5) = 6.0, not more than 1 year over
        self.assertEqual(calculate_max_experience(5.5, "5.5"), 6.0)
        self.assertEqual(calculate_max_experience(3.5, "3.5"), 4.0)
        
        # Rule 3: plus suffix (e.g. "5+") -> 6.0, not more than 1 year over
        self.assertEqual(calculate_max_experience(5.0, "5+"), 6.0)
        self.assertEqual(calculate_max_experience(6.0, "6+"), 7.0)
        self.assertEqual(calculate_max_experience(2.0, "2+"), 3.0)
        
        # Entry level / 0 years
        self.assertEqual(calculate_max_experience(0.0, "0"), 1.0)
        
    def test_resolve_allowed_roles(self):
        role_data = [
            {"id": 135, "name": "Network Engineer", "alternate_roles": "Network Support Engineer, Network Operations Engineer, Cisco Network Engineer", "status": "Active"},
            {"id": 99, "name": "Data Analyst", "alternate_roles": "Power BI, Tableau, Data Visualisation, Reporting, Data Analytics", "status": "Active"},
            {"id": 83, "name": "Bioinformatics for UK", "alternate_roles": "Computational Biology, clinical research", "status": "Active"}
        ]
        
        # Network Engineer domain resolution
        allowed = resolve_allowed_roles("Network Engineer", "Network Infrastructure Engineer", role_data)
        self.assertIn("network engineer", allowed)
        self.assertIn("network support engineer", allowed)
        self.assertIn("cisco network engineer", allowed)
        self.assertIn("network infrastructure engineer", allowed)
        
        # Suffix stripping ("Bioinformatics for UK" -> "bioinformatics")
        allowed_bio = resolve_allowed_roles("Bioinformatics", "", role_data)
        self.assertIn("bioinformatics", allowed_bio)
        self.assertIn("computational biology", allowed_bio)

    def test_matches_job_role(self):
        allowed = ["network engineer", "network support engineer", "cisco network engineer"]
        
        # Positive domain matches
        self.assertTrue(matches_job_role("Network Engineer", allowed))
        self.assertTrue(matches_job_role("Senior Network Engineer", allowed))
        self.assertTrue(matches_job_role("Network Engineer II", allowed))
        self.assertTrue(matches_job_role("Lead Cisco Network Engineer", allowed))
        self.assertTrue(matches_job_role("Network Systems Engineer", allowed))
        self.assertTrue(matches_job_role("Staff Network Support Engineer", allowed))
        self.assertTrue(matches_job_role("Engineer - Network Security", allowed))
        self.assertTrue(matches_job_role("ITS Sr Engineer I / IS Network", allowed))
        
        # Negative domain matches (domain mismatch!)
        self.assertFalse(matches_job_role("Software Engineer", allowed))
        self.assertFalse(matches_job_role("Senior Software Engineer", allowed))
        self.assertFalse(matches_job_role("Java Developer", allowed))
        self.assertFalse(matches_job_role("Data Analyst", allowed))
        self.assertFalse(matches_job_role("Data Engineer", allowed))
        self.assertFalse(matches_job_role("Mechanical Engineer", allowed))
        self.assertFalse(matches_job_role("Civil Engineer", allowed))
        self.assertFalse(matches_job_role("Account Executive", allowed))
        self.assertFalse(matches_job_role("Technical Recruiter", allowed))

    def test_matches_location_preference(self):
        # Client prefers Connecticut
        pref_ct = ["Connecticut"]
        
        # In Connecticut
        job_hartford = JobPost(
            title="Network Engineer",
            company_name="TechCorp",
            job_url="https://dice.com/job/1",
            location=Location(city="Hartford", state="CT", country="USA")
        )
        self.assertTrue(matches_location_preference(job_hartford, pref_ct))
        
        job_stamford = JobPost(
            title="Network Engineer",
            company_name="TechCorp",
            job_url="https://dice.com/job/2",
            location=Location(city="Stamford", state="Connecticut", country="USA")
        )
        self.assertTrue(matches_location_preference(job_stamford, pref_ct))
        
        # Conflicting locations (Atlanta, GA / Dallas, TX)
        job_atlanta = JobPost(
            title="Network Engineer",
            company_name="TechCorp",
            job_url="https://dice.com/job/3",
            location=Location(city="Atlanta", state="GA", country="USA")
        )
        self.assertFalse(matches_location_preference(job_atlanta, pref_ct))
        
        job_dallas = JobPost(
            title="Network Engineer",
            company_name="TechCorp",
            job_url="https://dice.com/job/4",
            location=Location(city="Dallas", state="TX", country="USA")
        )
        self.assertFalse(matches_location_preference(job_dallas, pref_ct))
        
        # Nationwide client (empty list) accepts all US locations
        self.assertTrue(matches_location_preference(job_atlanta, []))
        self.assertTrue(matches_location_preference(job_dallas, ["United States"]))

    def test_filter_jobs_for_client_end_to_end(self):
        # Setup client requirements: 5 years experience, Network Engineer, in Connecticut
        requirements = {
            "applywizz_id": "AWL-39223",
            "name": "Adarsh Teakaigari",
            "role": "Network Engineer",
            "alternate_roles": "Network Support Engineer, Cisco Network Engineer",
            "experience": 5.0,
            "experience_raw": "5",
            "locations": ["Connecticut"],
            "work_preference": "all",
            "exclude_companies": ["BlacklistedCorp"],
            "sponsorship": "no"
        }
        
        # Create test jobs
        # 1. Perfect match (5 yrs, Network Engineer, CT)
        j1 = JobPost(
            title="Senior Network Engineer",
            company_name="GoodCorp",
            job_url="https://dice.com/job/101",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j1, 'experience', '5+ years')
        
        # 2. Perfect match (4 yrs, Cisco Network Engineer, CT)
        j2 = JobPost(
            title="Cisco Network Engineer",
            company_name="NetworkPro",
            job_url="https://dice.com/job/102",
            location=Location(city="New Haven", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j2, 'experience', '4 years')
        
        # 3. Domain mismatch (Software Engineer, CT, 5 yrs) -> REJECT
        j3 = JobPost(
            title="Software Engineer",
            company_name="TechCorp",
            job_url="https://dice.com/job/103",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j3, 'experience', '5 years')
        
        # 4. Experience mismatch (Network Engineer, CT, but requires 7+ yrs) -> REJECT
        j4 = JobPost(
            title="Principal Network Engineer",
            company_name="EnterpriseCorp",
            job_url="https://dice.com/job/104",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j4, 'experience', '7+ years')
        
        # 5. Experience mismatch (Network Engineer, CT, but requires 6 yrs when client has 5) -> REJECT
        j5 = JobPost(
            title="Lead Network Engineer",
            company_name="EnterpriseCorp",
            job_url="https://dice.com/job/105",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j5, 'experience', '6+ years')
        
        # 6. Location mismatch (Network Engineer, 5 yrs, but in Atlanta GA) -> REJECT
        j6 = JobPost(
            title="Network Engineer",
            company_name="SouthernTech",
            job_url="https://dice.com/job/106",
            location=Location(city="Atlanta", state="GA", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j6, 'experience', '5 years')
        
        # 7. Excluded company -> REJECT
        j7 = JobPost(
            title="Network Engineer",
            company_name="BlacklistedCorp",
            job_url="https://dice.com/job/107",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j7, 'experience', '5 years')
        
        all_jobs = [j1, j2, j3, j4, j5, j6, j7]
        filtered = filter_jobs_for_client(all_jobs, requirements)
        
        # Only j1 and j2 should survive!
        filtered_urls = [j.job_url for j in filtered]
        self.assertEqual(len(filtered), 2)
        self.assertIn("https://dice.com/job/101", filtered_urls)
        self.assertIn("https://dice.com/job/102", filtered_urls)
        self.assertNotIn("https://dice.com/job/103", filtered_urls) # Domain mismatch rejected
        self.assertNotIn("https://dice.com/job/104", filtered_urls) # 7 yrs rejected
        self.assertNotIn("https://dice.com/job/105", filtered_urls) # 6 yrs rejected for 5 yr exp
        self.assertNotIn("https://dice.com/job/106", filtered_urls) # GA rejected for CT pref
        self.assertNotIn("https://dice.com/job/107", filtered_urls) # Excluded company rejected

    def test_filter_jobs_fractional_experience(self):
        # Client with 5+ or 5.5 experience: accepts up to 6 years
        requirements_plus = {
            "role": "Network Engineer",
            "experience": 5.0,
            "experience_raw": "5+",
            "locations": ["Connecticut"],
            "work_preference": "all"
        }
        
        j_6yr = JobPost(
            title="Network Engineer",
            company_name="NetCorp",
            job_url="https://dice.com/job/201",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j_6yr, 'experience', '6 years')
        
        j_7yr = JobPost(
            title="Network Engineer",
            company_name="NetCorp",
            job_url="https://dice.com/job/202",
            location=Location(city="Hartford", state="CT", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(j_7yr, 'experience', '7 years')
        
        filtered = filter_jobs_for_client([j_6yr, j_7yr], requirements_plus)
        filtered_urls = [j.job_url for j in filtered]
        self.assertIn("https://dice.com/job/201", filtered_urls) # 6 yrs allowed for 5+
        self.assertNotIn("https://dice.com/job/202", filtered_urls) # 7 yrs rejected (not more than 1 yr+)

        # Test 5.5 decimal
        requirements_decimal = {
            "role": "Network Engineer",
            "experience": 5.5,
            "experience_raw": "5.5",
            "locations": ["Connecticut"],
            "work_preference": "all"
        }
        filtered_dec = filter_jobs_for_client([j_6yr, j_7yr], requirements_decimal)
        self.assertEqual(len(filtered_dec), 1)
        self.assertEqual(filtered_dec[0].job_url, "https://dice.com/job/201")

    def test_data_analyst_domain_matching(self):
        allowed = resolve_allowed_roles("Data Analyst", "Power BI, Tableau, Reporting")
        
        # Matches
        self.assertTrue(matches_job_role("Senior Data Analyst", allowed))
        self.assertTrue(matches_job_role("Lead Data Analyst - Analytics", allowed))
        self.assertTrue(matches_job_role("Power BI Developer / Analyst", allowed))
        self.assertTrue(matches_job_role("Tableau Reporting Analyst", allowed))
        
        # Rejections (domain mismatch)
        self.assertFalse(matches_job_role("Software Engineer", allowed))
        self.assertFalse(matches_job_role("Java Developer", allowed))
        self.assertFalse(matches_job_role("Financial Analyst", allowed))
        self.assertFalse(matches_job_role("Credit Analyst", allowed))
        self.assertFalse(matches_job_role("Civil Engineer", allowed))

    def test_city_and_state_location_matching(self):
        # Client prefers Dallas, TX
        pref_dallas = ["Dallas, TX"]
        j_dallas = JobPost(
            title="DevOps Engineer",
            company_name="Tech",
            job_url="https://dice.com/job/301",
            location=Location(city="Dallas", state="TX", country="USA")
        )
        j_austin = JobPost(
            title="DevOps Engineer",
            company_name="Tech",
            job_url="https://dice.com/job/302",
            location=Location(city="Austin", state="TX", country="USA")
        )
        j_atlanta = JobPost(
            title="DevOps Engineer",
            company_name="Tech",
            job_url="https://dice.com/job/303",
            location=Location(city="Atlanta", state="GA", country="USA")
        )
        self.assertTrue(matches_location_preference(j_dallas, pref_dallas))
        self.assertFalse(matches_location_preference(j_atlanta, pref_dallas))

    def test_is_internship_or_trainee_role(self):
        self.assertTrue(is_internship_or_trainee_role("Artificial Intelligence/Machine Learning Engineer 2 - Intern"))
        self.assertTrue(is_internship_or_trainee_role("Data Science Internship"))
        self.assertTrue(is_internship_or_trainee_role("Software Engineering Co-op"))
        self.assertTrue(is_internship_or_trainee_role("Graduate Trainee Engineer"))
        self.assertTrue(is_internship_or_trainee_role("Student Research Assistant"))
        
        self.assertFalse(is_internship_or_trainee_role("Machine Learning Engineer"))
        self.assertFalse(is_internship_or_trainee_role("Senior AI Engineer"))
        self.assertFalse(is_internship_or_trainee_role("Lead Data Scientist"))

    def test_is_seniority_compatible(self):
        # 1. Intern role: rejected for 4-year candidate, allowed for 0-year
        self.assertFalse(is_seniority_compatible("Machine Learning Engineer - Intern", client_exp=4.0))
        self.assertTrue(is_seniority_compatible("Machine Learning Engineer - Intern", client_exp=0.0))
        self.assertTrue(is_seniority_compatible("Machine Learning Engineer - Intern", client_exp=4.0, client_wants_internship=True))
        
        # 2. Executive / Principal / Staff / Architect / Director: rejected if client exp < 6.0
        self.assertFalse(is_seniority_compatible("Principal Machine Learning Engineer", client_exp=4.0))
        self.assertFalse(is_seniority_compatible("Staff AI Engineer", client_exp=5.0))
        self.assertFalse(is_seniority_compatible("Enterprise Cloud Architect", client_exp=3.0))
        self.assertTrue(is_seniority_compatible("Principal Machine Learning Engineer", client_exp=7.0))
        self.assertTrue(is_seniority_compatible("Staff AI Engineer", client_exp=6.0))
        
        # 3. Lead / Manager: rejected if client exp < 5.0
        self.assertFalse(is_seniority_compatible("Lead Full Stack AI/ML Engineer", client_exp=4.0))
        self.assertFalse(is_seniority_compatible("Engineering Manager - Data", client_exp=3.0))
        self.assertTrue(is_seniority_compatible("Lead Full Stack AI/ML Engineer", client_exp=5.0))
        self.assertTrue(is_seniority_compatible("Lead Full Stack AI/ML Engineer", client_exp=6.0))
        
        # 4. Senior: rejected if client exp < 4.0
        self.assertFalse(is_seniority_compatible("Senior GEN AI Engineer", client_exp=1.0))
        self.assertFalse(is_seniority_compatible("Sr. Machine Learning Engineer", client_exp=0.0))
        self.assertFalse(is_seniority_compatible("Senior GEN AI Engineer", client_exp=3.0))
        self.assertTrue(is_seniority_compatible("Senior GEN AI Engineer", client_exp=4.0))
        self.assertTrue(is_seniority_compatible("Senior GEN AI Engineer", client_exp=5.0))

    def test_filter_jobs_rejects_intern_for_experienced_candidate(self):
        # Candidate with 4 years exp
        requirements = {
            "applywizz_id": "AWL-26023",
            "role": "AI/ML Engineer",
            "alternate_roles": "Machine Learning, AI Engineer",
            "experience": 4.0,
            "locations": ["Texas"],
            "work_preference": "all"
        }
        
        intern_job = JobPost(
            title="Artificial Intelligence/Machine Learning Engineer 2 - Intern",
            company_name="TechCo",
            job_url="https://dice.com/job/intern1",
            location=Location(city="Austin", state="TX", country="USA"),
            employment_type="Internship"
        )
        
        senior_job = JobPost(
            title="Senior GEN AI Engineer",
            company_name="TechCo",
            job_url="https://dice.com/job/senior1",
            location=Location(city="Plano", state="TX", country="USA"),
            employment_type="Full Time"
        )
        
        lead_job = JobPost(
            title="Lead Full Stack AI/ML Engineer",
            company_name="TechCo",
            job_url="https://dice.com/job/lead1",
            location=Location(city="Dallas", state="TX", country="USA"),
            employment_type="Full Time"
        )
        
        mid_job = JobPost(
            title="Gen AI Engineer",
            company_name="TechCo",
            job_url="https://dice.com/job/mid1",
            location=Location(city="Irving", state="TX", country="USA"),
            employment_type="Full Time"
        )
        object.__setattr__(mid_job, 'experience', '3+ years')
        
        filtered = filter_jobs_for_client([intern_job, senior_job, lead_job, mid_job], requirements)
        filtered_titles = [j.title for j in filtered]
        
        # Intern and Lead must be rejected for 4-year candidate!
        self.assertNotIn("Artificial Intelligence/Machine Learning Engineer 2 - Intern", filtered_titles)
        self.assertNotIn("Lead Full Stack AI/ML Engineer", filtered_titles)
        
        # Senior and Mid should be accepted!
        self.assertIn("Senior GEN AI Engineer", filtered_titles)
        self.assertIn("Gen AI Engineer", filtered_titles)

    def test_extract_experience_prefix_and_range(self):
        from jobspy_enhanced.dice.util import extract_experience_from_description
        from filtering import extract_job_experience
        
        # 1. Prefix format: "Experience Range: 4 - 6 Years"
        desc1 = "Location: Diamond Bar, CA (Onsite)\nExperience Range: 4 - 6 Years\nPosition Overview:"
        self.assertEqual(extract_experience_from_description(desc1), "4-6 years")
        dummy_job1 = JobPost(title="Test", company_name="Test", job_url="http://test", location=Location(city="San Jose", state="CA"), description=desc1)
        self.assertEqual(extract_job_experience(dummy_job1), 4.0)
        
        # 2. Prefix format: "Experience Required: 5+ years"
        desc2 = "Experience Required: 5+ years in Python and AWS"
        self.assertEqual(extract_experience_from_description(desc2), "5+ years")
        dummy_job2 = JobPost(title="Test", company_name="Test", job_url="http://test", location=Location(city="San Jose", state="CA"), description=desc2)
        self.assertEqual(extract_job_experience(dummy_job2), 5.0)
        
        # 3. En-dash with domain words: "3–6 years of Business Analyst experience"
        desc3 = "Qualifications: 3–6 years of Business Analyst/Functional Consultant experience"
        self.assertEqual(extract_experience_from_description(desc3), "3-6 years")
        dummy_job3 = JobPost(title="Test", company_name="Test", job_url="http://test", location=Location(city="San Jose", state="CA"), description=desc3)
        self.assertEqual(extract_job_experience(dummy_job3), 3.0)
        
        # 4. Years of Experience prefix
        desc4 = "Years of Experience: 2 - 4"
        self.assertEqual(extract_experience_from_description(desc4), "2-4 years")
        
        # 5. Word to num
        desc5 = "Requires five to seven years of relevant experience"
        self.assertEqual(extract_experience_from_description(desc5), "5-7 years")

    def test_matches_job_role_delimiter_and_security_guard(self):
        ai_allowed = [
            "ai/ml engineer", "machine learning", "ai engineer", "ml engineer",
            "artificial intelligence", "deep learning", "data scientist", "applied scientist"
        ]
        
        # False positive across pipe delimiter: "Security Engineer ... | AI Security" must be REJECTED!
        self.assertFalse(matches_job_role(
            "Security Engineer Offensive Security | Bug Bounty | Penetration Testing | AI Security",
            ai_allowed
        ))
        
        # Real AI titles must be ACCEPTED
        self.assertTrue(matches_job_role("AI ENGINEER", ai_allowed))
        self.assertTrue(matches_job_role("Machine Learning Engineer", ai_allowed))
        self.assertTrue(matches_job_role("Senior Data Scientist / ML Engineer", ai_allowed))
        self.assertTrue(matches_job_role("AI and Agentic Engineer II", ai_allowed))
        self.assertTrue(matches_job_role("Senior Engineer - AI/ML", ai_allowed))
        
        # Inverted Network Engineer titles must still be ACCEPTED
        net_allowed = ["network engineer", "network support engineer"]
        self.assertTrue(matches_job_role("Engineer - Network Security", net_allowed))
        self.assertTrue(matches_job_role("ITS Sr Engineer I / IS Network", net_allowed))

    def test_extract_state_from_location(self):
        from filtering import extract_state_from_location
        self.assertEqual(extract_state_from_location("San Antonio, TX"), "Texas")
        self.assertEqual(extract_state_from_location(" West Haven, CT "), "Connecticut")
        self.assertEqual(extract_state_from_location("Saint Louis, MO"), "Missouri")
        self.assertEqual(extract_state_from_location("Lewisville, TX"), "Texas")
        self.assertEqual(extract_state_from_location("Colorado"), "Colorado")
        self.assertEqual(extract_state_from_location("TX"), "Texas")
        self.assertIsNone(extract_state_from_location("Denver"))
        self.assertIsNone(extract_state_from_location("na"))
        self.assertIsNone(extract_state_from_location(""))

    def test_client_location_prioritization_and_country_fallback(self):
        from api_client import extract_client_requirements
        
        # 1. AWL-35179: Explicit preferences first, then residence Colorado, then United States last
        c1 = {
            "client": {
                "applywizz_id": "AWL-35179",
                "location_preferences": ["Denver", "Virginia", "Utah", "California", "Texas"]
            },
            "additional_information": {
                "state_of_residence": "Colorado"
            }
        }
        r1 = extract_client_requirements(c1)
        self.assertEqual(r1["client_location"], "Colorado")
        self.assertEqual(r1["locations"][0], "Denver")
        self.assertEqual(r1["locations"][-2], "Colorado")
        self.assertEqual(r1["locations"][-1], "United States")
        self.assertEqual(r1["client_preferred_locations"], "Denver, Virginia, Utah, California, Texas, Colorado, United States")
        
        # 2. AWL-39240: Empty preferences -> residence San Antonio, TX, Texas, then United States last
        c2 = {
            "client": {
                "applywizz_id": "AWL-39240",
                "location_preferences": []
            },
            "additional_information": {
                "state_of_residence": "San Antonio, TX"
            }
        }
        r2 = extract_client_requirements(c2)
        self.assertEqual(r2["client_location"], "San Antonio, TX")
        self.assertEqual(r2["locations"], ["San Antonio, TX", "Texas", "United States"])
        self.assertEqual(r2["client_preferred_locations"], "San Antonio, TX, Texas, United States")
        
        # 3. AWL-39223: Preferences ["Connecticut"] first, then residence West Haven, CT, then United States
        c3 = {
            "client": {
                "applywizz_id": "AWL-39223",
                "location_preferences": ["Connecticut"]
            },
            "additional_information": {
                "state_of_residence": " West Haven, CT "
            }
        }
        r3 = extract_client_requirements(c3)
        self.assertEqual(r3["client_location"], "West Haven, CT")
        self.assertEqual(r3["locations"], ["Connecticut", "West Haven, CT", "United States"])
        self.assertEqual(r3["client_preferred_locations"], "Connecticut, West Haven, CT, United States")
        
        # 4. Empty residence and empty preferences -> fallback to country
        c4 = {
            "client": {
                "applywizz_id": "AWL-EMPTY",
                "location_preferences": []
            },
            "additional_information": {
                "state_of_residence": ""
            }
        }
        r4 = extract_client_requirements(c4)
        self.assertEqual(r4["client_location"], "United States")
        self.assertEqual(r4["locations"], ["United States"])
        self.assertEqual(r4["client_preferred_locations"], "United States")

    def test_employment_type_rules(self):
        """
        Tests the 9 required business rules for Dice employment types:
        1. Full Time job is accepted.
        2. Contract W2 job is rejected.
        3. Contract Independent job is rejected.
        4. Contract Corp To Corp job is rejected.
        5. A Full Time job whose unrelated page/description text mentions 'contract' is accepted.
        6. Internship with client experience 0 is accepted.
        7. Internship with client experience greater than 0 is rejected.
        8. Missing or ambiguous employment type follows conservative behavior (rejected).
        """
        # 1. Full Time job is accepted
        job_ft = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/1",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Full Time"
        )
        ok, reason = matches_employment_type(job_ft, client_exp=5.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "full_time")
        
        # 2. Contract W2 job is accepted
        job_w2 = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/2",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Contract W2"
        )
        ok, reason = matches_employment_type(job_w2, client_exp=5.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "w2_contract")
        
        # 3. Contract Independent job is rejected
        job_ind = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/3",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Contract Independent"
        )
        ok, reason = matches_employment_type(job_ind, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "contract")
        
        # 4. Contract Corp To Corp job is rejected
        job_c2c = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/4",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Contract Corp To Corp"
        )
        ok, reason = matches_employment_type(job_c2c, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "contract")
        
        # Additional contract variants: 1099, C2C, Independent Contractor, Contract
        for c_type in ["1099", "C2C", "Corp To Corp", "Independent Contractor", "Contract", "Contract to Hire"]:
            job_c = JobPost(
                title="Data Analyst",
                company_name="Co",
                job_url="http://dice.com/c",
                location=Location(city="Atlanta", state="GA"),
                employment_type=c_type
            )
            ok, reason = matches_employment_type(job_c, client_exp=5.0)
            self.assertFalse(ok, f"Expected {c_type} to be rejected")
            self.assertEqual(reason, "contract")
            
        # 5. Full Time job with unrelated description/page text mentioning 'contract' is accepted
        desc_with_contract = "Great full time position. You will manage software vendor contracts and agreements with external contractors."
        job_ft_contract_desc = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/5",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Full Time",
            description=desc_with_contract
        )
        ok, reason = matches_employment_type(job_ft_contract_desc, client_exp=5.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "full_time")
        
        # 6. Internship with client experience 0 is accepted
        job_intern = JobPost(
            title="Data Analyst Intern",
            company_name="Co",
            job_url="http://dice.com/6",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Internship"
        )
        ok, reason = matches_employment_type(job_intern, client_exp=0.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "internship_allowed_for_0_exp")
        
        # 7. Internship with client experience > 0 is rejected
        ok, reason = matches_employment_type(job_intern, client_exp=1.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "internship_rejected_for_experienced_client")
        
        ok, reason = matches_employment_type(job_intern, client_exp=4.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "internship_rejected_for_experienced_client")

        # 8. Missing or ambiguous employment type follows conservative behavior (rejected)
        job_missing1 = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/7",
            location=Location(city="Atlanta", state="GA")
        )
        ok, reason = matches_employment_type(job_missing1, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "missing_employment_type")
        
        job_missing2 = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/8",
            location=Location(city="Atlanta", state="GA"),
            employment_type=""
        )
        ok, reason = matches_employment_type(job_missing2, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "missing_employment_type")
        
        job_missing3 = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/9",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Not Specified"
        )
        ok, reason = matches_employment_type(job_missing3, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "missing_employment_type")
        
        job_ambiguous = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/10",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Part Time"
        )
        ok, reason = matches_employment_type(job_ambiguous, client_exp=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "ambiguous_employment_type")

        # Multi-badge: Contract W2 + Full Time -> Full Time accepted by priority 1
        job_multi = JobPost(
            title="Data Analyst",
            company_name="Co",
            job_url="http://dice.com/11",
            location=Location(city="Atlanta", state="GA"),
            employment_type="Full Time, Contract W2"
        )
        ok, reason = matches_employment_type(job_multi, client_exp=5.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "full_time")

    def test_filter_jobs_for_client_employment_type_integration(self):
        """Tests filter_jobs_for_client end-to-end with employment-type rules."""
        requirements_exp5 = {
            "applywizz_id": "AWL-EXP5",
            "role": "Data Analyst",
            "experience": 5.0,
            "locations": ["Georgia"],
            "work_preference": "all"
        }
        requirements_exp0 = {
            "applywizz_id": "AWL-EXP0",
            "role": "Data Analyst",
            "experience": 0.0,
            "locations": ["Georgia"],
            "work_preference": "all"
        }
        
        j_ft = JobPost(title="Data Analyst", company_name="Co", job_url="http://dice.com/ft", location=Location(city="Atlanta", state="GA"), employment_type="Full Time")
        j_w2 = JobPost(title="Data Analyst", company_name="Co", job_url="http://dice.com/w2", location=Location(city="Atlanta", state="GA"), employment_type="Contract W2")
        j_c2c = JobPost(title="Data Analyst", company_name="Co", job_url="http://dice.com/c2c", location=Location(city="Atlanta", state="GA"), employment_type="Contract Corp To Corp")
        j_ind = JobPost(title="Data Analyst", company_name="Co", job_url="http://dice.com/ind", location=Location(city="Atlanta", state="GA"), employment_type="Contract Independent")
        j_intern = JobPost(title="Data Analyst Intern", company_name="Co", job_url="http://dice.com/intern", location=Location(city="Atlanta", state="GA"), employment_type="Internship")
        j_missing = JobPost(title="Data Analyst", company_name="Co", job_url="http://dice.com/missing", location=Location(city="Atlanta", state="GA"))
        
        all_test_jobs = [j_ft, j_w2, j_c2c, j_ind, j_intern, j_missing]
        
        # Experienced client (5 years): Full Time and Contract W2 pass
        res_exp5 = filter_jobs_for_client(all_test_jobs, requirements_exp5)
        res_urls5 = [j.job_url for j in res_exp5]
        self.assertEqual(len(res_exp5), 2)
        self.assertIn("http://dice.com/ft", res_urls5)
        self.assertIn("http://dice.com/w2", res_urls5)
        self.assertNotIn("http://dice.com/c2c", res_urls5)
        self.assertNotIn("http://dice.com/ind", res_urls5)
        self.assertNotIn("http://dice.com/intern", res_urls5)
        self.assertNotIn("http://dice.com/missing", res_urls5)
        
        # Entry-level client (0 years): Full Time, Contract W2, and Internship pass
        res_exp0 = filter_jobs_for_client(all_test_jobs, requirements_exp0)
        res_urls0 = [j.job_url for j in res_exp0]
        self.assertEqual(len(res_exp0), 3)
        self.assertIn("http://dice.com/ft", res_urls0)
        self.assertIn("http://dice.com/w2", res_urls0)
        self.assertIn("http://dice.com/intern", res_urls0)
        self.assertNotIn("http://dice.com/c2c", res_urls0)
        self.assertNotIn("http://dice.com/ind", res_urls0)
        self.assertNotIn("http://dice.com/missing", res_urls0)

    def test_parser_extract_dice_employment_type(self):
        """Tests util.extract_dice_employment_type on mock HTML and JSON-LD."""
        from bs4 import BeautifulSoup
        from jobspy_enhanced.dice.util import extract_dice_employment_type
        
        # 1. Header card with Full Time badge
        html_ft = """
        <div data-testid="job-detail-header-card">
            <h1>Data Analyst</h1>
            <span class="SeuiInfoBadge-root">Full Time</span>
            <span class="SeuiInfoBadge-root">On-site</span>
        </div>
        """
        soup_ft = BeautifulSoup(html_ft, 'html.parser')
        self.assertEqual(extract_dice_employment_type(soup_ft), "Full Time")
        
        # 2. Header card with multiple contract badges
        html_contract = """
        <div data-testid="job-detail-header-card">
            <h1>Data Analyst</h1>
            <span class="SeuiInfoBadge-root">Contract W2</span>
            <span class="SeuiInfoBadge-root">Contract Corp To Corp</span>
            <span class="SeuiInfoBadge-root">Contract Independent</span>
        </div>
        """
        soup_contract = BeautifulSoup(html_contract, 'html.parser')
        extracted_contract = extract_dice_employment_type(soup_contract)
        self.assertIn("Contract W2", extracted_contract)
        self.assertIn("Contract Corp To Corp", extracted_contract)
        self.assertIn("Contract Independent", extracted_contract)
        
        # 3. Fallback: JSON-LD FULL_TIME
        html_jsonld_ft = """
        <div>
            <h1>Data Analyst</h1>
            <script type="application/ld+json">
            {
                "@type": "JobPosting",
                "title": "Data Analyst",
                "employmentType": "FULL_TIME"
            }
            </script>
        </div>
        """
        soup_jsonld_ft = BeautifulSoup(html_jsonld_ft, 'html.parser')
        self.assertEqual(extract_dice_employment_type(soup_jsonld_ft), "Full Time")
        
        # 4. Fallback: JSON-LD CONTRACTOR
        html_jsonld_contract = """
        <div>
            <h1>Data Analyst</h1>
            <script type="application/ld+json">
            {
                "@type": "JobPosting",
                "title": "Data Analyst",
                "employmentType": "CONTRACTOR"
            }
            </script>
        </div>
        """
        soup_jsonld_contract = BeautifulSoup(html_jsonld_contract, 'html.parser')
        self.assertEqual(extract_dice_employment_type(soup_jsonld_contract), "Contract")

    def test_get_active_clients_json_and_fallback(self):
        """Tests that get_active_clients loads from JSON if present, else falls back to API."""
        import tempfile
        import json
        from unittest.mock import patch
        import api_client
        
        # 1. With JSON file present
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as tf:
            json.dump(["AWL-TEST-1", "AWL-TEST-2"], tf)
            tf_path = tf.name
            
        with patch.object(api_client, 'CLIENTS_FILE', tf_path):
            clients = api_client.get_active_clients()
            self.assertEqual(clients, ["AWL-TEST-1", "AWL-TEST-2"])
            
        # 2. When JSON file does not exist -> falls back to API
        with patch.object(api_client, 'CLIENTS_FILE', '/path/does/not/exist.json'):
            with patch('requests.get') as mock_get:
                mock_get.return_value.status_code = 200
                mock_get.return_value.json.return_value = {"applywizz_ids": ["AWL-API-1", "AWL-API-2"]}
                clients = api_client.get_active_clients()
                self.assertEqual(clients, ["AWL-API-1", "AWL-API-2"])
                mock_get.assert_called_once()

    def test_non_us_country_location_filtering(self):
        """Tests that non-US country location preferences are properly supported."""
        from jobspy_enhanced.dice.util import parse_location
        
        # 1. Location parsing for non-US
        loc_uk = parse_location("London, UK")
        self.assertEqual(loc_uk.city, "London")
        self.assertEqual(loc_uk.country, "UK")

        loc_ca = parse_location("Toronto, ON")
        self.assertEqual(loc_ca.city, "Toronto")
        self.assertEqual(loc_ca.state, "ON")
        self.assertEqual(loc_ca.country, "CA")

        loc_ca_full = parse_location("Vancouver, BC, Canada")
        self.assertEqual(loc_ca_full.city, "Vancouver")
        self.assertEqual(loc_ca_full.country, "CA")

        # 2. Location preference matching for UK client
        job_london = JobPost(
            id="job-uk-1",
            title="Data Analyst",
            company_name="Test Corp",
            job_url="https://dice.com/job/uk-1",
            location=Location(city="London", country="UK"),
            employment_type="Full Time"
        )
        job_ny = JobPost(
            id="job-us-1",
            title="Data Analyst",
            company_name="Test Corp",
            job_url="https://dice.com/job/us-1",
            location=Location(city="New York", state="NY", country="USA"),
            employment_type="Full Time"
        )
        
        # UK client with London / UK
        self.assertTrue(matches_location_preference(job_london, ["Greater London", "United Kingdom"], "all", client_country="United Kingdom"))
        # UK client should not match random US job in NY
        self.assertFalse(matches_location_preference(job_ny, ["Greater London", "United Kingdom"], "all", client_country="United Kingdom"))

        # Canada client with Toronto / Canada
        job_toronto = JobPost(
            id="job-ca-1",
            title="Software Engineer",
            company_name="Test Corp",
            job_url="https://dice.com/job/ca-1",
            location=Location(city="Toronto", state="ON", country="CA"),
            employment_type="Full Time"
        )
        self.assertTrue(matches_location_preference(job_toronto, ["Toronto, ON", "Canada"], "all", client_country="Canada"))
        self.assertFalse(matches_location_preference(job_ny, ["Toronto, ON", "Canada"], "all", client_country="Canada"))

    def test_dice_scraper_country_code(self):
        """Tests that Dice scraper derives the correct 2-letter ISO countryCode."""
        from jobspy_enhanced.dice import Dice
        from jobspy_enhanced.model import ScraperInput, Site, Country
        
        scraper = Dice()
        
        # USA -> US
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.USA)
        self.assertEqual(scraper._get_country_code(), "US")
        
        # UK -> GB
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.UK)
        self.assertEqual(scraper._get_country_code(), "GB")

        # Canada -> CA
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.CANADA)
        self.assertEqual(scraper._get_country_code(), "CA")

        # Ireland -> IE
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.IRELAND)
        self.assertEqual(scraper._get_country_code(), "IE")

        # Germany -> DE
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.GERMANY)
        self.assertEqual(scraper._get_country_code(), "DE")

        # Australia -> AU
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.AUSTRALIA)
        self.assertEqual(scraper._get_country_code(), "AU")

        # India -> IN
        scraper.scraper_input = ScraperInput(site_type=[Site.DICE], search_term="Dev", country=Country.INDIA)
        self.assertEqual(scraper._get_country_code(), "IN")

    def test_is_job_expired(self):
        from bs4 import BeautifulSoup
        from jobspy_enhanced.dice.util import is_job_expired

        # Expired page with inline-message alert
        expired_html = '''
        <html>
            <body>
                <div data-testid="inline-message" role="alert" class="text-danger">
                    Sorry this job is no longer available. The Similar Jobs shown below might interest you.
                </div>
                <h1>Software Engineer</h1>
            </body>
        </html>
        '''
        soup_expired = BeautifulSoup(expired_html, 'html.parser')
        self.assertTrue(is_job_expired(soup_expired, expired_html))

        # Active job page
        active_html = '''
        <html>
            <body>
                <h1>Senior Python Developer</h1>
                <button data-testid="apply-button">Apply Now</button>
            </body>
        </html>
        '''
        soup_active = BeautifulSoup(active_html, 'html.parser')
        self.assertFalse(is_job_expired(soup_active, active_html))

if __name__ == '__main__':
    unittest.main()


