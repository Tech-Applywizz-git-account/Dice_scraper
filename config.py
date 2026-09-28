import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Essential URLs
ACTIVE_CLIENTS_URL = os.getenv("ACTIVE_CLIENTS_URL", "https://applywizz-ca-management.vercel.app/api/active-clients")
CLIENT_DETAILS_URL = os.getenv("CLIENT_DETAILS_URL", "https://www.apply-wizz.me/api/get-client-details")
JOB_ROLES_URL = os.getenv("JOB_ROLES_URL", "https://dashboard.apply-wizz.com/job-roles/")

# Redis URL for the queue
REDIS_URL = os.getenv("REDIS_URL")

# Database URL for Azure PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")

# Configuration for workers
MAX_JOBS_PER_SEARCH = int(os.getenv("MAX_JOBS_PER_SEARCH", "30"))
HOURS_OLD = int(os.getenv("HOURS_OLD", "72"))

# Optional test client IDs for local test mode (e.g. "AWL-39223,AWL-32830")
TEST_CLIENT_IDS = os.getenv("TEST_CLIENT_IDS", "")

# Optional local JSON file for specific clients to scrape daily (e.g. "clients.json")
CLIENTS_FILE = os.getenv("CLIENTS_FILE", "clients.json")

def validate_config():
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: DATABASE_URL environment variable is missing. It is required for connecting to Azure PostgreSQL.")
    
    if not REDIS_URL:
        # We might not need Redis strictly if running in --test-mode locally, but good to check normally.
        print("WARNING: REDIS_URL environment variable is missing. Required for distributed worker processing.")
