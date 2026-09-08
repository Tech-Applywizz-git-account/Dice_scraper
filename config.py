import os
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()

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

def validate_config():
    if not DATABASE_URL:
        raise ValueError("CRITICAL ERROR: DATABASE_URL environment variable is missing. It is required for connecting to Azure PostgreSQL.")
    
    if not REDIS_URL:
        # We might not need Redis strictly if running in --test-mode locally, but good to check normally.
        print("WARNING: REDIS_URL environment variable is missing. Required for distributed worker processing.")
