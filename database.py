import psycopg2
from psycopg2.extras import execute_values
from config import DATABASE_URL, validate_config

def get_connection():
    validate_config()
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Ensure the unique constraint exists for duplicate protection."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check if constraint exists
            cur.execute("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'jobs' 
                AND constraint_type = 'UNIQUE' 
                AND constraint_name = 'jobs_url_applywizz_unique';
            """)
            if not cur.fetchone():
                print("Adding unique constraint jobs_url_applywizz_unique to jobs table...")
                cur.execute("""
                    ALTER TABLE public.jobs 
                    ADD CONSTRAINT jobs_url_applywizz_unique UNIQUE (url, applywizz_id);
                """)
                conn.commit()
            else:
                print("Unique constraint jobs_url_applywizz_unique already exists.")
    finally:
        conn.close()

def upsert_jobs(jobs_data):
    """
    Inserts jobs into Azure PostgreSQL. Skips existing jobs (duplicate URL + applywizz_id).
    
    jobs_data should be a list of dicts:
    [{'url': '...', 'title': '...', 'company': '...', 'applywizz_id': '...', 'company_email': '...'}]
    
    Returns: (inserted_count, skipped_count)
    """
    if not jobs_data:
        return 0, 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO public.jobs (url, title, company, applywizz_id, company_email)
                VALUES %s
                ON CONFLICT (url, applywizz_id) DO NOTHING
                RETURNING id;
            """
            
            values = [
                (j['url'], j.get('title'), j.get('company'), j['applywizz_id'], j.get('company_email'))
                for j in jobs_data
            ]
            
            # Using RETURNING with execute_values to get the count of inserted rows
            # is a bit tricky, but we can do it by checking the result of fetchall
            # Note: execute_values doesn't fetch, we must explicitly fetch if we use RETURNING
            # Alternatively, we can use standard parameterized execute if we need exact counts easily,
            # but execute_values is much faster for batches.
            # However, execute_values doesn't return the fetched rows by default unless we set fetch=True.
            
            inserted_rows = execute_values(cur, query, values, fetch=True)
            conn.commit()
            
            inserted = len(inserted_rows) if inserted_rows else 0
            skipped = len(jobs_data) - inserted
            return inserted, skipped
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
