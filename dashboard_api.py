from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime

from database import get_connection
from relevance_service import analyze_match
from api_client import get_client_details, extract_client_requirements

app = FastAPI(title="Dice Scraper Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ReviewPayload(BaseModel):
    status: str
    reason: Optional[str] = None
    comment: Optional[str] = None

@app.get("/admin/dice/relevance/summary")
def get_summary():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Jobs in last 48 hours
            cur.execute("""
                SELECT COUNT(*) as total_jobs
                FROM public.dice_scraped_jobs
                WHERE scraped_at >= NOW() - INTERVAL '2 days'
            """)
            total_jobs = cur.fetchone()['total_jobs']
            
            cur.execute("""
                SELECT review_status, COUNT(*) as count
                FROM public.dice_job_relevance_reviews r
                JOIN public.dice_scraped_jobs j ON r.job_url = j.url AND r.applywizz_id = j.applywizz_id
                WHERE j.scraped_at >= NOW() - INTERVAL '2 days'
                GROUP BY review_status
            """)
            
            counts = {row['review_status']: row['count'] for row in cur.fetchall()}
            
            relevant = counts.get('relevant', 0)
            irrelevant = counts.get('irrelevant', 0)
            needs_review = counts.get('needs_review', 0)
            reviewed_total = relevant + irrelevant + needs_review
            pending = total_jobs - reviewed_total
            
            relevance_rate = round((relevant / reviewed_total * 100), 1) if reviewed_total > 0 else 0
            
            return {
                "total_jobs": total_jobs,
                "pending": pending,
                "relevant": relevant,
                "irrelevant": irrelevant,
                "needs_review": needs_review,
                "relevance_rate": relevance_rate
            }
    finally:
        conn.close()

@app.get("/admin/dice/relevance/jobs")
def get_jobs(
    page: int = 1, 
    limit: int = 50,
    status: Optional[str] = None,
    applywizz_id: Optional[str] = None
):
    offset = (page - 1) * limit
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT j.id, j.title, j.company, j.location, j.scraped_at, j.applywizz_id,
                       r.review_status, r.system_relevance_score
                FROM public.dice_scraped_jobs j
                LEFT JOIN public.dice_job_relevance_reviews r 
                  ON j.url = r.job_url AND j.applywizz_id = r.applywizz_id
                WHERE j.scraped_at >= NOW() - INTERVAL '2 days'
            """
            params = []
            
            if applywizz_id:
                query += " AND j.applywizz_id = %s"
                params.append(applywizz_id)
                
            if status:
                if status == 'pending':
                    query += " AND r.review_status IS NULL"
                else:
                    query += " AND r.review_status = %s"
                    params.append(status)
                    
            query += " ORDER BY j.scraped_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cur.execute(query, params)
            jobs = cur.fetchall()
            
            # Count total for pagination
            count_query = """
                SELECT COUNT(*) as total
                FROM public.dice_scraped_jobs j
                LEFT JOIN public.dice_job_relevance_reviews r 
                  ON j.url = r.job_url AND j.applywizz_id = r.applywizz_id
                WHERE j.scraped_at >= NOW() - INTERVAL '2 days'
            """
            c_params = params[:-2]
            if applywizz_id:
                count_query += " AND j.applywizz_id = %s"
            if status:
                if status == 'pending':
                    count_query += " AND r.review_status IS NULL"
                else:
                    count_query += " AND r.review_status = %s"
                    
            cur.execute(count_query, c_params)
            total = cur.fetchone()['total']
            
            return {
                "data": jobs,
                "pagination": {
                    "total": total,
                    "page": page,
                    "limit": limit
                }
            }
    finally:
        conn.close()

@app.get("/admin/dice/relevance/jobs/{job_id}")
def get_job_detail(job_id: int):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT j.*, 
                       r.review_status, r.review_reason, r.review_comment
                FROM public.dice_scraped_jobs j
                LEFT JOIN public.dice_job_relevance_reviews r 
                  ON j.url = r.job_url AND j.applywizz_id = r.applywizz_id
                WHERE j.id = %s
            """, (job_id,))
            job = cur.fetchone()
            
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
                
            # Fetch client requirement
            try:
                client_details = get_client_details(job['applywizz_id'])
                client_req = extract_client_requirements(client_details)
            except Exception as e:
                client_req = {"error": f"Failed to fetch client reqs: {e}"}
                
            # Match analysis
            match_analysis = {}
            if "error" not in client_req:
                match_analysis = analyze_match(client_req, job)
                
            return {
                "job": job,
                "client_requirement": client_req,
                "match_analysis": match_analysis,
                "scrape_trace": {
                    "search_keyword": job.get('search_keyword'),
                    "search_location": job.get('search_location'),
                    "applywizz_id": job.get('applywizz_id')
                },
                "review": {
                    "status": job.get('review_status'),
                    "reason": job.get('review_reason'),
                    "comment": job.get('review_comment')
                }
            }
    finally:
        conn.close()

@app.post("/admin/dice/relevance/jobs/{job_id}/review")
def submit_review(job_id: int, payload: ReviewPayload):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT url, applywizz_id FROM public.dice_scraped_jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
                
            # Upsert review
            cur.execute("""
                INSERT INTO public.dice_job_relevance_reviews 
                (job_url, applywizz_id, review_status, review_reason, review_comment, reviewed_by, reviewed_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'human_reviewer', NOW(), NOW())
                ON CONFLICT (job_url, applywizz_id) DO UPDATE SET
                review_status = EXCLUDED.review_status,
                review_reason = EXCLUDED.review_reason,
                review_comment = EXCLUDED.review_comment,
                updated_at = NOW(),
                reviewed_at = NOW()
            """, (job['url'], job['applywizz_id'], payload.status, payload.reason, payload.comment))
            
            conn.commit()
            return {"success": True}
    finally:
        conn.close()

@app.get("/admin/dice/relevance/analytics")
def get_analytics():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Top irrelevance reasons
            cur.execute("""
                SELECT review_reason, COUNT(*) as count
                FROM public.dice_job_relevance_reviews r
                JOIN public.dice_scraped_jobs j ON r.job_url = j.url AND r.applywizz_id = j.applywizz_id
                WHERE review_status = 'irrelevant' AND j.scraped_at >= NOW() - INTERVAL '2 days'
                GROUP BY review_reason
                ORDER BY count DESC
            """)
            reasons = cur.fetchall()
            
            return {
                "top_irrelevance_reasons": reasons
            }
    finally:
        conn.close()
