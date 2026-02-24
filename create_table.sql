-- Supabase SQL: Create dice_jobs table
-- Run this in Supabase SQL Editor (Dashboard → SQL Editor → New query)

CREATE TABLE IF NOT EXISTS dice_jobs (
    id                  BIGSERIAL PRIMARY KEY,
    job_id              TEXT UNIQUE NOT NULL,
    search_term         TEXT,
    title               TEXT NOT NULL,
    company_name        TEXT,
    company_logo_url    TEXT,
    location            TEXT,
    is_remote           BOOLEAN DEFAULT FALSE,
    job_type            TEXT,
    w2_c2c_type         TEXT,
    employment_type     TEXT,
    date_posted         TEXT,
    job_url             TEXT,
    apply_url           TEXT,
    compensation        TEXT,
    experience          TEXT,
    skills              TEXT,
    description         TEXT,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_dice_jobs_job_id ON dice_jobs(job_id);
CREATE INDEX IF NOT EXISTS idx_dice_jobs_search_term ON dice_jobs(search_term);
CREATE INDEX IF NOT EXISTS idx_dice_jobs_w2_c2c_type ON dice_jobs(w2_c2c_type);
CREATE INDEX IF NOT EXISTS idx_dice_jobs_scraped_at ON dice_jobs(scraped_at);
