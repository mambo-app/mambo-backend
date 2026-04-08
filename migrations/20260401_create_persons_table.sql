-- Migration: Create persons and content_credits tables
-- These tables store actor/director info and their roles in movies/series.

CREATE TABLE IF NOT EXISTS persons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tmdb_id INTEGER UNIQUE,
    name TEXT NOT NULL,
    profile_image_url TEXT,
    known_for_department TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS content_credits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id UUID NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    person_id UUID NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    role TEXT NOT NULL, -- 'cast' or 'crew'
    character_name TEXT,
    job TEXT,
    department TEXT,
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE(content_id, person_id, role)
);

CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(name);
CREATE INDEX IF NOT EXISTS idx_content_credits_content_id ON content_credits(content_id);
CREATE INDEX IF NOT EXISTS idx_content_credits_person_id ON content_credits(person_id);
