-- Migration: Add user_person_favorites table
-- This table stores a user's favorite actors and directors.

CREATE TABLE IF NOT EXISTS user_person_favorites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    person_id TEXT NOT NULL, 
    name TEXT NOT NULL,
    profile_url TEXT,
    is_actor BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE(user_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_user_person_favorites_user_id ON user_person_favorites(user_id);
CREATE INDEX IF NOT EXISTS idx_user_person_favorites_person_id ON user_person_favorites(person_id);
