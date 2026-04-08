-- Add username_updated_at column to profiles table
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS username_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;

-- For existing users, let's keep it NULL so they can change it once immediately.
