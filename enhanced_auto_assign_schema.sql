-- Enhanced Auto-Assign System Database Schema
-- This file contains all the necessary database changes for the auto-assign system

-- =============================================================================
-- 1. ADD AUTO_ASSIGN_COUNT COLUMN TO CRE_USERS TABLE
-- =============================================================================

-- Add auto_assign_count column to track fair distribution
ALTER TABLE cre_users 
ADD COLUMN IF NOT EXISTS auto_assign_count INTEGER DEFAULT 0;

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_cre_users_auto_assign_count 
ON cre_users(auto_assign_count);

-- =============================================================================
-- 2. CREATE AUTO_ASSIGN_CONFIG TABLE
-- =============================================================================

-- Table to store auto-assign configurations for each source
CREATE TABLE IF NOT EXISTS auto_assign_config (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    cre_id INTEGER NOT NULL REFERENCES cre_users(id) ON DELETE CASCADE,
    is_active BOOLEAN DEFAULT TRUE,
    priority INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(source, cre_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_auto_assign_config_source 
ON auto_assign_config(source);

CREATE INDEX IF NOT EXISTS idx_auto_assign_config_cre_id 
ON auto_assign_config(cre_id);

CREATE INDEX IF NOT EXISTS idx_auto_assign_config_active 
ON auto_assign_config(is_active);

-- =============================================================================
-- 3. CREATE AUTO_ASSIGN_HISTORY TABLE
-- =============================================================================

-- Table to track all auto-assignments for audit and analytics
CREATE TABLE IF NOT EXISTS auto_assign_history (
    id SERIAL PRIMARY KEY,
    lead_uid VARCHAR(255) NOT NULL,
    source VARCHAR(100) NOT NULL,
    assigned_cre_id INTEGER NOT NULL REFERENCES cre_users(id),
    assigned_cre_name VARCHAR(255) NOT NULL,
    cre_total_leads_before INTEGER NOT NULL,
    cre_total_leads_after INTEGER NOT NULL,
    assignment_method VARCHAR(100) DEFAULT 'fair_distribution',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_auto_assign_history_lead_uid 
ON auto_assign_history(lead_uid);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_source 
ON auto_assign_history(source);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_cre_id 
ON auto_assign_history(assigned_cre_id);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_created_at 
ON auto_assign_history(created_at);

-- =============================================================================
-- 4. CREATE CRE_CALL_ATTEMPT_HISTORY TABLE
-- =============================================================================

-- Table to track call attempts for leads
CREATE TABLE IF NOT EXISTS cre_call_attempt_history (
    id SERIAL PRIMARY KEY,
    uid VARCHAR(255) NOT NULL,
    call_no VARCHAR(50) NOT NULL,
    attempt INTEGER NOT NULL,
    status VARCHAR(100) DEFAULT 'Pending',
    cre_name VARCHAR(255) NOT NULL,
    call_was_recorded BOOLEAN DEFAULT FALSE,
    follow_up_date DATE,
    remarks TEXT,
    final_status VARCHAR(100) DEFAULT 'Pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_uid 
ON cre_call_attempt_history(uid);

CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_cre_name 
ON cre_call_attempt_history(cre_name);

CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_status 
ON cre_call_attempt_history(status);

-- =============================================================================
-- 5. CREATE AUTO_ASSIGN_STATS VIEW
-- =============================================================================

-- View for easy access to auto-assign statistics
CREATE OR REPLACE VIEW auto_assign_stats AS
SELECT 
    c.id as cre_id,
    c.name as cre_name,
    c.auto_assign_count,
    COUNT(aah.id) as total_auto_assigned,
    COUNT(CASE WHEN aah.created_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as assigned_last_24h,
    COUNT(CASE WHEN aah.created_at >= NOW() - INTERVAL '7 days' THEN 1 END) as assigned_last_7d,
    COUNT(CASE WHEN aah.created_at >= NOW() - INTERVAL '30 days' THEN 1 END) as assigned_last_30d
FROM cre_users c
LEFT JOIN auto_assign_history aah ON c.id = aah.assigned_cre_id
GROUP BY c.id, c.name, c.auto_assign_count
ORDER BY c.auto_assign_count ASC;

-- =============================================================================
-- 6. CREATE SOURCE_ASSIGNMENT_STATS VIEW
-- =============================================================================

-- View for source-specific assignment statistics
CREATE OR REPLACE VIEW source_assignment_stats AS
SELECT 
    source,
    COUNT(*) as total_assignments,
    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as assignments_last_24h,
    COUNT(CASE WHEN created_at >= NOW() - INTERVAL '7 days' THEN 1 END) as assignments_last_7d,
    COUNT(DISTINCT assigned_cre_id) as unique_cres_assigned,
    AVG(cre_total_leads_after - cre_total_leads_before) as avg_leads_per_assignment
FROM auto_assign_history
GROUP BY source
ORDER BY total_assignments DESC;

-- =============================================================================
-- 7. CREATE TRIGGER FOR UPDATED_AT COLUMN
-- =============================================================================

-- Function to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for auto_assign_config table
DROP TRIGGER IF EXISTS update_auto_assign_config_updated_at ON auto_assign_config;
CREATE TRIGGER update_auto_assign_config_updated_at 
    BEFORE UPDATE ON auto_assign_config 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for cre_call_attempt_history table
DROP TRIGGER IF EXISTS update_cre_call_attempt_history_updated_at ON cre_call_attempt_history;
CREATE TRIGGER update_cre_call_attempt_history_updated_at 
    BEFORE UPDATE ON cre_call_attempt_history 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- 8. CLEANUP AND MAINTENANCE FUNCTIONS
-- =============================================================================

-- Function to cleanup old auto-assign history (older than 90 days)
CREATE OR REPLACE FUNCTION cleanup_old_auto_assign_history()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM auto_assign_history
    WHERE created_at < NOW() - INTERVAL '90 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get auto-assign statistics summary
CREATE OR REPLACE FUNCTION get_auto_assign_summary()
RETURNS TABLE(
    total_assignments BIGINT,
    assignments_today BIGINT,
    assignments_this_week BIGINT,
    active_sources BIGINT,
    active_cres BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_assignments,
        COUNT(CASE WHEN created_at >= CURRENT_DATE THEN 1 END) as assignments_today,
        COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as assignments_this_week,
        COUNT(DISTINCT source) as active_sources,
        COUNT(DISTINCT assigned_cre_id) as active_cres
    FROM auto_assign_history;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 9. GRANT PERMISSIONS (ADJUST AS NEEDED)
-- =============================================================================

-- Grant necessary permissions to your application user
-- Uncomment and modify the following lines based on your setup:

-- GRANT SELECT, INSERT, UPDATE, DELETE ON auto_assign_config TO your_app_user;
-- GRANT SELECT, INSERT ON auto_assign_history TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE ON cre_call_attempt_history TO your_app_user;
-- GRANT SELECT ON auto_assign_stats TO your_app_user;
-- GRANT SELECT ON source_assignment_stats TO your_app_user;
-- GRANT USAGE ON SEQUENCE auto_assign_config_id_seq TO your_app_user;
-- GRANT USAGE ON SEQUENCE auto_assign_history_id_seq TO your_app_user;
-- GRANT USAGE ON SEQUENCE cre_call_attempt_history_id_seq TO your_app_user;

-- =============================================================================
-- 10. VERIFICATION QUERIES
-- =============================================================================

-- Use these queries to verify the setup:

-- Check if tables exist:
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('auto_assign_config', 'auto_assign_history', 'cre_call_attempt_history');

-- Check table structure:
-- \d auto_assign_config
-- \d auto_assign_history
-- \d cre_call_attempt_history

-- Check if views exist:
-- SELECT viewname FROM pg_views WHERE schemaname = 'public' AND viewname IN ('auto_assign_stats', 'source_assignment_stats');

-- Check if functions exist:
-- SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public' AND routine_name IN ('update_updated_at_column', 'cleanup_old_auto_assign_history', 'get_auto_assign_summary');

-- Check if triggers exist:
-- SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = 'public';

-- Check auto-assign configuration:
-- SELECT * FROM auto_assign_config ORDER BY source, priority;

-- Check auto-assign history:
-- SELECT COUNT(*) as auto_assign_history_count FROM auto_assign_history;

-- Check CRE auto-assign counts:
-- SELECT name, auto_assign_count FROM cre_users ORDER BY auto_assign_count ASC;


