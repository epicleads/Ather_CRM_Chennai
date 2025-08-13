-- Update Auto-Assign Schema Script
-- Run this script to add missing columns to your existing auto_assign_config table

-- =============================================================================
-- 1. ADD MISSING COLUMNS TO AUTO_ASSIGN_CONFIG TABLE
-- =============================================================================

-- Add is_active column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'auto_assign_config' 
        AND column_name = 'is_active'
    ) THEN
        ALTER TABLE auto_assign_config ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
        RAISE NOTICE 'Added is_active column to auto_assign_config table';
    ELSE
        RAISE NOTICE 'is_active column already exists in auto_assign_config table';
    END IF;
END $$;

-- Add priority column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'auto_assign_config' 
        AND column_name = 'priority'
    ) THEN
        ALTER TABLE auto_assign_config ADD COLUMN priority INTEGER DEFAULT 1;
        RAISE NOTICE 'Added priority column to auto_assign_config table';
    ELSE
        RAISE NOTICE 'priority column already exists in auto_assign_config table';
    END IF;
END $$;

-- =============================================================================
-- 2. CREATE MISSING TABLES
-- =============================================================================

-- Create auto_assign_history table if it doesn't exist
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

-- Create cre_call_attempt_history table if it doesn't exist
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

-- =============================================================================
-- 3. CREATE INDEXES FOR PERFORMANCE
-- =============================================================================

-- Indexes for auto_assign_config
CREATE INDEX IF NOT EXISTS idx_auto_assign_config_active 
ON auto_assign_config(is_active);

-- Indexes for auto_assign_history
CREATE INDEX IF NOT EXISTS idx_auto_assign_history_lead_uid 
ON auto_assign_history(lead_uid);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_source 
ON auto_assign_history(source);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_cre_id 
ON auto_assign_history(assigned_cre_id);

CREATE INDEX IF NOT EXISTS idx_auto_assign_history_created_at 
ON auto_assign_history(created_at);

-- Indexes for cre_call_attempt_history
CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_uid 
ON cre_call_attempt_history(uid);

CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_cre_name 
ON cre_call_attempt_history(cre_name);

CREATE INDEX IF NOT EXISTS idx_cre_call_attempt_history_status 
ON cre_call_attempt_history(status);

-- =============================================================================
-- 4. CREATE TRIGGER FUNCTION
-- =============================================================================

-- Function to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- =============================================================================
-- 5. CREATE TRIGGERS
-- =============================================================================

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
-- 6. VERIFICATION QUERIES
-- =============================================================================

-- Check if columns were added successfully
SELECT 
    column_name, 
    data_type, 
    is_nullable, 
    column_default
FROM information_schema.columns 
WHERE table_name = 'auto_assign_config' 
ORDER BY ordinal_position;

-- Check if tables were created successfully
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('auto_assign_config', 'auto_assign_history', 'cre_call_attempt_history')
ORDER BY table_name;

-- Check if indexes were created successfully
SELECT 
    indexname, 
    tablename, 
    indexdef
FROM pg_indexes 
WHERE tablename IN ('auto_assign_config', 'auto_assign_history', 'cre_call_attempt_history')
ORDER BY tablename, indexname;

-- Check if triggers were created successfully
SELECT 
    trigger_name, 
    event_object_table, 
    action_statement
FROM information_schema.triggers 
WHERE trigger_schema = 'public'
AND event_object_table IN ('auto_assign_config', 'cre_call_attempt_history')
ORDER BY event_object_table, trigger_name;

-- =============================================================================
-- 7. UPDATE EXISTING DATA (OPTIONAL)
-- =============================================================================

-- Update existing records to have proper values for new columns
UPDATE auto_assign_config 
SET 
    is_active = TRUE,
    priority = 1,
    updated_at = NOW()
WHERE is_active IS NULL OR priority IS NULL;

-- =============================================================================
-- 8. FINAL VERIFICATION
-- =============================================================================

-- Verify the complete setup
SELECT 'Schema Update Complete!' as status;

-- Show current auto_assign_config data
SELECT 
    id,
    source,
    cre_id,
    is_active,
    priority,
    created_at,
    updated_at
FROM auto_assign_config 
ORDER BY source, priority;

-- Count records in each table
SELECT 
    'auto_assign_config' as table_name,
    COUNT(*) as record_count
FROM auto_assign_config
UNION ALL
SELECT 
    'auto_assign_history' as table_name,
    COUNT(*) as record_count
FROM auto_assign_history
UNION ALL
SELECT 
    'cre_call_attempt_history' as table_name,
    COUNT(*) as record_count
FROM cre_call_attempt_history;
