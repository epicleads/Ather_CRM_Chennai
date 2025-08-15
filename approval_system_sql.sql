-- SQL Script for Lead Approval System
-- This script adds the necessary fields to implement double verification for leads

-- 1. Add approval fields to ps_followup_master table
ALTER TABLE ps_followup_master 
ADD COLUMN IF NOT EXISTS order_id VARCHAR(8),
ADD COLUMN IF NOT EXISTS approval_status VARCHAR(50) DEFAULT 'Pending',
ADD COLUMN IF NOT EXISTS approval_requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS approved_by VARCHAR(100),
ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS approval_remarks TEXT;

-- 2. Add approval fields to lead_master table (for consistency)
ALTER TABLE lead_master 
ADD COLUMN IF NOT EXISTS order_id VARCHAR(8),
ADD COLUMN IF NOT EXISTS approval_status VARCHAR(50) DEFAULT 'Pending',
ADD COLUMN IF NOT EXISTS approval_requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS approved_by VARCHAR(100),
ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS approval_remarks TEXT;

-- 3. Add approval fields to activity_leads table (for event leads)
ALTER TABLE activity_leads 
ADD COLUMN IF NOT EXISTS order_id VARCHAR(8),
ADD COLUMN IF NOT EXISTS approval_status VARCHAR(50) DEFAULT 'Pending',
ADD COLUMN IF NOT EXISTS approval_requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS approved_by VARCHAR(100),
ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS approval_remarks TEXT;

-- 4. Add approval fields to walkin_table (for walk-in leads)
ALTER TABLE walkin_table 
ADD COLUMN IF NOT EXISTS order_id VARCHAR(8),
ADD COLUMN IF NOT EXISTS approval_status VARCHAR(50) DEFAULT 'Pending',
ADD COLUMN IF NOT EXISTS approval_requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS approved_by VARCHAR(100),
ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS approval_remarks TEXT;

-- 5. Create indexes for better performance on approval queries
CREATE INDEX IF NOT EXISTS idx_ps_followup_approval_status ON ps_followup_master(approval_status);
CREATE INDEX IF NOT EXISTS idx_ps_followup_ps_branch ON ps_followup_master(ps_branch);
CREATE INDEX IF NOT EXISTS idx_lead_master_approval_status ON lead_master(approval_status);
CREATE INDEX IF NOT EXISTS idx_activity_leads_approval_status ON activity_leads(approval_status);
CREATE INDEX IF NOT EXISTS idx_walkin_table_approval_status ON walkin_table(approval_status);

-- 6. Add constraints for order_id (8 digits only)
ALTER TABLE ps_followup_master 
ADD CONSTRAINT chk_order_id_format CHECK (order_id ~ '^[0-9]{8}$');

ALTER TABLE lead_master 
ADD CONSTRAINT chk_order_id_format CHECK (order_id ~ '^[0-9]{8}$');

ALTER TABLE activity_leads 
ADD CONSTRAINT chk_order_id_format CHECK (order_id ~ '^[0-9]{8}$');

ALTER TABLE walkin_table 
ADD CONSTRAINT chk_order_id_format CHECK (order_id ~ '^[0-9]{8}$');

-- 7. Update existing leads with 'Won' status to have 'Approved' approval status
UPDATE ps_followup_master 
SET approval_status = 'Approved', 
    approved_at = COALESCE(won_timestamp, NOW()),
    approved_by = 'System Migration'
WHERE final_status = 'Won';

UPDATE lead_master 
SET approval_status = 'Approved', 
    approved_at = COALESCE(won_timestamp, NOW()),
    approved_by = 'System Migration'
WHERE final_status = 'Won';

UPDATE activity_leads 
SET approval_status = 'Approved', 
    approved_at = COALESCE(won_timestamp, NOW()),
    approved_by = 'System Migration'
WHERE final_status = 'Won';

UPDATE walkin_table 
SET approval_status = 'Approved', 
    approved_at = COALESCE(won_timestamp, NOW()),
    approved_by = 'System Migration'
WHERE status = 'Won';

-- 8. Create a view for Branch Head to see all pending approvals
CREATE OR REPLACE VIEW branch_approval_leads AS
SELECT 
    'ps_followup' as source_table,
    lead_uid as lead_id,
    customer_name,
    customer_mobile_number,
    source,
    lead_status,
    final_status,
    order_id,
    approval_status,
    approval_requested_at,
    ps_name,
    ps_branch as branch,
    created_at
FROM ps_followup_master 
WHERE approval_status = 'Waiting for Approval'

UNION ALL

SELECT 
    'activity_leads' as source_table,
    activity_uid as lead_id,
    customer_name,
    customer_mobile_number,
    source,
    lead_status,
    final_status,
    order_id,
    approval_status,
    approval_requested_at,
    ps_name,
    location as branch,
    created_at
FROM activity_leads 
WHERE approval_status = 'Waiting for Approval'

UNION ALL

SELECT 
    'walkin_table' as source_table,
    uid as lead_id,
    customer_name,
    customer_mobile_number,
    source,
    status as lead_status,
    status as final_status,
    order_id,
    approval_status,
    approval_requested_at,
    ps_assigned as ps_name,
    branch,
    created_at
FROM walkin_table 
WHERE approval_status = 'Waiting for Approval';

-- 9. Grant necessary permissions (adjust according to your database setup)
-- GRANT SELECT ON branch_approval_leads TO authenticated;
-- GRANT UPDATE ON ps_followup_master TO authenticated;
-- GRANT UPDATE ON lead_master TO authenticated;
-- GRANT UPDATE ON activity_leads TO authenticated;
-- GRANT UPDATE ON walkin_table TO authenticated;

-- 10. Add comments for documentation
COMMENT ON COLUMN ps_followup_master.order_id IS '8-digit order ID required for Booked/Retailed leads';
COMMENT ON COLUMN ps_followup_master.approval_status IS 'Status: Pending, Waiting for Approval, Approved, Rejected';
COMMENT ON COLUMN ps_followup_master.approval_requested_at IS 'When the approval was requested';
COMMENT ON COLUMN ps_followup_master.approved_by IS 'Branch Head who approved the lead';
COMMENT ON COLUMN ps_followup_master.approved_at IS 'When the lead was approved';

COMMENT ON VIEW branch_approval_leads IS 'View for Branch Head to see all leads waiting for approval in their branch';
