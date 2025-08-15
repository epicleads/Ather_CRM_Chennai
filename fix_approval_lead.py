#!/usr/bin/env python3
"""
Fix the approval status and order_id mismatch for the lead with Booked status
"""

import os
from dotenv import load_dotenv
from supabase.client import create_client, Client

# Load environment variables
load_dotenv()

# Get Supabase credentials
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_ANON_KEY')

try:
    # Initialize Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    lead_uid = 'MI-7258-0009'
    
    print(f"🔧 Fixing approval status and order_id for lead {lead_uid}...")
    
    # First, get the current data from both tables
    ps_result = supabase.table('ps_followup_master').select('*').eq('lead_uid', lead_uid).execute()
    lm_result = supabase.table('lead_master').select('*').eq('uid', lead_uid).execute()
    
    if not ps_result.data or not lm_result.data:
        print("❌ Lead not found in one or both tables")
        exit(1)
    
    ps_lead = ps_result.data[0]
    lm_lead = lm_result.data[0]
    
    print(f"Current state:")
    print(f"  PS Followup - Approval Status: {ps_lead.get('approval_status')}, Order ID: {ps_lead.get('order_id')}")
    print(f"  Lead Master - Approval Status: {lm_lead.get('approval_status')}, Order ID: {lm_lead.get('order_id')}")
    
    # Update ps_followup_master to match lead_master
    update_data = {
        'approval_status': 'Waiting for Approval',
        'order_id': lm_lead.get('order_id'),
        'approval_requested_at': ps_lead.get('approval_requested_at')  # Keep existing timestamp
    }
    
    print(f"\nUpdating ps_followup_master with: {update_data}")
    
    result = supabase.table('ps_followup_master').update(update_data).eq('lead_uid', lead_uid).execute()
    
    if result.data:
        print("✅ Successfully updated ps_followup_master")
        
        # Verify the update
        updated_result = supabase.table('ps_followup_master').select('*').eq('lead_uid', lead_uid).execute()
        if updated_result.data:
            updated_lead = updated_result.data[0]
            print(f"Updated state:")
            print(f"  PS Followup - Approval Status: {updated_lead.get('approval_status')}, Order ID: {updated_lead.get('order_id')}")
            
            # Now check if the lead appears in approval leads
            approval_result = supabase.table('ps_followup_master').select('*').eq('approval_status', 'Waiting for Approval').execute()
            print(f"\n📊 Total leads waiting for approval: {len(approval_result.data)}")
            
            if approval_result.data:
                for lead in approval_result.data:
                    print(f"  - {lead.get('lead_uid')}: {lead.get('customer_name')} - Order ID: {lead.get('order_id')}")
        else:
            print("❌ Failed to verify update")
    else:
        print("❌ Failed to update ps_followup_master")
        
except Exception as e:
    print(f"❌ Error: {e}")
