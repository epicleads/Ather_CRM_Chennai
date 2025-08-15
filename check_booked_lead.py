#!/usr/bin/env python3
"""
Check the specific lead with Booked status to understand the approval workflow issue
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
    
    # Check the specific lead with Booked status
    print("🔍 Examining lead with Booked status...")
    
    result = supabase.table('ps_followup_master').select('*').eq('lead_uid', 'MI-7258-0009').execute()
    
    if result.data:
        lead = result.data[0]
        print(f"Lead Details:")
        print(f"  Lead UID: {lead.get('lead_uid')}")
        print(f"  Customer: {lead.get('customer_name')}")
        print(f"  Lead Status: {lead.get('lead_status')}")
        print(f"  Final Status: {lead.get('final_status')}")
        print(f"  Order ID: {lead.get('order_id')}")
        print(f"  Approval Status: {lead.get('approval_status')}")
        print(f"  Approval Requested At: {lead.get('approval_requested_at')}")
        print(f"  PS Name: {lead.get('ps_name')}")
        print(f"  PS Branch: {lead.get('ps_branch')}")
        print(f"  Created At: {lead.get('created_at')}")
        print(f"  Updated At: {lead.get('updated_at')}")
        
        # Check if this lead exists in lead_master
        lm_result = supabase.table('lead_master').select('*').eq('uid', 'MI-7258-0009').execute()
        if lm_result.data:
            lm_lead = lm_result.data[0]
            print(f"\nLead Master Details:")
            print(f"  Final Status: {lm_lead.get('final_status')}")
            print(f"  Approval Status: {lm_lead.get('approval_status')}")
            print(f"  Order ID: {lm_lead.get('order_id')}")
        else:
            print("\n❌ Lead not found in lead_master table")
            
    else:
        print("❌ Lead not found")
        
    # Check if there are any leads with "Waiting for Approval" status
    print(f"\n🔍 Checking for leads with 'Waiting for Approval' status...")
    waiting_result = supabase.table('ps_followup_master').select('*').eq('approval_status', 'Waiting for Approval').execute()
    print(f"Leads waiting for approval: {len(waiting_result.data)}")
    
    if waiting_result.data:
        for lead in waiting_result.data:
            print(f"  - {lead.get('lead_uid')}: {lead.get('customer_name')} - Order ID: {lead.get('order_id')}")
    
    # Check if there are any leads with "Pending" approval status
    print(f"\n🔍 Checking for leads with 'Pending' approval status...")
    pending_result = supabase.table('ps_followup_master').select('*').eq('approval_status', 'Pending').execute()
    print(f"Leads with pending approval: {len(pending_result.data)}")
    
    if pending_result.data:
        for lead in pending_result.data:
            print(f"  - {lead.get('lead_uid')}: {lead.get('customer_name')} - Status: {lead.get('lead_status')} - Order ID: {lead.get('order_id')}")
    
except Exception as e:
    print(f"❌ Error: {e}")
