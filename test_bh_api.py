#!/usr/bin/env python3
"""
Test the Branch Head approval leads API endpoint
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
    
    print("🔍 Testing Branch Head approval leads query logic...")
    
    # Simulate the API logic from app.py
    branch = 'PORUR'  # Hardcoded for testing
    approval_leads = []
    
    # 1. PS Followup leads waiting for approval
    try:
        ps_leads = supabase.table('ps_followup_master').select('*').eq('ps_branch', branch).eq('approval_status', 'Waiting for Approval').execute()
        if ps_leads.data:
            for lead in ps_leads.data:
                approval_leads.append({
                    'source_table': 'ps_followup',
                    'lead_id': lead['lead_uid'],
                    'customer_name': lead.get('customer_name'),
                    'customer_mobile_number': lead.get('customer_mobile_number'),
                    'source': lead.get('source'),
                    'lead_status': lead.get('lead_status'),
                    'final_status': lead.get('final_status'),
                    'order_id': lead.get('order_id'),
                    'approval_status': lead.get('approval_status'),
                    'approval_requested_at': lead.get('approval_requested_at'),
                    'ps_name': lead.get('ps_name'),
                    'ps_branch': lead.get('ps_branch')
                })
        print(f"✅ PS Followup leads found: {len(ps_leads.data) if ps_leads.data else 0}")
    except Exception as e:
        print(f"❌ PS Followup query error: {e}")

    # 2. Activity leads waiting for approval
    try:
        activity_leads = supabase.table('activity_leads').select('*').eq('location', branch).eq('approval_status', 'Waiting for Approval').execute()
        if activity_leads.data:
            for lead in activity_leads.data:
                approval_leads.append({
                    'source_table': 'activity_leads',
                    'lead_id': lead['activity_uid'],
                    'customer_name': lead.get('customer_name'),
                    'customer_mobile_number': lead.get('customer_mobile_number'),
                    'source': lead.get('source'),
                    'lead_status': lead.get('lead_status'),
                    'final_status': lead.get('final_status'),
                    'order_id': lead.get('order_id'),
                    'approval_status': lead.get('approval_status'),
                    'approval_requested_at': lead.get('approval_requested_at'),
                    'ps_name': lead.get('ps_name'),
                    'ps_branch': lead.get('location')
                })
        print(f"✅ Activity leads found: {len(activity_leads.data) if activity_leads.data else 0}")
    except Exception as e:
        print(f"❌ Activity leads query error: {e}")

    # 3. Walk-in leads waiting for approval
    try:
        walkin_leads = supabase.table('walkin_table').select('*').eq('branch', branch).eq('approval_status', 'Waiting for Approval').execute()
        if walkin_leads.data:
            for lead in walkin_leads.data:
                approval_leads.append({
                    'source_table': 'walkin_table',
                    'lead_id': lead['uid'],
                    'customer_name': lead.get('customer_name'),
                    'customer_mobile_number': lead.get('customer_mobile_number'),
                    'source': lead.get('source'),
                    'lead_status': lead.get('status'),
                    'final_status': lead.get('status'),
                    'order_id': lead.get('order_id'),
                    'approval_status': lead.get('approval_status'),
                    'approval_requested_at': lead.get('approval_requested_at'),
                    'ps_name': lead.get('ps_assigned'),
                    'ps_branch': lead.get('branch')
                })
        print(f"✅ Walk-in leads found: {len(walkin_leads.data) if walkin_leads.data else 0}")
    except Exception as e:
        print(f"❌ Walk-in leads query error: {e}")

    # 4. Fallback: lead_master
    try:
        lm_leads = supabase.table('lead_master').select('*').eq('branch', branch).eq('final_status', 'Waiting for Approval').execute()
        if lm_leads.data:
            for lead in lm_leads.data:
                approval_leads.append({
                    'source_table': 'lead_master',
                    'lead_id': lead['uid'],
                    'customer_name': lead.get('customer_name'),
                    'customer_mobile_number': lead.get('customer_mobile_number'),
                    'source': lead.get('source'),
                    'lead_status': lead.get('lead_status'),
                    'final_status': lead.get('final_status'),
                    'order_id': lead.get('order_id'),
                    'approval_status': lead.get('approval_status'),
                    'approval_requested_at': lead.get('approval_requested_at'),
                    'ps_name': lead.get('ps_name'),
                    'ps_branch': lead.get('branch')
                })
        print(f"✅ Lead Master leads found: {len(lm_leads.data) if lm_leads.data else 0}")
    except Exception as e:
        print(f"❌ Lead Master query error: {e}")
    
    # Sort by approval requested date (newest first)
    approval_leads.sort(key=lambda x: x['approval_requested_at'] or '', reverse=True)
    
    print(f"\n📊 Total approval leads found: {len(approval_leads)}")
    
    if approval_leads:
        print("\nApproval leads details:")
        for lead in approval_leads:
            print(f"  - {lead['source_table']}: {lead['lead_id']} - {lead['customer_name']} - Order ID: {lead['order_id']} - Status: {lead['lead_status']}")
    
    # Test the specific query that the API uses
    print(f"\n🔍 Testing specific query for PORUR branch...")
    try:
        result = supabase.table('ps_followup_master').select('*').eq('ps_branch', 'PORUR').eq('approval_status', 'Waiting for Approval').execute()
        print(f"Direct query result: {len(result.data) if result.data else 0} leads")
        if result.data:
            for lead in result.data:
                print(f"  - {lead.get('lead_uid')}: {lead.get('customer_name')} - Order ID: {lead.get('order_id')}")
    except Exception as e:
        print(f"❌ Direct query error: {e}")
        
except Exception as e:
    print(f"❌ Error: {e}")
