#!/usr/bin/env python3
"""
Test script to check database connection and approval system tables
"""

import os
from dotenv import load_dotenv
from supabase.client import create_client, Client

# Load environment variables
load_dotenv()

# Get Supabase credentials
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_ANON_KEY')

print(f"SUPABASE_URL: {SUPABASE_URL}")
print(f"SUPABASE_KEY: {SUPABASE_KEY[:20]}..." if SUPABASE_KEY else "None")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Environment variables not loaded!")
    exit(1)

try:
    # Initialize Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Supabase client initialized successfully")
    
    # Test connection by checking if tables exist
    print("\n🔍 Checking database tables...")
    
    # Check ps_followup_master table structure
    try:
        result = supabase.table('ps_followup_master').select('*').limit(1).execute()
        print("✅ ps_followup_master table accessible")
        
        # Check if approval columns exist
        if result.data:
            sample_row = result.data[0]
            approval_fields = ['order_id', 'approval_status', 'approval_requested_at', 'approved_by', 'approved_at']
            missing_fields = []
            for field in approval_fields:
                if field not in sample_row:
                    missing_fields.append(field)
            
            if missing_fields:
                print(f"❌ Missing approval fields: {missing_fields}")
            else:
                print("✅ All approval fields present in ps_followup_master")
                
            # Check for leads waiting for approval
            approval_result = supabase.table('ps_followup_master').select('*').eq('approval_status', 'Waiting for Approval').execute()
            print(f"📊 Leads waiting for approval: {len(approval_result.data)}")
            
            if approval_result.data:
                for lead in approval_result.data[:3]:  # Show first 3
                    print(f"  - Lead {lead.get('lead_uid')}: {lead.get('customer_name')} - Order ID: {lead.get('order_id')}")
        else:
            print("⚠️  ps_followup_master table is empty")
            
    except Exception as e:
        print(f"❌ Error accessing ps_followup_master: {e}")
    
    # Check lead_master table
    try:
        result = supabase.table('lead_master').select('*').limit(1).execute()
        print("✅ lead_master table accessible")
        
        if result.data:
            sample_row = result.data[0]
            approval_fields = ['order_id', 'approval_status', 'approval_requested_at', 'approved_by', 'approved_at']
            missing_fields = []
            for field in approval_fields:
                if field not in sample_row:
                    missing_fields.append(field)
            
            if missing_fields:
                print(f"❌ Missing approval fields in lead_master: {missing_fields}")
            else:
                print("✅ All approval fields present in lead_master")
                
    except Exception as e:
        print(f"❌ Error accessing lead_master: {e}")
    
    # Check if branch_approval_leads view exists
    try:
        result = supabase.rpc('get_branch_approval_leads', {'branch_name': 'PORUR'}).execute()
        print("✅ branch_approval_leads view accessible")
    except Exception as e:
        print(f"❌ Error accessing branch_approval_leads view: {e}")
        print("   This view might not exist yet - need to run approval_system_sql.sql")
    
    print("\n🔍 Checking for any leads with Booked/Retailed status...")
    try:
        booked_result = supabase.table('ps_followup_master').select('*').in_('lead_status', ['Booked', 'Retailed']).execute()
        print(f"📊 Leads with Booked/Retailed status: {len(booked_result.data)}")
        
        if booked_result.data:
            for lead in booked_result.data[:3]:
                print(f"  - Lead {lead.get('lead_uid')}: {lead.get('customer_name')} - Status: {lead.get('lead_status')} - Order ID: {lead.get('order_id')} - Approval: {lead.get('approval_status')}")
        else:
            print("⚠️  No leads with Booked/Retailed status found")
            
    except Exception as e:
        print(f"❌ Error checking Booked/Retailed leads: {e}")
    
except Exception as e:
    print(f"❌ Error initializing Supabase client: {e}")
    exit(1)

print("\n✅ Database connection test completed!")
