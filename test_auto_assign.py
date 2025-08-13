#!/usr/bin/env python3
"""
Test script to verify auto-assignment configuration and functionality
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

def test_auto_assign_config():
    """Test auto-assignment configuration"""
    try:
        # Initialize Supabase client
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        print("🔍 Testing Auto-Assignment Configuration...")
        print("=" * 60)
        
        # 1. Check if auto_assign_config table exists and has data
        print("\n1️⃣ Checking auto_assign_config table...")
        try:
            config_result = supabase.table('auto_assign_config').select('*').execute()
            configs = config_result.data or []
            
            if configs:
                print(f"✅ Found {len(configs)} auto-assign configurations:")
                for config in configs:
                    print(f"   📋 Source: {config['source']}, CRE ID: {config['cre_id']}, Active: {config['is_active']}, Priority: {config['priority']}")
            else:
                print("❌ No auto-assign configurations found!")
                print("   💡 You need to add configurations in the admin panel")
                return False
                
        except Exception as e:
            print(f"❌ Error accessing auto_assign_config table: {e}")
            return False
        
        # 2. Check if configured CREs exist
        print("\n2️⃣ Checking configured CREs...")
        try:
            cre_ids = [config['cre_id'] for config in configs if config['is_active']]
            if cre_ids:
                cre_result = supabase.table('cre_users').select('id, name, auto_assign_count').in_('id', cre_ids).execute()
                cres = cre_result.data or []
                
                if cres:
                    print(f"✅ Found {len(cres)} configured CREs:")
                    for cre in cres:
                        print(f"   👤 ID: {cre['id']}, Name: {cre['name']}, Auto-assign count: {cre.get('auto_assign_count', 0)}")
                else:
                    print("❌ Configured CREs not found in cre_users table!")
                    return False
            else:
                print("❌ No active CRE configurations found!")
                return False
                
        except Exception as e:
            print(f"❌ Error accessing cre_users table: {e}")
            return False
        
        # 3. Check for unassigned leads
        print("\n3️⃣ Checking for unassigned leads...")
        try:
            # Get all sources with configurations
            configured_sources = list(set([config['source'] for config in configs if config['is_active']]))
            
            for source in configured_sources:
                unassigned_result = supabase.table('lead_master').select('uid, customer_mobile_number, source').eq('assigned', 'No').eq('source', source).execute()
                unassigned_leads = unassigned_result.data or []
                
                print(f"   📱 {source}: {len(unassigned_leads)} unassigned leads")
                
                if unassigned_leads:
                    print(f"      Sample UIDs: {[lead['uid'] for lead in unassigned_leads[:3]]}")
        
        except Exception as e:
            print(f"❌ Error checking unassigned leads: {e}")
            return False
        
        # 4. Test auto-assignment function
        print("\n4️⃣ Testing auto-assignment function...")
        try:
            from knowlaritytosupabase import auto_assign_new_leads_for_source
            
            for source in configured_sources:
                print(f"\n   🔄 Testing auto-assignment for {source}...")
                result = auto_assign_new_leads_for_source(supabase, source)
                
                if result['success']:
                    print(f"      ✅ Success: {result['assigned_count']} leads assigned")
                else:
                    print(f"      ⚠️ Failed: {result['message']}")
                    
        except Exception as e:
            print(f"❌ Error testing auto-assignment function: {e}")
            return False
        
        print("\n" + "=" * 60)
        print("🎉 Auto-assignment configuration test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Critical error in test: {e}")
        return False

def show_manual_fix_instructions():
    """Show manual fix instructions"""
    print("\n" + "=" * 60)
    print("🔧 MANUAL FIX INSTRUCTIONS")
    print("=" * 60)
    print("If auto-assignment is not working, follow these steps:")
    print()
    print("1️⃣ Add Auto-Assign Configuration:")
    print("   • Go to Admin Dashboard → Manage CRE")
    print("   • For each CRE, add auto-assign configuration")
    print("   • Set source (GOOGLE, META, BTL) and priority")
    print()
    print("2️⃣ Verify CRE Users:")
    print("   • Ensure CRE users exist in cre_users table")
    print("   • Check that auto_assign_count field exists")
    print()
    print("3️⃣ Check Lead Status:")
    print("   • Ensure leads have assigned='No' initially")
    print("   • Verify source field matches configuration")
    print()
    print("4️⃣ Database Tables:")
    print("   • auto_assign_config - configuration table")
    print("   • auto_assign_history - audit trail")
    print("   • cre_call_attempt_history - call records")
    print()
    print("5️⃣ Test Configuration:")
    print("   • Run this test script again")
    print("   • Check logs for auto-assignment messages")
    print("=" * 60)

if __name__ == "__main__":
    print("🚀 Auto-Assignment Configuration Test")
    print("=" * 60)
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Missing Supabase credentials in environment variables")
        print("   Please check your .env file")
        exit(1)
    
    success = test_auto_assign_config()
    
    if not success:
        show_manual_fix_instructions()
    else:
        print("\n✅ All tests passed! Auto-assignment should be working.")
        print("   💡 If leads are still not being assigned, check the logs for errors.")

