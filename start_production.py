#!/usr/bin/env python3
"""
Production startup script for Ather CRM
This script ensures the auto-assign system starts properly in production environments
"""

import os
import sys
import time
import threading
import traceback

# Set production environment variables
os.environ['PRODUCTION'] = 'true'
os.environ['RENDER'] = 'true'

print("🚀 Production startup script for Ather CRM")
print("=" * 50)
print(f"🏭 Environment: PRODUCTION={os.environ.get('PRODUCTION')}")
print(f"🏭 Environment: RENDER={os.environ.get('RENDER')}")
print(f"🏭 Environment: PORT={os.environ.get('PORT', 'Not Set')}")

def start_auto_assign_system():
    """Start the auto-assign system with production settings"""
    try:
        print("🔄 Importing auto-assign module...")
        from auto_assign_module import AutoAssignSystem
        
        print("🔄 Creating Supabase client...")
        from supabase import create_client, Client
        
        # Get Supabase credentials from environment
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            print("❌ Missing Supabase credentials")
            return False
        
        print("🔄 Initializing Supabase client...")
        supabase: Client = create_client(supabase_url, supabase_key)
        
        print("🔄 Creating auto-assign system...")
        auto_assign_system = AutoAssignSystem(supabase)
        
        print("🔄 Starting auto-assign system...")
        thread = auto_assign_system.start_robust_auto_assign_system()
        
        if thread and thread.is_alive():
            print("✅ Auto-assign system started successfully!")
            print(f"   🧵 Thread ID: {thread.ident}")
            print(f"   🧵 Thread Name: {thread.name}")
            print(f"   🧵 Thread Alive: {thread.is_alive()}")
            return True
        else:
            print("❌ Auto-assign system failed to start")
            return False
            
    except Exception as e:
        print(f"❌ Error starting auto-assign system: {e}")
        print(f"   🚨 Exception type: {type(e).__name__}")
        traceback.print_exc()
        return False

def main():
    """Main production startup function"""
    try:
        print("🔄 Starting production initialization...")
        
        # Wait a bit for the environment to be ready
        time.sleep(3)
        
        # Start auto-assign system
        success = start_auto_assign_system()
        
        if success:
            print("🎉 Production initialization completed successfully!")
            print("🔄 Auto-assign system is running in background...")
            
            # Keep the script alive to maintain the auto-assign system
            try:
                while True:
                    time.sleep(60)  # Check every minute
                    print("💓 Production auto-assign system heartbeat...")
            except KeyboardInterrupt:
                print("🛑 Production script interrupted")
        else:
            print("❌ Production initialization failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Critical error in production startup: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
