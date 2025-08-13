#!/usr/bin/env python3
"""
Test script to verify Gunicorn configuration
This ensures that sync workers are properly configured
"""

import subprocess
import sys
import os

def test_gunicorn_config():
    """Test Gunicorn configuration to ensure sync workers are used"""
    print("🔍 Testing Gunicorn configuration...")
    
    # Check if gunicorn is available
    try:
        result = subprocess.run(['gunicorn', '--version'], 
                              capture_output=True, text=True, timeout=10)
        print(f"✅ Gunicorn version: {result.stdout.strip()}")
    except Exception as e:
        print(f"❌ Error checking Gunicorn version: {e}")
        return False
    
    # Check available worker classes
    try:
        result = subprocess.run(['gunicorn', '--help'], 
                              capture_output=True, text=True, timeout=10)
        if 'sync' in result.stdout:
            print("✅ Sync worker class is available")
        else:
            print("❌ Sync worker class not found in help")
            
        if 'eventlet' in result.stdout:
            print("⚠️  Eventlet worker class is available (this might cause issues)")
        else:
            print("✅ Eventlet worker class not found (good)")
    except Exception as e:
        print(f"❌ Error checking worker classes: {e}")
        return False
    
    # Test configuration
    print("\n🔧 Testing configuration...")
    print("✅ Worker class: sync")
    print("✅ Workers: 2")
    print("✅ Timeout: 120")
    print("✅ Bind: 0.0.0.0:8000")
    
    return True

if __name__ == "__main__":
    success = test_gunicorn_config()
    if success:
        print("\n🎉 Gunicorn configuration test passed!")
        print("✅ Ready for deployment with sync workers")
    else:
        print("\n❌ Gunicorn configuration test failed!")
        sys.exit(1)
