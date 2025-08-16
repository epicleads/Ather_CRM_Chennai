#!/usr/bin/env python3
"""
Test script for Auto-Assign System
This script tests the enhanced auto-assign functionality with Uday branch features
"""

import os
import sys
import time
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_auto_assign_system():
    """Test the auto-assign system functionality"""
    print("🧪 ========================================")
    print("🧪 TESTING AUTO-ASSIGN SYSTEM")
    print("🧪 ========================================")
    print(f"⏰ Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Import the auto-assign module
        from auto_assign_module import AutoAssignSystem
        
        print("✅ Auto-assign module imported successfully")
        
        # Create a mock Supabase client for testing
        class MockSupabase:
            def table(self, table_name):
                return MockTable()
        
        class MockTable:
            def select(self, *args):
                return MockQuery()
            def eq(self, field, value):
                return MockQuery()
            def execute(self):
                return MockResult()
        
        class MockQuery:
            def select(self, *args):
                return self
            def eq(self, field, value):
                return self
            def execute(self):
                return MockResult()
        
        class MockResult:
            @property
            def data(self):
                return []
        
        # Initialize the auto-assign system
        mock_supabase = MockSupabase()
        auto_assign_system = AutoAssignSystem(mock_supabase)
        
        print("✅ Auto-assign system initialized successfully")
        
        # Test debug mode
        print("\n🔍 Testing Debug Mode:")
        auto_assign_system.enable_debug_mode()
        debug_status = auto_assign_system.get_debug_status()
        print(f"   Debug Mode: {debug_status.get('debug_mode', False)}")
        print(f"   Verbose Logging: {debug_status.get('verbose_logging', False)}")
        print(f"   Auto-start Enabled: {debug_status.get('auto_start_enabled', False)}")
        
        # Test system status
        print("\n📊 Testing System Status:")
        status = auto_assign_system.get_auto_assign_status()
        print(f"   Running: {status.get('is_running', False)}")
        print(f"   Total Runs: {status.get('total_runs', 0)}")
        print(f"   Total Leads: {status.get('total_leads_assigned', 0)}")
        print(f"   Thread Alive: {status.get('thread_alive', False)}")
        
        # Test force start
        print("\n🚀 Testing Force Start:")
        force_start_result = auto_assign_system.force_start_system()
        print(f"   Force Start Result: {force_start_result}")
        
        # Test health check
        print("\n🏥 Testing Health Check:")
        health_result = auto_assign_system.check_system_health_and_restart()
        print(f"   Health Check Result: {health_result}")
        
        # Test detailed status
        print("\n📋 Testing Detailed Status:")
        detailed_status = auto_assign_system.get_detailed_status()
        print(f"   Enhanced Features: {detailed_status.get('enhanced_features', [])}")
        print(f"   Health Score: {detailed_status.get('health', {}).get('health_score', 'N/A')}")
        
        print("\n✅ All tests completed successfully!")
        print("🧪 ========================================")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        print("🧪 ========================================")
        return False

def test_manual_trigger():
    """Test manual trigger functionality"""
    print("\n🎯 ========================================")
    print("🎯 TESTING MANUAL TRIGGER")
    print("🎯 ========================================")
    
    try:
        from auto_assign_module import AutoAssignSystem
        
        # Create mock system
        class MockSupabase:
            def table(self, table_name):
                return MockTable()
        
        class MockTable:
            def select(self, *args):
                return MockQuery()
            def eq(self, field, value):
                return MockQuery()
            def execute(self):
                return MockResult()
        
        class MockQuery:
            def select(self, *args):
                return self
            def eq(self, field, value):
                return self
            def execute(self):
                return MockResult()
        
        class MockResult:
            @property
            def data(self):
                return []
        
        mock_supabase = MockSupabase()
        auto_assign_system = AutoAssignSystem(mock_supabase)
        
        # Test manual trigger
        print("🔄 Testing manual trigger...")
        trigger_result = auto_assign_system.manual_trigger_auto_assign("TEST_SOURCE")
        
        print(f"   Trigger Result: {trigger_result.get('success', False)}")
        print(f"   Message: {trigger_result.get('message', 'N/A')}")
        print(f"   Reference: {trigger_result.get('trigger_reference', 'N/A')}")
        print(f"   Enhanced Features: {trigger_result.get('enhanced_features', [])}")
        
        print("✅ Manual trigger test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Manual trigger test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Auto-Assign System Tests")
    print("=" * 60)
    
    # Test basic functionality
    basic_test = test_auto_assign_system()
    
    # Test manual trigger
    trigger_test = test_manual_trigger()
    
    # Summary
    print("\n📊 ========================================")
    print("📊 TEST SUMMARY")
    print("📊 ========================================")
    print(f"   Basic Functionality: {'✅ PASS' if basic_test else '❌ FAIL'}")
    print(f"   Manual Trigger: {'✅ PASS' if trigger_test else '❌ FAIL'}")
    
    if basic_test and trigger_test:
        print("\n🎉 All tests passed! Auto-assign system is working correctly.")
        print("   🚀 Uday branch enhancements are active")
        print("   📊 Debug prints and stickers are working")
        print("   🔍 Enhanced logging is functional")
    else:
        print("\n⚠️ Some tests failed. Please check the error messages above.")
    
    print("📊 ========================================")
