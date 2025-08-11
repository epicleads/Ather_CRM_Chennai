#!/usr/bin/env python3
"""
Test Script for CRE Dashboard Filtering
This script helps verify that the dashboard cards are correctly filtering by current month
"""

from datetime import datetime, timedelta
import pytz

def test_month_filtering():
    """Test the month filtering logic used in the dashboard"""
    
    # Get current month in YYYY-MM format
    current_month = datetime.now().strftime('%Y-%m')
    print(f"Current Month: {current_month}")
    
    # Test various timestamp formats
    test_timestamps = [
        # Current month timestamps (should pass)
        f"{current_month}-15 14:30:00+05:30",
        f"{current_month}-01 09:00:00+05:30",
        f"{current_month}-31 23:59:59+05:30",
        
        # Previous month timestamps (should fail)
        f"{datetime.now().replace(day=1) - timedelta(days=1):%Y-%m}-15 14:30:00+05:30",
        f"{datetime.now().replace(day=1) - timedelta(days=30):%Y-%m}-15 14:30:00+05:30",
        
        # Next month timestamps (should fail)
        next_month = datetime.now().replace(day=28) + timedelta(days=4)
        f"{next_month:%Y-%m}-15 14:30:00+05:30",
    ]
    
    print("\n=== Testing Month Filtering Logic ===")
    for timestamp in test_timestamps:
        # Extract YYYY-MM part (same logic used in dashboard)
        timestamp_month = timestamp[:7]
        is_current_month = timestamp_month == current_month
        
        print(f"Timestamp: {timestamp}")
        print(f"  Extracted Month: {timestamp_month}")
        print(f"  Is Current Month: {is_current_month}")
        print(f"  Should Count in Dashboard: {is_current_month}")
        print()
    
    # Test edge cases
    print("=== Edge Cases ===")
    
    # Test with None values
    print("None timestamp:", "None"[:7] if None else "Error - None has no [:7]")
    
    # Test with empty string
    print("Empty string:", ""[:7])
    
    # Test with invalid format
    try:
        invalid_timestamp = "invalid-date"
        print(f"Invalid timestamp '{invalid_timestamp}': {invalid_timestamp[:7]}")
    except Exception as e:
        print(f"Invalid timestamp error: {e}")

def test_dashboard_logic():
    """Test the dashboard counting logic"""
    
    current_month = datetime.now().strftime('%Y-%m')
    print(f"\n=== Dashboard Logic Test ===")
    print(f"Current Month: {current_month}")
    
    # Simulate the dashboard filtering logic
    print("\n1. PS Assigned Filter:")
    print("   - Must have: ps_name IS NOT NULL")
    print("   - Must have: ps_assigned_at IS NOT NULL") 
    print("   - Must have: ps_assigned_at[:7] == current_month")
    
    print("\n2. Won Leads Filter:")
    print("   - Must have: final_status = 'Won'")
    print("   - Must have: won_timestamp IS NOT NULL")
    print("   - Must have: won_timestamp[:7] == current_month")
    
    print("\n3. Lost Leads Filter:")
    print("   - Must have: final_status = 'Lost'")
    print("   - Must have: lost_timestamp IS NOT NULL")
    print("   - Must have: lost_timestamp[:7] == current_month")
    
    print(f"\n4. Month Comparison:")
    print(f"   - Current month: {current_month}")
    print(f"   - Format: YYYY-MM (e.g., 2025-08)")
    print(f"   - Extraction: timestamp[:7]")

if __name__ == "__main__":
    test_month_filtering()
    test_dashboard_logic()
    
    print("\n=== How to Test in Dashboard ===")
    print("1. Open CRE Dashboard in browser")
    print("2. Check console logs for DEBUG messages")
    print("3. Look for 'Current Month Filter: YYYY-MM' message")
    print("4. Verify card numbers match current month data only")
    print("5. Check that leads from previous months are not counted")
    print("\nExpected DEBUG output:")
    print("- 'DEBUG: Added to PS assigned - UID: xxx, PS: xxx, Assigned: timestamp'")
    print("- 'DEBUG: Added to won leads - UID: xxx, Won: timestamp'") 
    print("- 'DEBUG: Added to lost leads - UID: xxx, Lost: timestamp'")
