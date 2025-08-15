#!/usr/bin/env python3
"""
Test if the Flask app is running and if the API endpoint is accessible
"""

import requests
import json

def test_api_endpoint():
    """Test the Branch Head approval leads API endpoint"""
    
    # Test the API endpoint directly
    api_url = "http://localhost:5000/api/bh_approval_leads"
    
    try:
        print(f"🔍 Testing API endpoint: {api_url}")
        
        # Make a GET request to the API
        response = requests.get(api_url, timeout=10)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"Response Data: {json.dumps(data, indent=2)}")
                
                if data.get('success'):
                    print("✅ API call successful!")
                    print(f"📊 Found {data.get('count', 0)} approval leads")
                    
                    if data.get('leads'):
                        for lead in data['leads']:
                            print(f"  - {lead.get('lead_id')}: {lead.get('customer_name')} - Order ID: {lead.get('order_id')}")
                else:
                    print(f"❌ API returned error: {data.get('message')}")
                    
            except json.JSONDecodeError:
                print(f"❌ Response is not valid JSON: {response.text}")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response Text: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Flask app is not running or not accessible")
        print("   Make sure to start the Flask app with: python app.py")
    except requests.exceptions.Timeout:
        print("❌ Timeout Error: API request took too long")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

def test_app_health():
    """Test if the Flask app is responding at all"""
    
    health_url = "http://localhost:5000/"
    
    try:
        print(f"\n🔍 Testing app health: {health_url}")
        
        response = requests.get(health_url, timeout=5)
        print(f"App Health Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Flask app is running and responding")
        else:
            print(f"⚠️  App responded with status: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Flask app is not running")
    except Exception as e:
        print(f"❌ Health check error: {e}")

if __name__ == "__main__":
    print("🚀 Testing Flask App and API Endpoints")
    print("=" * 50)
    
    test_app_health()
    test_api_endpoint()
    
    print("\n" + "=" * 50)
    print("✅ Testing completed!")
