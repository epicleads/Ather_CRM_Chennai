#!/usr/bin/env python3
"""
Clean up the massive duplication in app.py
This script will extract only the first section and remove all duplicates
"""

def clean_app_file():
    print("🧹 Starting cleanup of app.py...")
    
    # Read the original file
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"📄 Original file size: {len(content)} characters")
    
    # Find the first complete section by looking for unique patterns
    # The first section ends around line 1464 (admin_dashboard route)
    
    # Split by a unique pattern that appears only once in the first section
    # Look for the end of the first section
    first_section_end = None
    
    # Find where the first section ends by looking for a unique pattern
    # Search for the first occurrence of a route that appears multiple times
    search_patterns = [
        '@app.route(\'/admin_dashboard\')',
        '@app.route(\'/upload_data\'',
        '@app.route(\'/manage_leads\'',
    ]
    
    for pattern in search_patterns:
        if pattern in content:
            # Find the first occurrence
            first_pos = content.find(pattern)
            if first_pos > 0:
                # Look for the next occurrence to find where first section ends
                next_pos = content.find(pattern, first_pos + 1)
                if next_pos > first_pos:
                    first_section_end = next_pos
                    print(f"🔍 Found first section end at position {first_section_end}")
                    break
    
    if first_section_end is None:
        # Fallback: look for the first major duplicate section
        # Search for the second occurrence of the index route
        first_index = content.find('@app.route(\'/\')')
        if first_index > 0:
            second_index = content.find('@app.route(\'/\')', first_index + 1)
            if second_index > first_index:
                first_section_end = second_index
                print(f"🔍 Found first section end at position {first_section_end}")
    
    if first_section_end is None:
        print("❌ Could not determine where first section ends")
        return False
    
    # Extract only the first section
    clean_content = content[:first_section_end]
    
    # Add the main execution block at the end
    main_block = """
if __name__ == '__main__':
    print(" Starting Ather CRM System...")
    print("📱 Server will be available at: http://127.0.0.1:5000")
    print("🌐 You can also try: http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
"""
    
    clean_content += main_block
    
    # Write the clean file
    with open('app_clean.py', 'w', encoding='utf-8') as f:
        f.write(clean_content)
    
    print(f"✅ Clean file created: app_clean.py")
    print(f"📊 Clean file size: {len(clean_content)} characters")
    print(f"🗑️ Removed approximately {len(content) - len(clean_content)} duplicate characters")
    
    return True

if __name__ == '__main__':
    clean_app_file()

