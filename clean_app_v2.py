#!/usr/bin/env python3
"""
Clean up the massive duplication in app.py - Version 2
This script will extract only the first section and remove all duplicates completely
"""

def clean_app_file():
    print("🧹 Starting comprehensive cleanup of app.py...")
    
    # Read the original file
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"📄 Original file size: {len(content)} characters")
    
    # Find the first complete section by looking for the second occurrence of a unique route
    # The first section ends when we find the second occurrence of the index route
    
    # Find the first occurrence of the index route
    first_index = content.find('@app.route(\'/\')')
    if first_index == -1:
        print("❌ Could not find index route")
        return False
    
    # Find the second occurrence of the index route
    second_index = content.find('@app.route(\'/\')', first_index + 1)
    if second_index == -1:
        print("✅ No duplicates found - file is already clean")
        return True
    
    print(f"🔍 Found first section end at position {second_index}")
    
    # Extract only the first section
    clean_content = content[:second_index]
    
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
    with open('app_final_clean.py', 'w', encoding='utf-8') as f:
        f.write(clean_content)
    
    print(f"✅ Clean file created: app_final_clean.py")
    print(f"📊 Clean file size: {len(clean_content)} characters")
    print(f"🗑️ Removed approximately {len(content) - len(clean_content)} duplicate characters")
    print(f"📈 Reduction: {((len(content) - len(clean_content)) / len(content) * 100):.1f}%")
    
    return True

if __name__ == '__main__':
    clean_app_file()

