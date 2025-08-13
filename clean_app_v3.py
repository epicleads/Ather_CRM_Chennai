#!/usr/bin/env python3
"""
Clean up the massive duplication in app.py - Version 3
This script will remove ALL duplicate routes and functions completely
"""

def clean_app_file():
    print("🧹 Starting comprehensive cleanup of app.py...")
    
    # Read the original file
    with open('app.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"📄 Original file: {len(lines)} lines")
    
    # Find where the first complete section ends
    # Look for the first occurrence of a route that appears multiple times
    first_section_end = None
    
    # Search for routes that appear multiple times
    route_patterns = [
        '@app.route(\'/password_reset_request\'',
        '@app.route(\'/change_password\'',
        '@app.route(\'/change_cre_password\'',
        '@app.route(\'/change_ps_password\'',
        '@app.route(\'/security_settings\'',
        '@app.route(\'/admin_dashboard\'',
    ]
    
    for pattern in route_patterns:
        # Find all occurrences of this pattern
        occurrences = []
        for i, line in enumerate(lines):
            if pattern in line:
                occurrences.append(i)
        
        if len(occurrences) > 1:
            # The first section ends before the second occurrence
            first_section_end = occurrences[1]
            print(f"🔍 Found first section end at line {first_section_end} using pattern: {pattern}")
            break
    
    if first_section_end is None:
        print("❌ Could not determine where first section ends")
        return False
    
    # Extract only the first section
    clean_lines = lines[:first_section_end]
    
    # Add the main execution block at the end
    main_block = [
        "\n",
        "if __name__ == '__main__':\n",
        "    print(\" Starting Ather CRM System...\")\n",
        "    print(\"📱 Server will be available at: http://127.0.0.1:5000\")\n",
        "    print(\"🌐 You can also try: http://localhost:5000\")\n",
        "    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)\n"
    ]
    
    clean_lines.extend(main_block)
    
    # Write the clean file
    with open('app_completely_clean.py', 'w', encoding='utf-8') as f:
        f.writelines(clean_lines)
    
    print(f"✅ Clean file created: app_completely_clean.py")
    print(f"📊 Clean file: {len(clean_lines)} lines")
    print(f"🗑️ Removed approximately {len(lines) - len(clean_lines)} duplicate lines")
    print(f"📈 Reduction: {((len(lines) - len(clean_lines)) / len(lines) * 100):.1f}%")
    
    return True

if __name__ == '__main__':
    clean_app_file()

