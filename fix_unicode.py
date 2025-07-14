#!/usr/bin/env python3
"""
Fix Unicode Characters in Migration Scripts
==========================================

This script finds and removes Unicode characters from migration scripts
that are causing encoding errors on Windows.
"""

import os
import re
import glob

def fix_unicode_in_file(file_path):
    """Remove Unicode characters from a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Track changes
        original_content = content
        
        # Remove common Unicode emojis and symbols
        content = re.sub(r'', '', content)  # magnifying glass
        content = re.sub(r'📊', '', content)  # bar chart
        content = re.sub(r'', '', content)  # rocket
        content = re.sub(r'✅', '', content)  # check mark
        content = re.sub(r'❌', '', content)  # cross mark
        content = re.sub(r'⚠️', '', content)  # warning
        content = re.sub(r'🔧', '', content)  # wrench
        content = re.sub(r'📝', '', content)  # memo
        content = re.sub(r'💾', '', content)  # floppy disk
        content = re.sub(r'🗂️', '', content)  # card index
        content = re.sub(r'📋', '', content)  # clipboard
        content = re.sub(r'🎯', '', content)  # direct hit
        content = re.sub(r'⭐', '', content)  # star
        content = re.sub(r'🔄', '', content)  # arrows counterclockwise
        content = re.sub(r'🌟', '', content)  # glowing star
        
        # Remove any other Unicode characters outside ASCII range
        content = re.sub(r'[^\x00-\x7F]+', '', content)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Fix Unicode characters in all migration scripts"""
    migration_files = glob.glob('*_migration.py')
    fixed_count = 0
    
    print("Fixing Unicode characters in migration scripts...")
    
    for file_path in migration_files:
        if fix_unicode_in_file(file_path):
            print(f"  Fixed: {file_path}")
            fixed_count += 1
        else:
            print(f"  OK: {file_path}")
    
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()
