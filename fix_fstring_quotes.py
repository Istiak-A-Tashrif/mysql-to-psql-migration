#!/usr/bin/    pattern = r"f\"SHOW CREATE TABLE.*?;\\\'"
#!/usr/bin/env python3
"""
Fix malformed f-strings in migration files
"""
import os
import re
import glob

def fix_fstring_quotes():
    """Fix malformed f-string quotes in migration files"""
    
    migration_files = glob.glob("*_migration.py")
    fixed_count = 0
    
    for file_path in migration_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if file has the issue
            if ";\'" in content and 'SHOW CREATE TABLE' in content:
                print(f"Fixing {file_path}...")
                # Fix the malformed f-strings
                new_content = content.replace(";\\'", ";")
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                fixed_count += 1
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"Fixed {fixed_count} files with malformed f-strings")

if __name__ == "__main__":
    fix_fstring_quotes()
