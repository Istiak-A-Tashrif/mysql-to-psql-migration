#!/usr/bin/env python3
"""
Fix malformed f-strings in migration files more robustly
"""
import os
import re
import glob

def fix_fstring_quotes_robust():
    """Fix malformed f-string quotes in migration files robustly"""
    
    migration_files = glob.glob("*_migration.py")
    fixed_count = 0
    
    for file_path in migration_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Fix pattern: f"SHOW CREATE TABLE `TableName`; (missing closing quote)
            pattern1 = r"f\"SHOW CREATE TABLE `[^`]+`;"
            if re.search(pattern1, content):
                content = re.sub(pattern1, lambda m: m.group(0) + '"', content)
            
            # Fix pattern: split(\'\n") - malformed split
            content = content.replace("split(\\'\\n\")", "split('\\n')")
            content = content.replace("split(\\'n\")", "split('\\n')")
            
            if content != original_content:
                print(f"Fixing {file_path}...")
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"Fixed {fixed_count} files with malformed f-strings")

if __name__ == "__main__":
    fix_fstring_quotes_robust()
