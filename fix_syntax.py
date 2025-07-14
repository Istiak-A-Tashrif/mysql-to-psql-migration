#!/usr/bin/env python3
"""
Fix Common Syntax Issues in Migration Scripts
===========================================

This script fixes common syntax issues found in migration scripts.
"""

import os
import re
import glob

def fix_syntax_issues_in_file(file_path):
    """Fix common syntax issues in a migration file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Fix 1: Literal newlines in split() calls
        content = re.sub(r"split\('\n'\)", r"split('\\n')", content)
        content = re.sub(r"split\(\s*'\s*\n\s*'\s*\)", r"split('\\n')", content)
        
        # Fix 2: Literal newlines in join() calls  
        content = re.sub(r"join\('\n'\)", r"join('\\n')", content)
        content = re.sub(r"join\(\s*'\s*\n\s*'\s*\)", r"join('\\n')", content)
        
        # Fix 3: Nested quotes in f-strings - common pattern
        content = re.sub(
            r"f'([^']*)'([^']*)'([^']*)'", 
            r'f"\1\'\2\'\3"', 
            content
        )
        
        # Fix 4: Missing return_result parameter fixes
        content = re.sub(
            r"execute_postgresql_sql\(([^,]+),\s*return_result=True\)",
            r"execute_postgresql_sql(\1)",
            content
        )
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Fix syntax issues in all migration scripts"""
    migration_files = glob.glob('*_migration.py')
    fixed_count = 0
    
    print("Fixing syntax issues in migration scripts...")
    
    for file_path in migration_files:
        try:
            # Test compilation first
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            compile(code, file_path, 'exec')
            print(f"  OK: {file_path}")
        except SyntaxError as e:
            print(f"  FIXING: {file_path} - {e}")
            if fix_syntax_issues_in_file(file_path):
                fixed_count += 1
                # Test again after fix
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        code = f.read()
                    compile(code, file_path, 'exec')
                    print(f"    FIXED: {file_path}")
                except SyntaxError as e2:
                    print(f"    STILL BROKEN: {file_path} - {e2}")
        except Exception as e:
            print(f"  ERROR: {file_path} - {e}")
    
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()
