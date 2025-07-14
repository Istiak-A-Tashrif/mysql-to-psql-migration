#!/usr/bin/env python3
"""
Comprehensive syntax error fix for migration files
"""
import os
import re
import glob

def fix_syntax_errors():
    """Fix common syntax errors in migration files"""
    
    migration_files = glob.glob("*_migration.py")
    fixed_count = 0
    
    for file_path in migration_files:
        print(f"Processing {file_path}...")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Fix 1: Unterminated f-strings in SHOW CREATE TABLE commands
            content = re.sub(r'f"SHOW CREATE TABLE `([^`]+)`;["\']?["\']?', r'f"SHOW CREATE TABLE `\1`;"', content)
            
            # Fix 2: Unterminated f-strings in SHOW INDEX commands  
            content = re.sub(r'f"SHOW INDEX FROM `([^`]+)`;["\']?["\']?', r'f"SHOW INDEX FROM `\1`;"', content)
            
            # Fix 3: Malformed split statements
            content = content.replace("split(\\'\\n\")", "split('\\n')")
            content = content.replace("split(\\'n\")", "split('\\n')")
            
            # Fix 4: Malformed regex with f-strings for CREATE TABLE
            content = re.sub(r'f"CREATE TABLE "([^"]+)"\\\'', r'"CREATE TABLE "\1"', content)
            
            # Fix 5: Malformed regex patterns with quotes
            content = re.sub(r'r\\\'`\(\[\^\`\]\+\)`", r\'""\'', r'r"`([^`]+)`", r\'"\1"\'', content)
            
            # Fix 6: Remove duplicate/malformed lines
            lines = content.split('\n')
            clean_lines = []
            prev_line = ""
            
            for line in lines:
                # Skip obvious duplicate malformed lines
                if 'f"CREATE TABLE "' in line and '\\' in line and line == prev_line:
                    continue
                clean_lines.append(line)
                prev_line = line
            
            content = '\n'.join(clean_lines)
            
            if content != original_content:
                print(f"  Fixing {file_path}...")
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                
                # Test syntax
                try:
                    compile(content, file_path, 'exec')
                    print(f"  ✅ {file_path} syntax is now valid")
                except SyntaxError as e:
                    print(f"  ⚠️  {file_path} still has syntax errors: {e}")
            else:
                print(f"  ⏭️  {file_path} - no changes needed")
                
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {e}")
    
    print(f"\nFixed {fixed_count} files with syntax errors")

if __name__ == "__main__":
    fix_syntax_errors()
