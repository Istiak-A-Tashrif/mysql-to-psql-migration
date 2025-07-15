#!/usr/bin/env python3
"""
Simple targeted fix for the most common syntax errors
"""
import os
import glob

def fix_common_syntax_errors():
    """Fix the most common syntax errors in migration files"""
    
    migration_files = glob.glob("*_migration.py")
    fixed_count = 0
    
    for file_path in migration_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Fix broken newline joins
            content = content.replace("create_table_statement = '\n'.join(create_table_lines[1:])", 
                                    "create_table_statement = '\\n'.join(create_table_lines[1:])")
            
            # Fix broken newline splits  
            content = content.replace("split('\n')", "split('\\n')")
            
            # Test if this fixes syntax
            if content != original_content:
                try:
                    compile(content, file_path, 'exec')
                    print(f" Fixing {file_path}")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    fixed_count += 1
                except SyntaxError as e:
                    print(f"⚠️ {file_path} still has errors after fix: {e}")
            
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
    
    print(f"\nFixed {fixed_count} files")

if __name__ == "__main__":
    fix_common_syntax_errors()
