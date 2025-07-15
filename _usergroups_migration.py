#!/usr/bin/env python3
"""
_UserGroups Table Migration Script
==================================

This script provides a complete 3-phase migration approach specifically for the _UserGroups table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles _UserGroups-specific data types and constraints
- Manages foreign key dependencies (Group, User)
- Creates appropriate indexes for _UserGroups table
- Junction table for User-Group many-to-many relationship

Usage: 
    python _usergroups_migration.py --phase=1
    python _usergroups_migration.py --phase=2
    python _usergroups_migration.py --phase=3
    python _usergroups_migration.py --full
    python _usergroups_migration.py --verify
"""

import re
import os
import argparse
from collections import OrderedDict
from table_utils import (
    verify_table_structure,
    run_command,
    create_postgresql_table,
    create_postgresql_table_with_enums,
    export_and_clean_mysql_data,
    import_data_to_postgresql,
    add_primary_key_constraint,
    setup_auto_increment_sequence,
    execute_postgresql_sql,
    robust_import_with_serial_id
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "_UserGroups"

def get_usergroups_table_info():
    """Get complete _UserGroups table information from MySQL including constraints"""
    print(f" Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"Failed to get table info: {result.stderr if result else 'No result'}")
        return None, [], []
    
    create_table_stmt = result.stdout
    
    # Extract indexes (show all indexes for the table)
    indexes_cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW INDEX FROM `{TABLE_NAME}`;"'
    indexes_result = run_command(indexes_cmd)
    
    indexes = []
    if indexes_result and indexes_result.returncode == 0:
        lines = indexes_result.stdout.strip().split('\n')[1:]  # Skip header
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 3:
                    key_name = parts[2]
                    if key_name not in ['PRIMARY'] and key_name not in [idx['name'] for idx in indexes]:
                        indexes.append({
                            'name': key_name,
                            'columns': parts[4] if len(parts) > 4 else '',
                            'type': 'INDEX'
                        })
    
    # Extract foreign keys
    fk_cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = \'source_db\' AND TABLE_NAME = \'{TABLE_NAME}\' AND REFERENCED_TABLE_NAME IS NOT NULL;"'
    fk_result = run_command(fk_cmd)
    
    foreign_keys = []
    if fk_result and fk_result.returncode == 0:
        lines = fk_result.stdout.strip().split('\n')[1:]  # Skip header
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 4:
                    foreign_keys.append({
                        'name': parts[0],
                        'column': parts[1],
                        'referenced_table': parts[2],
                        'referenced_column': parts[3]
                    })
    
    print(f" Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return create_table_stmt, indexes, foreign_keys

def convert_usergroups_mysql_to_postgresql(mysql_ddl, include_constraints=False):
    """Convert _UserGroups MySQL DDL to PostgreSQL format"""
    print(f" Converting {TABLE_NAME} table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {PRESERVE_MYSQL_CASE})...")
    
    # Handle the _UserGroups specific structure with proper quoting
    postgresql_ddl = f'''CREATE TABLE {"\"_UserGroups\"" if PRESERVE_MYSQL_CASE else "\"_usergroups\""} (
  {"\"A\"" if PRESERVE_MYSQL_CASE else "\"a\""} INTEGER NOT NULL,
  {"\"B\"" if PRESERVE_MYSQL_CASE else "\"b\""} INTEGER NOT NULL
)'''
    
    print(f" Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgresql_ddl)
    print("=" * 50)
    
    return postgresql_ddl

def create_usergroups_table(mysql_ddl):
    """Create _UserGroups table in PostgreSQL"""
    pg_ddl = convert_usergroups_mysql_to_postgresql(mysql_ddl, include_constraints=False)
    return create_postgresql_table(TABLE_NAME, pg_ddl, PRESERVE_MYSQL_CASE)

def create_usergroups_indexes(indexes):
    """Create indexes for _UserGroups table using manual commands"""
    if not indexes:
        print(f" No indexes to create for {TABLE_NAME}")
        return True
    
    print(f" Creating {len(indexes)} indexes for {TABLE_NAME}...")
    success_count = 0
    
    from table_utils import execute_postgresql_sql
    
    # Create the specific indexes manually based on MySQL structure
    # 1. Unique constraint on (A, B)
    col_a = "A" if PRESERVE_MYSQL_CASE else "a"
    col_b = "B" if PRESERVE_MYSQL_CASE else "b"
    table_ref = '"_UserGroups"' if PRESERVE_MYSQL_CASE else '"_usergroups"'
    
    unique_constraint_sql = f'ALTER TABLE {table_ref} ADD CONSTRAINT "_UserGroups_AB_unique" UNIQUE ("{col_a}", "{col_b}");'
    success, result = execute_postgresql_sql(unique_constraint_sql, f"{TABLE_NAME} unique constraint")
    if success:
        print(f" Created {TABLE_NAME} unique constraint: _UserGroups_AB_unique")
        success_count += 1
    else:
        print(f" Failed to create {TABLE_NAME} unique constraint")
        if result:
            print(f"   Error: {result.stderr}")
    
    # 2. Index on B column
    index_sql = f'CREATE INDEX "_UserGroups_B_index" ON {table_ref} ("{col_b}");'
    success, result = execute_postgresql_sql(index_sql, f"{TABLE_NAME} B index")
    if success:
        print(f" Created {TABLE_NAME} index: _UserGroups_B_index")
        success_count += 1
    else:
        print(f" Failed to create {TABLE_NAME} B index")
        if result:
            print(f"   Error: {result.stderr}")
    
    print(f" {TABLE_NAME} Indexes: {success_count} created, {2 - success_count} failed")
    return success_count == 2

def create_usergroups_foreign_keys(foreign_keys):
    """Create foreign keys for _UserGroups table using manual commands"""
    print(f" Creating foreign keys for {TABLE_NAME} using manual commands...")
    
    from table_utils import execute_postgresql_sql
    
    # Manual foreign key creation commands
    foreign_key_commands = [
        'ALTER TABLE "_UserGroups" ADD CONSTRAINT "_UserGroups_A_fkey" FOREIGN KEY ("A") REFERENCES "Group" ("id") ON DELETE CASCADE ON UPDATE CASCADE;',
        'ALTER TABLE "_UserGroups" ADD CONSTRAINT "_UserGroups_B_fkey" FOREIGN KEY ("B") REFERENCES "User" ("id") ON DELETE CASCADE ON UPDATE CASCADE;'
    ]
    
    success_count = 0
    for fk_sql in foreign_key_commands:
        success, result = execute_postgresql_sql(fk_sql, f"Foreign key creation for {TABLE_NAME}")
        if success:
            print(f" Created foreign key successfully")
            success_count += 1
        else:
            print(f" Failed to create foreign key: {fk_sql}")
            if result:
                print(f"   Error: {result.stderr}")
    
    print(f" {TABLE_NAME} Foreign Keys: {success_count} created, {len(foreign_key_commands) - success_count} failed")
    return success_count > 0

def import_usergroups_data_manual():
    """Import _UserGroups data using the proven manual approach"""
    print("Importing _UserGroups data using proven manual approach...")
    
    try:
        # Clear existing data first
        clear_sql = 'DELETE FROM "_UserGroups";'
        success, _ = execute_postgresql_sql(clear_sql, "Clear _UserGroups data")
        if not success:
            print("Failed to clear existing _UserGroups data")
            return False
        
        # Step 1: Export data from MySQL, filter headers, and convert to CSV
        export_cmd = 'docker exec mysql_source mysql -u mysql -pmysql source_db --batch --raw -e "SELECT A, B FROM _UserGroups ORDER BY A, B"'
        export_result = run_command(export_cmd)
        
        if not export_result or export_result.returncode != 0:
            print("Failed to export _UserGroups data from MySQL")
            return False
        
        # Step 2: Process the output (remove header and convert tabs to commas)
        lines = export_result.stdout.strip().split('\n')
        csv_lines = []
        for line in lines[1:]:  # Skip header
            if line.strip():
                csv_line = line.replace('\t', ',')
                csv_lines.append(csv_line)
        
        csv_content = '\n'.join(csv_lines)
        
        # Step 3: Write CSV data to file (using Windows-compatible path)
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        temp_file.write(csv_content)
        temp_file.close()
        
        # Step 4: Copy CSV to PostgreSQL container
        copy_result = run_command(f'docker cp "{temp_file.name}" postgres_target:/tmp/_UserGroups_data.csv')
        
        if not copy_result or copy_result.returncode != 0:
            print("Failed to copy CSV to PostgreSQL container")
            # Cleanup temp file
            import os
            os.unlink(temp_file.name)
            return False
        
        # Step 5: Import data using COPY
        # Use the correct column names based on PRESERVE_MYSQL_CASE
        col_names = f'({"A, B" if PRESERVE_MYSQL_CASE else "a, b"})'
        copy_sql = f'''COPY "_UserGroups" {col_names} FROM '/tmp/_UserGroups_data.csv' WITH (FORMAT csv, DELIMITER ',', NULL '');'''
        success, output = execute_postgresql_sql(copy_sql, "Import _UserGroups data")
        
        # Cleanup temp file
        import os
        os.unlink(temp_file.name)
        
        if success:
            print("Successfully imported _UserGroups data using proven manual approach")
            return True
        else:
            print(f"Failed to import _UserGroups data: {output}")
            return False
            
    except Exception as e:
        print(f"Error importing _UserGroups data: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Migrate _UserGroups table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Run specific phase')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure')
    
    args = parser.parse_args()
    
    if not any([args.phase, args.full, args.verify]):
        print("Please specify --phase, --full, or --verify")
        return
    
    if args.verify:
        verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return
    
    # Get table information
    mysql_ddl, indexes, foreign_keys = get_usergroups_table_info()
    if not mysql_ddl:
        print("Failed to get table information")
        return
    
    success = True
    
    if args.phase == '1' or args.full:
        print(f" Phase 1: Creating {TABLE_NAME} table and importing data")
        if not create_usergroups_table(mysql_ddl):
            success = False
        else:
            # Use custom import for this junction table (no ID column)
            if not import_usergroups_data_manual():
                print(f"Failed to import data using custom approach")
                success = False
            print(f" Phase 1 complete for {TABLE_NAME}")
    
    if args.phase == '2' or args.full:
        print(f" Phase 2: Creating indexes for {TABLE_NAME}")
        if not create_usergroups_indexes(indexes):
            success = False
    
    if args.phase == '3' or args.full:
        print(f" Phase 3: Creating foreign keys for {TABLE_NAME}")
        if not create_usergroups_foreign_keys(foreign_keys):
            success = False
    
    if success:
        print(f" Operation completed successfully!")
    else:
        print(f" Operation completed with some errors.")

if __name__ == "__main__":
    main()
