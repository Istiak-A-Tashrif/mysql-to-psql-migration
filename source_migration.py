#!/usr/bin/env python3
"""
Source Table Migration Script
============================

This script provides a complete 3-phase migration approach specifically for the Source table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles Source-specific data types and constraints
- Manages foreign key dependencies (Company)
- Creates appropriate indexes for Source table

Usage: 
    python source_migration.py --phase=1
    python source_migration.py --phase=2
    python source_migration.py --phase=3
    python source_migration.py --full
    python source_migration.py --verify
"""

import re
import os
import argparse
from collections import OrderedDict
from table_utils import (
    verify_table_structure,
    run_command,
    create_postgresql_table,
    export_and_clean_mysql_data,
    import_data_to_postgresql,
    add_primary_key_constraint,
    setup_auto_increment_sequence
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "Source"

def get_source_table_info():
    """Get complete Source table information from MySQL including constraints"""
    print(f" Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f" Failed to get Source table structure: {result.stderr if result else 'No result'}")
        return None, [], []
    
    mysql_ddl = result.stdout
    
    # Extract indexes
    indexes = extract_source_indexes_from_ddl(mysql_ddl)
    print(f" Found {len(indexes)} indexes for Source table")
    
    # Extract foreign keys
    foreign_keys = extract_source_foreign_keys_from_ddl(mysql_ddl)
    print(f" Found {len(foreign_keys)} foreign keys for Source table")
    
    return mysql_ddl, indexes, foreign_keys

def extract_source_indexes_from_ddl(ddl):
    """Extract index definitions from Source table MySQL DDL"""
    indexes = []
    
    # Pattern for KEY definitions
    key_pattern = r'(?:UNIQUE\s+)?KEY\s+`([^`]+)`\s*\(([^)]+)\)'
    
    matches = re.finditer(key_pattern, ddl, re.IGNORECASE)
    for match in matches:
        is_unique = 'UNIQUE' in match.group(0).upper()
        index_name = match.group(1)
        columns = match.group(2).strip()
        
        indexes.append({
            'name': index_name,
            'columns': columns,
            'unique': is_unique,
            'type': 'UNIQUE' if is_unique else 'INDEX',
            'table': 'Source'
        })
    
    return indexes

def extract_source_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from Source table MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY specific to Source
    fk_pattern = r'CONSTRAINT\s+`([^`]+)`\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`([^`]+)`\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?'
    
    matches = re.finditer(fk_pattern, ddl, re.IGNORECASE)
    for match in matches:
        constraint_name = match.group(1)
        local_columns = match.group(2)
        ref_table = match.group(3)
        ref_columns = match.group(4)
        on_delete = match.group(5) or 'RESTRICT'
        on_update = match.group(6) or 'RESTRICT'
        
        foreign_keys.append({
            'name': constraint_name,
            'local_columns': local_columns,
            'ref_table': ref_table,
            'ref_columns': ref_columns,
            'on_delete': on_delete,
            'on_update': on_update,
            'original': match.group(0),
            'table': 'Source'
        })
    
    return foreign_keys

def convert_source_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert Source table MySQL DDL to PostgreSQL DDL with Source-specific optimizations"""
    print(f" Converting Source table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Fix literal \n characters to actual newlines first
    postgres_ddl = mysql_ddl.replace('\\n', '\n')
    
    # Extract just the column definitions part
    create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)\)\s*ENGINE', postgres_ddl, re.DOTALL)
    if not create_match:
        create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)$', postgres_ddl, re.DOTALL)
    
    if not create_match:
        raise ValueError("Could not parse MySQL DDL structure")
    
    # Get column definitions part
    table_content = create_match.group(1)
    
    # Split by lines and process each line
    lines = table_content.split('\n')
    column_lines = []
    
    for line in lines:
        line = line.strip()
        if not line or line == '':
            continue
            
        # Skip constraint definitions if not including constraints
        if not include_constraints:
            if (line.startswith('PRIMARY KEY') or 
                line.startswith('UNIQUE KEY') or 
                line.startswith('KEY ') or 
                line.startswith('CONSTRAINT')):
                continue
        
        # Process column definition
        if line.endswith(','):
            line = line[:-1]  # Remove trailing comma
            
        # Convert backticks
        if preserve_case:
            line = re.sub(r'`([^`]+)`', r'"\1"', line)
        else:
            line = re.sub(r'`([^`]+)`', r'\1', line)
        
        # Convert data types
        line = re.sub(r'\bint\b(?!\s+NOT\s+NULL\s*,)', 'INTEGER', line, flags=re.IGNORECASE)
        line = re.sub(r'\bvarchar\(\d+\)', 'VARCHAR', line, flags=re.IGNORECASE)
        line = re.sub(r'\bdatetime\(\d+\)', 'TIMESTAMP', line, flags=re.IGNORECASE)
        
        # Fix PostgreSQL timestamp defaults
        line = re.sub(r'CURRENT_TIMESTAMP\(\d+\)', 'CURRENT_TIMESTAMP', line, flags=re.IGNORECASE)
        
        # Remove MySQL-specific syntax
        line = re.sub(r'\s+CHARACTER SET [a-zA-Z0-9_]+', '', line)
        line = re.sub(r'\s+COLLATE [a-zA-Z0-9_]+', '', line)
        line = re.sub(r'\s+AUTO_INCREMENT\b', '', line, flags=re.IGNORECASE)
        
        # Handle id column specially - convert to INTEGER (preserve original IDs)
        if '"id"' in line and ('int' in line.lower() or 'integer' in line.lower()):
            line = '"id" INTEGER NOT NULL'
        
        # Clean up whitespace
        line = re.sub(r'\s+', ' ', line).strip()
        
        if line:
            column_lines.append(line)
    
    # Rebuild the CREATE TABLE statement
    table_name = '"Source"' if preserve_case else 'source'
    postgres_ddl = f"CREATE TABLE {table_name} (\n"
    postgres_ddl += ',\n'.join(f"  {line}" for line in column_lines)
    postgres_ddl += "\n)"
    
    return postgres_ddl

def create_source_indexes(indexes):
    """Create indexes for Source table"""
    if not indexes:
        print(f" No indexes to create for {TABLE_NAME}")
        return True
    
    print(f" Creating {len(indexes)} indexes for {TABLE_NAME}...")
    
    success = True
    created_indexes = set()  # Track created index names to avoid duplicates
    
    for index in indexes:
        index_name = f"{TABLE_NAME.lower()}_{index['name']}"
        
        # Skip duplicates (can happen with UNIQUE variants)
        if index_name in created_indexes:
            print(f" Skipping duplicate index: {index_name}")
            continue
            
        created_indexes.add(index_name)
        columns = index['columns'].replace('`', '"')  # Convert backticks to quotes for case preservation
        unique_clause = "UNIQUE " if index['unique'] else ""
        
        # For case-sensitive table, quote the table name
        table_ref = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        
        create_index_sql = f"CREATE {unique_clause}INDEX {index_name} ON {table_ref} ({columns});"
        
        print(f" Creating Source index: {index_name}")
        
        # Write to file and execute
        sql_file = f"create_source_index_{index_name}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(create_index_sql)
        
        # Copy and execute
        copy_cmd = f"docker cp {sql_file} postgres_target:/tmp/{sql_file}"
        copy_result = run_command(copy_cmd)
        
        exec_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{sql_file}'
        result = run_command(exec_cmd)
        
        # Cleanup
        try:
            os.remove(sql_file)
        except:
            pass
        run_command(f"docker exec postgres_target rm /tmp/{sql_file}")
        
        if result and result.returncode == 0:
            print(f" Created Source index: {index_name}")
        else:
            print(f" Failed to create Source index: {index_name}")
            if result:
                print(f"   Error: {result.stderr}")
                print(f"   SQL: {create_index_sql}")
            success = False
    
    return success

def check_source_referenced_table_exists(ref_table):
    """Check if referenced table exists in PostgreSQL for Source foreign keys"""
    # Source references: Company
    table_name = ref_table if PRESERVE_MYSQL_CASE else ref_table.lower()
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'{table_name}\' AND table_schema = \'public\';"'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            count = int(result.stdout.strip())
            return count > 0
        except:
            return False
    return False

def create_source_foreign_keys(foreign_keys):
    """Create foreign keys for Source table"""
    if not foreign_keys:
        print(f" No foreign keys to create for {TABLE_NAME}")
        return True
    
    print(f" Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    
    created = 0
    skipped = 0
    
    for fk in foreign_keys:
        ref_table = fk['ref_table']
        
        # Check if referenced table exists
        if not check_source_referenced_table_exists(ref_table):
            print(f" Skipping Source FK {fk['name']}: Referenced table '{ref_table}' does not exist")
            skipped += 1
            continue
        
        constraint_name = fk['name']
        local_cols = fk['local_columns'].replace('`', '"')
        ref_cols = fk['ref_columns'].replace('`', '"')
        
        # Handle case preservation for referenced table
        if PRESERVE_MYSQL_CASE:
            ref_table_name = f'"{ref_table}"'
        else:
            ref_table_name = ref_table.lower()
        
        on_delete = fk['on_delete']
        on_update = fk['on_update']
        
        # Validate actions
        if on_delete not in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
            on_delete = 'RESTRICT'
        if on_update not in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
            on_update = 'RESTRICT'
        
        table_ref = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        alter_sql = f"""
ALTER TABLE {table_ref}
ADD CONSTRAINT {constraint_name}
FOREIGN KEY ({local_cols})
REFERENCES {ref_table_name} ({ref_cols})
ON DELETE {on_delete}
ON UPDATE {on_update};
"""
        print(f" Creating Source FK: {constraint_name} -> {ref_table}")
        
        # Write to file and execute
        sql_file = f"create_source_fk_{constraint_name}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(alter_sql)
        
        # Copy and execute
        copy_cmd = f"docker cp {sql_file} postgres_target:/tmp/{sql_file}"
        run_command(copy_cmd)
        
        exec_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{sql_file}'
        result = run_command(exec_cmd)
        
        # Cleanup
        try:
            os.remove(sql_file)
        except:
            pass
        run_command(f"docker exec postgres_target rm /tmp/{sql_file}")
        
        if result and result.returncode == 0:
            print(f" Created Source FK: {constraint_name}")
            created += 1
        else:
            print(f" Failed to create Source FK {constraint_name}: {result.stderr if result else 'Unknown error'}")
    
    print(f" Source Foreign Keys: {created} created, {skipped} skipped")
    return True

def migrate_source_phase1():
    """Phase 1: Create Source table and import data"""
    print(f" Phase 1: Creating Source table and importing data")
    
    mysql_ddl, indexes, foreign_keys = get_source_table_info()
    if not mysql_ddl:
        return False
    
    postgres_ddl = convert_source_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    
    print(f" Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    if not create_postgresql_table(TABLE_NAME, postgres_ddl, preserve_case=PRESERVE_MYSQL_CASE):
        return False
    
    cleaned_data = export_and_clean_mysql_data(TABLE_NAME)
    if cleaned_data is None:
        return False
    
    if not import_data_to_postgresql(TABLE_NAME, cleaned_data, preserve_case=PRESERVE_MYSQL_CASE, include_id=True):
        return False

    # Add PRIMARY KEY constraint if not exists
    add_primary_key_constraint(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
    
    # Setup auto-increment sequence for preserved IDs
    if not setup_auto_increment_sequence(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE):
        print(" Warning: Could not setup auto-increment sequence")

    print(f" Phase 1 complete for {TABLE_NAME}")
    return True

def migrate_source_phase2():
    """Phase 2: Create indexes for Source table"""
    print(f" Phase 2: Creating indexes for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_source_table_info()
    if not mysql_ddl:
        return False
    
    return create_source_indexes(indexes)

def migrate_source_phase3():
    """Phase 3: Create foreign keys for Source table"""
    print(f" Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_source_table_info()
    if not mysql_ddl:
        return False
    
    return create_source_foreign_keys(foreign_keys)

def migrate_source_full():
    """Full migration: All phases in sequence"""
    print(f" Full Source migration: Running all phases")
    
    if not migrate_source_phase1():
        print(f" Phase 1 failed for {TABLE_NAME}")
        return False
    
    if not migrate_source_phase2():
        print(f" Phase 2 failed for {TABLE_NAME}")
        return False
    
    if not migrate_source_phase3():
        print(f" Phase 3 failed for {TABLE_NAME}")
        return False
    
    print(f" Full Source migration completed successfully!")
    return True

def verify_source_migration():
    """Verify Source table migration"""
    print(f" Verifying table structure for {TABLE_NAME}")
    return verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)

def main():
    """Main entry point for Source migration script"""
    parser = argparse.ArgumentParser(description=f'Migrate {TABLE_NAME} table from MySQL to PostgreSQL')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], help='Run specific migration phase')
    parser.add_argument('--full', action='store_true', help='Run all migration phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration results')
    
    args = parser.parse_args()
    
    if args.phase == 1:
        success = migrate_source_phase1()
    elif args.phase == 2:
        success = migrate_source_phase2()
    elif args.phase == 3:
        success = migrate_source_phase3()
    elif args.full:
        success = migrate_source_full()
    elif args.verify:
        success = verify_source_migration()
    else:
        print("Please specify --phase=1|2|3, --full, or --verify")
        return 1
    
    if success:
        print(" Operation completed successfully!")
        return 0
    else:
        print(" Operation failed!")
        return 1

if __name__ == "__main__":
    exit(main())
