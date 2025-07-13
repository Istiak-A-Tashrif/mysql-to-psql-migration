#!/usr/bin/env python3
"""
VehicleColor Table Migration Script
===================================

This script provides a complete 3-phase migration approach specifically for the VehicleColor table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles VehicleColor-specific data types and constraints
- Manages foreign key dependencies (Company)
- Creates appropriate indexes for VehicleColor table
- Simple table structure with no ENUMs needed

Usage: 
    python vehiclecolor_migration.py --phase=1
    python vehiclecolor_migration.py --phase=2
    python vehiclecolor_migration.py --phase=3
    python vehiclecolor_migration.py --full
    python vehiclecolor_migration.py --verify
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
TABLE_NAME = "VehicleColor"

def get_vehiclecolor_table_info():
    """Get complete VehicleColor table information from MySQL including constraints"""
    print(f"🔍 Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get MySQL table structure for {TABLE_NAME}: {result.stderr if result else 'No result'}")
        return None, [], []
    
    # Parse the output - look for the line containing CREATE TABLE
    lines = result.stdout.split('\n')
    mysql_ddl = None
    
    for line in lines:
        if 'CREATE TABLE' in line:
            # Extract DDL from tab-separated output
            parts = line.split('\t')
            if len(parts) >= 2:
                mysql_ddl = parts[1]
                break
    
    if not mysql_ddl:
        print(f"❌ Could not find CREATE TABLE statement for {TABLE_NAME}")
        return None, [], []
    
    # Extract indexes and foreign keys
    indexes = extract_vehiclecolor_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_vehiclecolor_foreign_keys_from_ddl(mysql_ddl)
    
    print(f"✅ Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_vehiclecolor_indexes_from_ddl(ddl):
    """Extract index definitions from VehicleColor table MySQL DDL"""
    indexes = []
    
    # Pattern for KEY definitions
    key_pattern = r'(?:UNIQUE\s+)?KEY\s+`([^`]+)`\s*\(([^)]+)\)'
    
    matches = re.finditer(key_pattern, ddl, re.IGNORECASE)
    for match in matches:
        index_name = match.group(1)
        columns = match.group(2)
        is_unique = 'UNIQUE' in match.group(0).upper()
        
        indexes.append({
            'name': index_name,
            'columns': columns,
            'unique': is_unique,
            'original': match.group(0),
            'table': 'VehicleColor'
        })
    
    return indexes

def extract_vehiclecolor_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from VehicleColor table MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY specific to VehicleColor
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
            'table': 'VehicleColor'
        })
    
    return foreign_keys

def convert_vehiclecolor_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert VehicleColor table MySQL DDL to PostgreSQL DDL with VehicleColor-specific optimizations"""
    print(f"🔄 Converting VehicleColor table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Fix literal \n characters to actual newlines first
    postgres_ddl = mysql_ddl.replace('\\n', '\n')
    
    # Extract just the column definitions part
    # Match everything between CREATE TABLE ... ( and the first ) that ends column definitions
    create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)\)\s*ENGINE', postgres_ddl, re.DOTALL)
    if not create_match:
        # Fallback - try to find just the parentheses content
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
        line = re.sub(r'\bdecimal\(\d+,\d+\)', 'DECIMAL', line, flags=re.IGNORECASE)
        line = re.sub(r'\bdatetime\(\d+\)', 'TIMESTAMP', line, flags=re.IGNORECASE)
        line = re.sub(r'\benum\([^)]+\)', 'VARCHAR(50)', line, flags=re.IGNORECASE)
        
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
    table_name = '"VehicleColor"' if preserve_case else 'vehiclecolor'
    postgres_ddl = f"CREATE TABLE {table_name} (\n"
    postgres_ddl += ',\n'.join(f"  {line}" for line in column_lines)
    postgres_ddl += "\n)"
    
    return postgres_ddl

def create_vehiclecolor_indexes(indexes):
    """Create indexes for VehicleColor table"""
    if not indexes:
        print(f"ℹ️ No indexes to create for {TABLE_NAME}")
        return True
    
    print(f"📊 Creating {len(indexes)} indexes for {TABLE_NAME}...")
    
    success = True
    created_indexes = set()  # Track created index names to avoid duplicates
    
    for index in indexes:
        index_name = f"{TABLE_NAME.lower()}_{index['name']}"
        
        # Skip duplicates (can happen with UNIQUE variants)
        if index_name in created_indexes:
            print(f"⚠️ Skipping duplicate index: {index_name}")
            continue
            
        created_indexes.add(index_name)
        columns = index['columns'].replace('`', '"')  # Convert backticks to quotes for case preservation
        unique_clause = "UNIQUE " if index['unique'] else ""
        
        # For case-sensitive table, quote the table name
        table_ref = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        
        create_index_sql = f"CREATE {unique_clause}INDEX {index_name} ON {table_ref} ({columns});"
        
        print(f"🔧 Creating VehicleColor index: {index_name}")
        
        # Write to file and execute
        sql_file = f"create_vehiclecolor_index_{index_name}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(create_index_sql)
        
        # Copy and execute
        copy_cmd = f'docker cp "{sql_file}" postgres_target:/tmp/'
        copy_result = run_command(copy_cmd)
        if not copy_result:
            print(f"❌ Failed to copy {sql_file} to container")
            success = False
            continue
            
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{sql_file}'
        result = run_command(cmd)
        
        # Clean up
        try:
            os.remove(sql_file)
        except:
            pass
            
        if result and "CREATE INDEX" in str(result):
            print(f"✅ Created VehicleColor index: {index_name}")
        else:
            print(f"❌ Failed to create VehicleColor index {index_name}: {result}")
            success = False
    
    return success

def create_vehiclecolor_foreign_keys(foreign_keys):
    """Create foreign keys for VehicleColor table"""
    if not foreign_keys:
        print(f"ℹ️ No foreign keys to create for {TABLE_NAME}")
        return True
    
    print(f"🔗 Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    
    created = 0
    skipped = 0
    
    for fk in foreign_keys:
        constraint_name = f"{TABLE_NAME}_{fk['name']}"
        local_cols = fk['local_columns'].replace('`', '"')
        ref_table = f'"{fk["ref_table"]}"' if PRESERVE_MYSQL_CASE else fk['ref_table']
        ref_cols = fk['ref_columns'].replace('`', '"')
        
        # Build the foreign key SQL
        fk_sql = f'''ALTER TABLE "{TABLE_NAME}" 
                     ADD CONSTRAINT "{constraint_name}" 
                     FOREIGN KEY ({local_cols}) 
                     REFERENCES {ref_table} ({ref_cols})
                     ON DELETE {fk['on_delete']} 
                     ON UPDATE {fk['on_update']};'''
        
        print(f"🔧 Creating VehicleColor FK: {constraint_name} -> {fk['ref_table']}")
        
        # Write to file and execute
        sql_file = f"create_vehiclecolor_fk_{constraint_name}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(fk_sql)
        
        # Copy and execute
        copy_cmd = f'docker cp "{sql_file}" postgres_target:/tmp/'
        copy_result = run_command(copy_cmd)
        if not copy_result:
            print(f"❌ Failed to copy {sql_file} to container")
            continue
            
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{sql_file}'
        result = run_command(cmd)
        
        # Clean up
        try:
            os.remove(sql_file)
        except:
            pass
            
        if result and "ALTER TABLE" in str(result):
            print(f"✅ Created VehicleColor FK: {constraint_name}")
            created += 1
        else:
            print(f"❌ Failed to create VehicleColor FK {constraint_name}: {result.stderr if result else 'Unknown error'}")
    
    print(f"🎯 VehicleColor Foreign Keys: {created} created, {skipped} skipped")
    return True

def migrate_vehiclecolor_phase1():
    """Phase 1: Create VehicleColor table and import data"""
    print(f"🚀 Phase 1: Creating VehicleColor table and importing data")
    
    mysql_ddl, indexes, foreign_keys = get_vehiclecolor_table_info()
    if not mysql_ddl:
        return False
    
    postgres_ddl = convert_vehiclecolor_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    
    print(f"📋 Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    # Create table
    success = create_postgresql_table(TABLE_NAME, postgres_ddl, preserve_case=PRESERVE_MYSQL_CASE)
    if not success:
        return False
    
    # Import data
    success = export_and_clean_mysql_data(TABLE_NAME)
    if not success:
        return False
    
    success = import_data_to_postgresql(TABLE_NAME, "VehicleColor data", preserve_case=PRESERVE_MYSQL_CASE, include_id=True)
    if not success:
        return False
    
    # Add primary key constraint (since we excluded constraints from DDL)
    success = add_primary_key_constraint(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
    if not success:
        return False
    
    # Setup auto-increment sequence
    success = setup_auto_increment_sequence(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
    if not success:
        return False
        
    print(f"✅ Phase 1 complete for {TABLE_NAME}")
    return True

def migrate_vehiclecolor_phase2():
    """Phase 2: Create indexes for VehicleColor table"""
    print(f"📊 Phase 2: Creating indexes for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_vehiclecolor_table_info()
    if not mysql_ddl:
        return False
    
    return create_vehiclecolor_indexes(indexes)

def migrate_vehiclecolor_phase3():
    """Phase 3: Create foreign keys for VehicleColor table"""
    print(f"🔗 Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_vehiclecolor_table_info()
    if not mysql_ddl:
        return False
    
    return create_vehiclecolor_foreign_keys(foreign_keys)

def main():
    parser = argparse.ArgumentParser(description='Migrate VehicleColor table from MySQL to PostgreSQL')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"🔍 Verifying table structure for {TABLE_NAME}")
        success = verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
        if success:
            print("🎉 Operation completed successfully!")
        else:
            print("❌ Operation failed!")
            exit(1)
        return
    
    if args.full:
        print(f"🚀 Running full migration for {TABLE_NAME}")
        success = (migrate_vehiclecolor_phase1() and 
                  migrate_vehiclecolor_phase2() and 
                  migrate_vehiclecolor_phase3())
    elif args.phase == 1:
        success = migrate_vehiclecolor_phase1()
    elif args.phase == 2:
        success = migrate_vehiclecolor_phase2()
    elif args.phase == 3:
        success = migrate_vehiclecolor_phase3()
    else:
        parser.print_help()
        return
    
    if success:
        print("🎉 Operation completed successfully!")
    else:
        print("❌ Operation failed!")
        exit(1)

if __name__ == "__main__":
    main()
