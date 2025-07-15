#!/usr/bin/env python3
"""
Fleet Table Migration Script
=============================

This script provides a complete 3-phase migration approach specifically for the Fleet table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles Fleet-specific data types and constraints
- Manages foreign key dependencies (Company, User, Source, VehicleColor)
- Creates appropriate indexes for Fleet table
- Large table with customer/fleet data

Usage: 
    python fleet_migration.py --phase=1
    python fleet_migration.py --phase=2
    python fleet_migration.py --phase=3
    python fleet_migration.py --full
    python fleet_migration.py --verify
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
    setup_auto_increment_sequence,
    execute_postgresql_sql
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "Fleet"

def get_fleet_table_info():
    """Get complete Fleet table information from MySQL including constraints"""
    print(f" Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f" Failed to get {TABLE_NAME} table info from MySQL")
        return None, [], []
    
    # Extract DDL
    lines = result.stdout.strip().split('\n')
    ddl_line = None
    for line in lines:
        # Look for lines containing CREATE TABLE (could be in second column)
        if 'CREATE TABLE' in line:
            # Split by tab and take the part with CREATE TABLE
            parts = line.split('\t')
            for part in parts:
                if 'CREATE TABLE' in part:
                    ddl_line = part
                    break
            if ddl_line:
                break
    
    if not ddl_line:
        print(f" Could not find CREATE TABLE statement for {TABLE_NAME}")
        print("Debug: MySQL output:")
        print(result.stdout)
        return None, [], []
    
    mysql_ddl = ddl_line.strip()
    
    # Extract indexes and foreign keys
    indexes = extract_fleet_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_fleet_foreign_keys_from_ddl(mysql_ddl)
    
    print(f" Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_fleet_indexes_from_ddl(ddl):
    """Extract index definitions from Fleet table MySQL DDL"""
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
            'table': 'Fleet'
        })
    
    return indexes

def extract_fleet_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from Fleet table MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY specific to Fleet - handle multi-word actions like "SET NULL"
    fk_pattern = r'CONSTRAINT\s+`([^`]+)`\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`([^`]+)`\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+([A-Z][A-Z\s]*?)(?=\s+ON|\s*$))?(?:\s+ON\s+UPDATE\s+([A-Z][A-Z\s]*?)(?=\s*$|\s*,))?'
    
    matches = re.finditer(fk_pattern, ddl, re.IGNORECASE)
    for match in matches:
        constraint_name = match.group(1)
        local_columns = match.group(2)
        ref_table = match.group(3)
        ref_columns = match.group(4)
        on_delete = match.group(5).strip() if match.group(5) else 'RESTRICT'
        on_update = match.group(6).strip() if match.group(6) else 'RESTRICT'
        
        foreign_keys.append({
            'name': constraint_name,
            'local_columns': local_columns,
            'ref_table': ref_table,
            'ref_columns': ref_columns,
            'on_delete': on_delete,
            'on_update': on_update,
            'original': match.group(0),
            'table': 'Fleet'
        })
    
    return foreign_keys

def convert_fleet_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert Fleet table MySQL DDL to PostgreSQL DDL with Fleet-specific optimizations"""
    print(f" Converting Fleet table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Fix literal \n characters to actual newlines first
    postgres_ddl = mysql_ddl.replace('\\n', '\n')
    
    # Extract just the column definitions part
    create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)\)\s*ENGINE', postgres_ddl, re.DOTALL)
    if not create_match:
        print(f" Could not parse CREATE TABLE statement for {TABLE_NAME}")
        return None
    
    columns_part = create_match.group(1)
    
    # Parse individual columns, indexes, and constraints
    lines = []
    for line in columns_part.split(',\n'):
        line = line.strip()
        if not line:
            continue
            
        # Skip constraints for now if include_constraints is False
        if not include_constraints and (
            line.startswith('PRIMARY KEY') or 
            line.startswith('KEY') or 
            line.startswith('UNIQUE KEY') or 
            line.startswith('CONSTRAINT')
        ):
            continue
            
        # Process column definitions
        if not (line.startswith('PRIMARY KEY') or line.startswith('KEY') or 
                line.startswith('UNIQUE KEY') or line.startswith('CONSTRAINT')):
            # This is a column definition
            processed_line = process_fleet_column_definition(line, preserve_case)
            if processed_line:
                lines.append(processed_line)
    
    # Build the PostgreSQL DDL
    table_name_pg = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    postgres_ddl = f"CREATE TABLE {table_name_pg} (\n"
    postgres_ddl += ",\n".join([f"  {line}" for line in lines])
    postgres_ddl += "\n)"
    
    return postgres_ddl

def process_fleet_column_definition(line, preserve_case):
    """Process a single column definition for Fleet table"""
    # Remove backticks and handle MySQL-specific types
    line = line.replace('`', '"' if preserve_case else '')
    
    # MySQL to PostgreSQL type conversions for Fleet
    conversions = [
        (r'\btinyint\(1\)\b', 'BOOLEAN'),
        (r'\btinyint\([^)]+\)\b', 'SMALLINT'),
        (r'\bsmallint\([^)]+\)\b', 'SMALLINT'),
        (r'\bmediumint\([^)]+\)\b', 'INTEGER'),
        (r'\bint\([^)]+\)\b', 'INTEGER'),
        (r'\bbigint\([^)]+\)\b', 'BIGINT'),
        (r'\bint\b', 'INTEGER'),
        (r'\bvarchar\([^)]+\)\b', 'VARCHAR'),
        (r'\btext\b', 'TEXT'),
        (r'\blongtext\b', 'TEXT'),
        (r'\bmediumtext\b', 'TEXT'),
        (r'\btinytext\b', 'TEXT'),
        (r'\bdatetime\([^)]+\)\b', 'TIMESTAMP'),
        (r'\bdatetime\b', 'TIMESTAMP'),
        (r'\btimestamp\([^)]+\)\b', 'TIMESTAMP'),
        (r'\btimestamp\b', 'TIMESTAMP'),
        (r'\bdate\b', 'DATE'),
        (r'\btime\b', 'TIME'),
        (r'\bdouble\b', 'DOUBLE PRECISION'),
        (r'\bfloat\b', 'REAL'),
        (r'\bdecimal\([^)]+\)\b', 'DECIMAL'),
        (r'\bjson\b', 'JSON'),
        (r'\bblob\b', 'BYTEA'),
        (r'\blongblob\b', 'BYTEA'),
        (r'\bmediumblob\b', 'BYTEA'),
        (r'\btinyblob\b', 'BYTEA'),
    ]
    
    for pattern, replacement in conversions:
        line = re.sub(pattern, replacement, line, flags=re.IGNORECASE)
    
    # Additional manual fixes for common issues
    line = line.replace("tinyint(1)", "BOOLEAN")  # Force tinyint(1) to BOOLEAN
    line = line.replace("tinyint", "SMALLINT")    # Any other tinyint to SMALLINT
    
    # Handle AUTO_INCREMENT
    line = re.sub(r'\bAUTO_INCREMENT\b', '', line, flags=re.IGNORECASE)
    
    # Handle MySQL DEFAULT expressions
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP\(\d*\)", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    
    # Handle MySQL character set and collation
    line = re.sub(r'\s+CHARACTER\s+SET\s+[^\s]+', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s+COLLATE\s+[^\s]+', '', line, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    line = re.sub(r'\s+', ' ', line).strip()
    
    return line

def create_fleet_table(mysql_ddl):
    """Create Fleet table in PostgreSQL"""
    postgres_ddl = convert_fleet_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    if not postgres_ddl:
        return False
    
    print(f" Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    return create_postgresql_table(TABLE_NAME, postgres_ddl, PRESERVE_MYSQL_CASE)

def create_fleet_indexes(indexes):
    """Create indexes for Fleet table"""
    if not indexes:
        print(f" No indexes to create for {TABLE_NAME}")
        return True
    
    print(f" Creating {len(indexes)} indexes for {TABLE_NAME}...")
    
    success = True
    for index in indexes:
        index_name = f"{TABLE_NAME.lower()}_{index['name']}"
        columns = index['columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        table_name = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        
        # Check if index already exists
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT indexname FROM pg_indexes WHERE tablename = \'{TABLE_NAME}\' AND indexname = \'{index_name}\';"'
        check_result = run_command(check_cmd)
        
        if check_result and check_result.returncode == 0 and check_result.stdout.strip():
            print(f" Skipping existing index: {index_name}")
            continue
        
        unique_clause = "UNIQUE " if index.get('unique', False) else ""
        index_sql = f'CREATE {unique_clause}INDEX "{index_name}" ON {table_name} ({columns});'
        
        print(f" Creating {TABLE_NAME} index: {index['name']}")
        success_flag, result = execute_postgresql_sql(index_sql, f"{TABLE_NAME} index {index['name']}")
        
        if success_flag and result and "CREATE INDEX" in result.stdout:
            print(f" Created {TABLE_NAME} index: {index['name']}")
        else:
            error_msg = result.stderr if result else "No result"
            print(f" Failed to create {TABLE_NAME} index {index['name']}: {error_msg}")
            success = False
    
    return success

def create_fleet_foreign_keys(foreign_keys):
    """Create foreign keys for Fleet table"""
    if not foreign_keys:
        print(f" No foreign keys to create for {TABLE_NAME}")
        return True
    
    print(f" Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    
    created = 0
    skipped = 0
    
    for fk in foreign_keys:
        constraint_name = f"{TABLE_NAME}_{fk['name']}"
        local_cols = fk['local_columns'].replace('`', '"')
        ref_table = f'"{fk["ref_table"]}"' if PRESERVE_MYSQL_CASE else fk['ref_table']
        ref_cols = fk['ref_columns'].replace('`', '"')
        
        # Check if foreign key already exists
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = \'{TABLE_NAME}\' AND constraint_name = \'{constraint_name}\' AND constraint_type = \'FOREIGN KEY\';"'
        check_result = run_command(check_cmd)
        
        if check_result and check_result.returncode == 0 and check_result.stdout.strip():
            print(f" Skipping existing FK: {constraint_name}")
            skipped += 1
            continue
        
        fk_sql = f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ({local_cols}) REFERENCES {ref_table} ({ref_cols}) ON DELETE {fk["on_delete"]} ON UPDATE {fk["on_update"]};'
        
        print(f" Creating {TABLE_NAME} FK: {constraint_name} -> {fk['ref_table']}")
        success_flag, result = execute_postgresql_sql(fk_sql, f"{TABLE_NAME} FK {constraint_name}")
        
        if success_flag and result and "ALTER TABLE" in result.stdout:
            print(f" Created {TABLE_NAME} FK: {constraint_name}")
            created += 1
        else:
            error_msg = result.stderr if result else "No result"
            print(f" Failed to create {TABLE_NAME} FK {constraint_name}: {error_msg}")
    
    print(f" {TABLE_NAME} Foreign Keys: {created} created, {skipped} skipped")
    return True

def main():
    parser = argparse.ArgumentParser(description=f'Migrate {TABLE_NAME} table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Run specific phase')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure')
    args = parser.parse_args()
    
    if args.verify:
        print(f" Verifying table structure for {TABLE_NAME}")
        verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return
    
    if not any([args.phase, args.full]):
        print("Please specify --phase, --full, or --verify")
        return
    
    # Get table information
    mysql_ddl, indexes, foreign_keys = get_fleet_table_info()
    if not mysql_ddl:
        return
    
    success = True
    
    if args.phase == '1' or args.full:
        print(f" Phase 1: Creating {TABLE_NAME} table and importing data")
        if not create_fleet_table(mysql_ddl):
            success = False
        else:
            data_indicator = export_and_clean_mysql_data(TABLE_NAME)
            import_data_to_postgresql(TABLE_NAME, data_indicator, PRESERVE_MYSQL_CASE, include_id=True)
            add_primary_key_constraint(TABLE_NAME, PRESERVE_MYSQL_CASE)
            setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE)
            print(f" Phase 1 complete for {TABLE_NAME}")
    
    if args.phase == '2' or args.full:
        print(f" Phase 2: Creating indexes for {TABLE_NAME}")
        if not create_fleet_indexes(indexes):
            success = False
    
    if args.phase == '3' or args.full:
        print(f" Phase 3: Creating foreign keys for {TABLE_NAME}")
        if not create_fleet_foreign_keys(foreign_keys):
            success = False
    
    if success:
        print(" Operation completed successfully!")
    else:
        print(" Operation completed with errors!")

if __name__ == "__main__":
    main()
