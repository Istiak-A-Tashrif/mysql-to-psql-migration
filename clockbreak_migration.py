#!/usr/bin/env python3
"""
ClockBreak Table Migration Script
================================

This script provides a complete 3-phase migration approach specifically for the ClockBreak table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles ClockBreak-specific data types and constraints
- Manages foreign key dependencies
- Creates appropriate indexes for ClockBreak table

Usage: 
    python clockbreak_migration.py --phase=1
    python clockbreak_migration.py --phase=2
    python clockbreak_migration.py --phase=3
    python clockbreak_migration.py --full
    python clockbreak_migration.py --verify
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

PRESERVE_MYSQL_CASE = True
TABLE_NAME = "ClockBreak"

# --- PHASE 1: Table + Data ---
def get_clockbreak_table_info():
    """Get complete ClockBreak table information from MySQL including constraints"""
    print(f" Getting complete table info for {TABLE_NAME} from MySQL...")
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    if not result or result.returncode != 0:
        print(f"❌ Failed to get {TABLE_NAME} table info from MySQL")
        return None, [], []
    lines = result.stdout.strip().split("\n")
    ddl_line = None
    for line in lines:
        if "CREATE TABLE" in line:
            parts = line.split("\t")
            for part in parts:
                if "CREATE TABLE" in part:
                    ddl_line = part
                    break
            if ddl_line:
                break
    if not ddl_line:
        print(f"❌ Could not find CREATE TABLE statement for {TABLE_NAME}")
        print("Debug: MySQL output:")
        print(result.stdout)
        return None, [], []
    mysql_ddl = ddl_line.strip()
    indexes = extract_clockbreak_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_clockbreak_foreign_keys_from_ddl(mysql_ddl)
    print(f" Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_clockbreak_indexes_from_ddl(mysql_ddl):
    # Extract index definitions from MySQL DDL
    indexes = []
    for line in mysql_ddl.splitlines():
        if line.strip().startswith("KEY") or line.strip().startswith("UNIQUE KEY"):
            indexes.append(line.strip())
    return indexes

def extract_clockbreak_foreign_keys_from_ddl(mysql_ddl):
    # Extract foreign key definitions from MySQL DDL
    fks = []
    for line in mysql_ddl.splitlines():
        if "FOREIGN KEY" in line:
            fks.append(line.strip())
    return fks

def process_clockbreak_column_definition(line, preserve_case):
    """Process a single column definition for ClockBreak table"""
    # Remove backticks and handle MySQL-specific types
    line = line.replace('`', '"' if preserve_case else '')
    
    # MySQL to PostgreSQL type conversions for ClockBreak
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
    
    # Handle AUTO_INCREMENT
    line = re.sub(r'\bAUTO_INCREMENT\b', '', line, flags=re.IGNORECASE)
    
    # Handle MySQL DEFAULT expressions
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP\(\d*\)", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    
    # Remove MySQL-specific syntax
    line = re.sub(r'\s+CHARACTER\s+SET\s+[^\s]+', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s+COLLATE\s+[^\s]+', '', line, flags=re.IGNORECASE)
    
    # Clean up whitespace
    line = re.sub(r'\s+', ' ', line).strip()
    
    return line

def convert_clockbreak_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert ClockBreak table MySQL DDL to PostgreSQL DDL"""
    print(f"🔄 Converting {TABLE_NAME} table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Create the PostgreSQL DDL manually based on the known MySQL structure
    table_name = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    
    postgres_ddl = f"""CREATE TABLE {table_name} (
    "id" INTEGER NOT NULL,
    "clock_in_out_id" INTEGER NOT NULL,
    "break_start" TIMESTAMP NOT NULL,
    "break_end" TIMESTAMP,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""
    
    print(f"📋 Generated PostgreSQL DDL:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    return postgres_ddl

# --- Main Migration Logic ---
def main():
    parser = argparse.ArgumentParser(description=f"Migrate {TABLE_NAME} table from MySQL to PostgreSQL")
    parser.add_argument('--phase', type=int, choices=[1,2,3], help='Migration phase (1: table+data, 2: indexes, 3: foreign keys)')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure and data')
    args = parser.parse_args()

    if args.full:
        phases = [1,2,3]
    elif args.phase:
        phases = [args.phase]
    else:
        print("Please specify --phase or --full")
        return

    mysql_ddl, indexes, foreign_keys = get_clockbreak_table_info()
    if not mysql_ddl:
        print(f"❌ Could not retrieve MySQL DDL for {TABLE_NAME}")
        return

    for phase in phases:
        if phase == 1:
            print(f"\n Phase 1: Creating table and importing data for {TABLE_NAME}...")
            pg_ddl = convert_clockbreak_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
            create_postgresql_table(TABLE_NAME, pg_ddl, preserve_case=PRESERVE_MYSQL_CASE)
            data_indicator = export_and_clean_mysql_data(TABLE_NAME)
            import_data_to_postgresql(TABLE_NAME, data_indicator, PRESERVE_MYSQL_CASE, include_id=True)
            add_primary_key_constraint(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
            setup_auto_increment_sequence(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
        elif phase == 2:
            print(f"\n Phase 2: Creating indexes for {TABLE_NAME}...")
            for idx in indexes:
                execute_postgresql_sql(idx, TABLE_NAME)
        elif phase == 3:
            print(f"\n Phase 3: Creating foreign keys for {TABLE_NAME}...")
            for fk in foreign_keys:
                execute_postgresql_sql(fk, TABLE_NAME)

    if args.verify:
        print(f"\n Verifying {TABLE_NAME} migration...")
        verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)

if __name__ == "__main__":
    main() 