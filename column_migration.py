#!/usr/bin/env python3
"""
Column Table Migration Script
============================

This script provides a complete 3-phase migration approach specifically for the Column table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles Column-specific data types and constraints
- Manages foreign key dependencies
- Creates appropriate indexes for Column table

Usage: 
    python column_migration.py --phase=1
    python column_migration.py --phase=2
    python column_migration.py --phase=3
    python column_migration.py --full
    python column_migration.py --verify
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
import csv

PRESERVE_MYSQL_CASE = True
TABLE_NAME = "Column"

def get_column_table_info():
    """Get complete Column table information from MySQL including constraints"""
    print(f"🔍 Getting complete table info for {TABLE_NAME} from MySQL...")
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
    indexes = extract_column_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_column_foreign_keys_from_ddl(mysql_ddl)
    print(f"✅ Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_column_indexes_from_ddl(mysql_ddl):
    indexes = []
    for line in mysql_ddl.splitlines():
        if line.strip().startswith("KEY") or line.strip().startswith("UNIQUE KEY"):
            indexes.append(line.strip())
    return indexes

def extract_column_foreign_keys_from_ddl(mysql_ddl):
    fks = []
    for line in mysql_ddl.splitlines():
        if "FOREIGN KEY" in line:
            fks.append(line.strip())
    return fks

def convert_column_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert Column table MySQL DDL to PostgreSQL DDL"""
    print(f"🔄 Converting {TABLE_NAME} table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Get actual MySQL structure first
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "DESCRIBE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    if result and result.returncode == 0:
        print(f"📋 MySQL structure for {TABLE_NAME}:")
        print(result.stdout)
    
    # Create the PostgreSQL DDL manually based on the known MySQL structure
    table_name = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    
    postgres_ddl = f"""CREATE TABLE {table_name} (
    "id" INTEGER NOT NULL,
    "name" VARCHAR NOT NULL,
    "company_id" INTEGER NOT NULL,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""
    
    print(f"📋 Generated PostgreSQL DDL:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    return postgres_ddl

def get_postgresql_column_table_ddl():
    return '''
CREATE TABLE "Column" (
    "id" INTEGER NOT NULL,
    "title" VARCHAR NOT NULL,
    "type" VARCHAR NOT NULL,
    "order" INTEGER NOT NULL,
    "textColor" VARCHAR,
    "bgColor" VARCHAR,
    "company_id" INTEGER NOT NULL
);
'''

def log_csv_preview(csv_filename, num_lines=5):
    print(f"Preview of {csv_filename} (first {num_lines} lines):")
    if not os.path.exists(csv_filename):
        print(f"  File not found: {csv_filename}")
        return
    with open(csv_filename, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            print(f"  {line.strip()}")
            if i >= num_lines - 1:
                break

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

    mysql_ddl, indexes, foreign_keys = get_column_table_info()
    if not mysql_ddl:
        print(f"❌ Could not retrieve MySQL DDL for {TABLE_NAME}")
        return

    for phase in phases:
        if phase == 1:
            print(f"\n🚦 Phase 1: Creating table and importing data for {TABLE_NAME}...")
            pg_ddl = get_postgresql_column_table_ddl()
            create_postgresql_table(TABLE_NAME, pg_ddl, preserve_case=PRESERVE_MYSQL_CASE)
            data_indicator = export_and_clean_mysql_data(TABLE_NAME)
            import_data_to_postgresql(TABLE_NAME, data_indicator, PRESERVE_MYSQL_CASE, include_id=True)
            add_primary_key_constraint(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
            setup_auto_increment_sequence(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)
        elif phase == 2:
            print(f"\n🚦 Phase 2: Creating indexes for {TABLE_NAME}...")
            for idx in indexes:
                execute_postgresql_sql(idx, TABLE_NAME)
        elif phase == 3:
            print(f"\n🚦 Phase 3: Creating foreign keys for {TABLE_NAME}...")
            for fk in foreign_keys:
                execute_postgresql_sql(fk, TABLE_NAME)

    if args.verify:
        print(f"\n🔍 Verifying {TABLE_NAME} migration...")
        verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)

    # After exporting CSV for Column, log the file and preview
    csv_filename = 'Column_robust_import.csv'
    if os.path.exists(csv_filename):
        with open(csv_filename, 'r', encoding='utf-8') as f:
            row_count = sum(1 for _ in f)
        print(f"Exported CSV row count: {row_count}")
        log_csv_preview(csv_filename)
    else:
        print(f"Exported CSV file not found: {csv_filename}")
    # After import, log the import SQL and any errors
    import_sql_filename = 'import_Column_robust.sql'
    if os.path.exists(import_sql_filename):
        print(f"Import SQL used:")
        with open(import_sql_filename, 'r', encoding='utf-8') as f:
            print(f.read())

if __name__ == "__main__":
    main() 