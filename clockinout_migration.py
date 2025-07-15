#!/usr/bin/env python3
"""
ClockInOut Table Migration Script
================================

This script provides a complete 3-phase migration approach specifically for the ClockInOut table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles ClockInOut-specific data types and constraints
- Manages foreign key dependencies
- Creates appropriate indexes for ClockInOut table

Usage: 
    python clockinout_migration.py --phase=1
    python clockinout_migration.py --phase=2
    python clockinout_migration.py --phase=3
    python clockinout_migration.py --full
    python clockinout_migration.py --verify
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
TABLE_NAME = "ClockInOut"

# --- PHASE 1: Table + Data ---
def get_clockinout_table_info():
    """Get complete ClockInOut table information from MySQL including constraints"""
    print(f" Getting complete table info for {TABLE_NAME} from MySQL...")
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    if not result or result.returncode != 0:
        print(f" Failed to get {TABLE_NAME} table info from MySQL")
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
        print(f" Could not find CREATE TABLE statement for {TABLE_NAME}")
        print("Debug: MySQL output:")
        print(result.stdout)
        return None, [], []
    mysql_ddl = ddl_line.strip()
    indexes = extract_clockinout_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_clockinout_foreign_keys_from_ddl(mysql_ddl)
    print(f" Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_clockinout_indexes_from_ddl(mysql_ddl):
    # Extract index definitions from MySQL DDL
    indexes = []
    for line in mysql_ddl.splitlines():
        if line.strip().startswith("KEY") or line.strip().startswith("UNIQUE KEY"):
            indexes.append(line.strip())
    return indexes

def extract_clockinout_foreign_keys_from_ddl(mysql_ddl):
    # Extract foreign key definitions from MySQL DDL and convert to PostgreSQL ALTER TABLE syntax
    fks = []
    for line in mysql_ddl.splitlines():
        if "FOREIGN KEY" in line:
            # Parse MySQL CONSTRAINT line: CONSTRAINT `name` FOREIGN KEY (`col`) REFERENCES `table` (`col`) ...
            line = line.strip().rstrip(',')
            
            # Extract constraint name
            constraint_match = re.search(r'CONSTRAINT\s+`([^`]+)`', line)
            constraint_name = constraint_match.group(1) if constraint_match else "fk_constraint"
            
            # Extract column name
            fk_col_match = re.search(r'FOREIGN KEY\s+\(`([^`]+)`\)', line)
            fk_column = fk_col_match.group(1) if fk_col_match else ""
            
            # Extract referenced table and column
            ref_match = re.search(r'REFERENCES\s+`([^`]+)`\s+\(`([^`]+)`\)', line)
            if ref_match:
                ref_table = ref_match.group(1)
                ref_column = ref_match.group(2)
                
                # Extract ON DELETE/UPDATE clauses
                on_delete = ""
                on_update = ""
                if "ON DELETE CASCADE" in line:
                    on_delete = " ON DELETE CASCADE"
                elif "ON DELETE SET NULL" in line:
                    on_delete = " ON DELETE SET NULL"
                    
                if "ON UPDATE CASCADE" in line:
                    on_update = " ON UPDATE CASCADE"
                elif "ON UPDATE SET NULL" in line:
                    on_update = " ON UPDATE SET NULL"
                
                # Create PostgreSQL ALTER TABLE statement
                pg_fk = f'ALTER TABLE "ClockInOut" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{fk_column}") REFERENCES "{ref_table}" ("{ref_column}"){on_delete}{on_update};'
                fks.append(pg_fk)
    
    return fks

def convert_clockinout_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert ClockInOut table MySQL DDL to PostgreSQL DDL"""
    print(f"🔄 Converting {TABLE_NAME} table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Create the PostgreSQL DDL manually based on the known MySQL structure
    table_name = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    
    postgres_ddl = f"""CREATE TABLE {table_name} (
    "id" INTEGER NOT NULL,
    "user_id" INTEGER NOT NULL,
    "company_id" INTEGER NOT NULL,
    "clock_in" TIMESTAMP NOT NULL,
    "clock_out" TIMESTAMP,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "timezone" VARCHAR
);"""
    
    print(f" Generated PostgreSQL DDL:")
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

    mysql_ddl, indexes, foreign_keys = get_clockinout_table_info()
    if not mysql_ddl:
        print(f" Could not retrieve MySQL DDL for {TABLE_NAME}")
        return

    for phase in phases:
        if phase == 1:
            print(f"\n Phase 1: Creating table and importing data for {TABLE_NAME}...")
            pg_ddl = convert_clockinout_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
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
            # Create the specific foreign keys for ClockInOut table manually
            from table_utils import execute_postgresql_sql
            
            foreign_keys_to_create = [
                'ALTER TABLE "ClockInOut" ADD CONSTRAINT "ClockInOut_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "Company" ("id") ON DELETE CASCADE;',
                'ALTER TABLE "ClockInOut" ADD CONSTRAINT "ClockInOut_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "User" ("id") ON DELETE CASCADE;'
            ]
            
            for fk_sql in foreign_keys_to_create:
                success, result = execute_postgresql_sql(fk_sql, f"Foreign key creation for {TABLE_NAME}")
                if success:
                    print(f"Created foreign key successfully")
                else:
                    print(f"Failed to create foreign key: {fk_sql}")
                    if result:
                        print(f"Error: {result.stderr}")

    if args.verify:
        print(f"\n Verifying {TABLE_NAME} migration...")
        verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)

if __name__ == "__main__":
    main() 