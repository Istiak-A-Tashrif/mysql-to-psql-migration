#!/usr/bin/env python3
"""
CalendarSettings Table Migration Script
======================================

This script provides a complete 3-phase migration approach specifically for the CalendarSettings table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles CalendarSettings-specific data types and constraints
- Manages foreign key dependencies (Company)
- Creates appropriate indexes for CalendarSettings table

Usage: 
    python calendarsettings_migration.py --phase=1
    python calendarsettings_migration.py --phase=2
    python calendarsettings_migration.py --phase=3
    python calendarsettings_migration.py --full
    python calendarsettings_migration.py --verify
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
TABLE_NAME = "CalendarSettings"

def get_calendarsettings_table_info():
    """Get complete CalendarSettings table information from MySQL including constraints"""
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
    indexes = extract_calendarsettings_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_calendarsettings_foreign_keys_from_ddl(mysql_ddl)
    print(f" Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_calendarsettings_indexes_from_ddl(ddl):
    indexes = []
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
            'table': 'CalendarSettings'
        })
    return indexes

def extract_calendarsettings_foreign_keys_from_ddl(ddl):
    foreign_keys = []
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
            'table': 'CalendarSettings'
        })
    return foreign_keys

def convert_calendarsettings_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    print(f" Converting CalendarSettings table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    postgres_ddl = mysql_ddl.replace('\\n', '\n')
    create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)\)\s*ENGINE', postgres_ddl, re.DOTALL)
    if not create_match:
        print(f" Could not parse CREATE TABLE statement for {TABLE_NAME}")
        return None
    columns_part = create_match.group(1)
    lines = []
    for line in columns_part.split(',\n'):
        line = line.strip()
        if not line:
            continue
        if not include_constraints and (
            line.startswith('PRIMARY KEY') or 
            line.startswith('KEY') or 
            line.startswith('UNIQUE KEY') or 
            line.startswith('CONSTRAINT')
        ):
            continue
        if not (line.startswith('PRIMARY KEY') or line.startswith('KEY') or 
                line.startswith('UNIQUE KEY') or line.startswith('CONSTRAINT')):
            processed_line = process_calendarsettings_column_definition(line, preserve_case)
            if processed_line:
                lines.append(processed_line)
    table_name_pg = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    postgres_ddl = f"CREATE TABLE {table_name_pg} (\n"
    postgres_ddl += ",\n".join([f"  {line}" for line in lines])
    postgres_ddl += "\n)"
    return postgres_ddl

def process_calendarsettings_column_definition(line, preserve_case):
    line = line.replace('`', '"' if preserve_case else '')
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
    line = re.sub(r'\bAUTO_INCREMENT\b', '', line, flags=re.IGNORECASE)
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP\(\d*\)", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    line = re.sub(r'\s+CHARACTER\s+SET\s+[^\s]+', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s+COLLATE\s+[^\s]+', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s+', ' ', line).strip()
    return line

def create_calendarsettings_table(mysql_ddl):
    postgres_ddl = convert_calendarsettings_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    if not postgres_ddl:
        return False
    print(f" Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    return create_postgresql_table(TABLE_NAME, postgres_ddl, PRESERVE_MYSQL_CASE)

def create_calendarsettings_indexes(indexes):
    if not indexes:
        print(f" No indexes to create for {TABLE_NAME}")
        return True
    print(f" Creating {len(indexes)} indexes for {TABLE_NAME}...")
    success = True
    for index in indexes:
        index_name = f"{TABLE_NAME.lower()}_{index['name']}"
        columns = index['columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        table_name = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        table_name_for_check = TABLE_NAME if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT indexname FROM pg_indexes WHERE tablename = \'{table_name_for_check}\' AND indexname = \'{index_name}\';"'
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

def create_calendarsettings_foreign_keys(foreign_keys):
    if not foreign_keys:
        print(f" No foreign keys to create for {TABLE_NAME}")
        return True
    print(f" Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    created_count = 0
    skipped_count = 0
    for fk in foreign_keys:
        constraint_name = f"{TABLE_NAME}_{fk['name']}"
        local_columns = fk['local_columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        ref_table = f'"{fk["ref_table"]}"' if PRESERVE_MYSQL_CASE else fk['ref_table'].lower()
        ref_columns = fk['ref_columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        table_name = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        table_name_for_check = TABLE_NAME if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = \'{table_name_for_check}\' AND constraint_type = \'FOREIGN KEY\' AND constraint_name = \'{constraint_name}\';"'
        check_result = run_command(check_cmd)
        if check_result and check_result.returncode == 0 and check_result.stdout.strip():
            print(f" Skipping existing FK: {constraint_name}")
            skipped_count += 1
            continue
        fk_sql = f'ALTER TABLE {table_name} ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ({local_columns}) REFERENCES {ref_table} ({ref_columns}) ON DELETE {fk["on_delete"]} ON UPDATE {fk["on_update"]};'
        print(f" Creating {TABLE_NAME} FK: {constraint_name} -> {fk['ref_table']}")
        success, result = execute_postgresql_sql(fk_sql, f"{TABLE_NAME} FK {constraint_name}")
        if success and result and "ALTER TABLE" in result.stdout:
            print(f" Created {TABLE_NAME} FK: {constraint_name}")
            created_count += 1
        else:
            error_msg = result.stderr if result else "No result"
            print(f" Failed to create {TABLE_NAME} FK {constraint_name}: {error_msg}")
    print(f" {TABLE_NAME} Foreign Keys: {created_count} created, {skipped_count} skipped")
    return True

def phase1_create_table_and_data():
    print(f" Phase 1: Creating {TABLE_NAME} table and importing data")
    mysql_ddl, indexes, foreign_keys = get_calendarsettings_table_info()
    if not mysql_ddl:
        return False
    if not create_calendarsettings_table(mysql_ddl):
        return False
    if not export_and_clean_mysql_data(TABLE_NAME):
        return False
    if not import_data_to_postgresql(TABLE_NAME, "CalendarSettings", PRESERVE_MYSQL_CASE, include_id=True):
        return False
    if not add_primary_key_constraint(TABLE_NAME, PRESERVE_MYSQL_CASE):
        return False
    if not setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE):
        return False
    print(f" Phase 1 complete for {TABLE_NAME}")
    return True

def phase2_create_indexes():
    print(f" Phase 2: Creating indexes for {TABLE_NAME}")
    mysql_ddl, indexes, foreign_keys = get_calendarsettings_table_info()
    if mysql_ddl is None:
        return False
    return create_calendarsettings_indexes(indexes)

def phase3_create_foreign_keys():
    print(f" Phase 3: Creating foreign keys for {TABLE_NAME}")
    mysql_ddl, indexes, foreign_keys = get_calendarsettings_table_info()
    if mysql_ddl is None:
        return False
    return create_calendarsettings_foreign_keys(foreign_keys)

def main():
    parser = argparse.ArgumentParser(description=f'Migrate {TABLE_NAME} table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure and data')
    args = parser.parse_args()
    if args.verify:
        mysql_ddl, indexes, foreign_keys = get_calendarsettings_table_info()
        if mysql_ddl:
            verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return
    if args.full:
        success = (phase1_create_table_and_data() and 
                  phase2_create_indexes() and 
                  phase3_create_foreign_keys())
        if success:
            print(" Operation completed successfully!")
        else:
            print(" Operation failed!")
            exit(1)
        return
    if args.phase == '1':
        if not phase1_create_table_and_data():
            exit(1)
    elif args.phase == '2':
        if not phase2_create_indexes():
            exit(1)
    elif args.phase == '3':
        if not phase3_create_foreign_keys():
            exit(1)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 