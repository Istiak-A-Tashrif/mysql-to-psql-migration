#!/usr/bin/env python3
"""
Company Table Migration Script
==============================

This script provides a complete 3-phase migration approach specifically for the Company table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles Company-specific data types and constraints
- Manages foreign key dependencies (TwilioCredentials, MailgunCredential)
- Creates appropriate indexes for Company table

Usage: 
    python company_migration.py --phase=1
    python company_migration.py --phase=2
    python company_migration.py --phase=3
    python company_migration.py --full
    python company_migration.py --verify
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
TABLE_NAME = "Company"

def get_company_table_info():
    """Get complete Company table information from MySQL including constraints"""
    print(f"🔍 Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get Company table structure: {result.stderr if result else 'No result'}")
        return None, None, None

    
    lines = result.stdout.strip().split('\n')
    create_statement = None
    for line in lines[1:]:
        if 'CREATE TABLE' in line:
            parts = line.split('\t')
            if len(parts) >= 2:
                create_statement = parts[1]
                break
    
    if not create_statement:
        print("❌ Could not find CREATE TABLE statement for Company")
        return None, None, None
    
    # Extract different components
    indexes = extract_company_indexes_from_ddl(create_statement)
    foreign_keys = extract_company_foreign_keys_from_ddl(create_statement)
    
    print(f"✅ Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for Company table")
    return create_statement, indexes, foreign_keys

def extract_company_indexes_from_ddl(ddl):
    """Extract index definitions from Company table MySQL DDL"""
    indexes = []
    
    # Find all KEY definitions specific to Company table
    key_patterns = [
        r'KEY\s+`([^`]+)`\s*\(([^)]+)\)',
        r'INDEX\s+`([^`]+)`\s*\(([^)]+)\)',
        r'UNIQUE\s+KEY\s+`([^`]+)`\s*\(([^)]+)\)',
        r'UNIQUE\s+INDEX\s+`([^`]+)`\s*\(([^)]+)\)'
    ]
    
    for pattern in key_patterns:
        matches = re.finditer(pattern, ddl, re.IGNORECASE)
        for match in matches:
            index_name = match.group(1)
            columns = match.group(2)
            is_unique = 'UNIQUE' in match.group(0).upper()
            
            indexes.append({
                'name': index_name,
                'columns': columns,
                'unique': is_unique,
                'original': match.group(0),
                'table': 'Company'
            })
    
    return indexes

def extract_company_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from Company table MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY specific to Company
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
            'table': 'Company'
        })
    
    return foreign_keys

def convert_company_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert Company table MySQL DDL to PostgreSQL DDL with Company-specific optimizations"""
    print(f"🔄 Converting Company table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Company-specific type mappings
    company_type_mappings = OrderedDict([
        # Modified: Don't auto-convert id to SERIAL - preserve original IDs
        (r'\bbigint\(\d+\)\s+auto_increment\b', 'BIGINT'),  # Remove auto-increment, handle manually
        (r'tinyint\(1\)', 'BOOLEAN'),
        (r'tinyint\(\d+\)', 'SMALLINT'),
        (r'smallint\(\d+\)', 'SMALLINT'),
        (r'mediumint\(\d+\)', 'INTEGER'),
        (r'int\(\d+\)', 'INTEGER'),
        (r'bigint\(\d+\)', 'BIGINT'),
        (r'\btinyint\b(?!\()', 'SMALLINT'),
        (r'\bint\(\d+\)\s+auto_increment\b', 'INTEGER'),  # Remove auto-increment, preserve original IDs
        (r'\bint\b(?!\()', 'INTEGER'),
        (r'varchar\((\d+)\)', r'VARCHAR(\1)'),
        (r'char\((\d+)\)', r'CHAR(\1)'),
        (r'text', 'TEXT'),
        (r'longtext', 'TEXT'),
        (r'mediumtext', 'TEXT'),
        (r'tinytext', 'TEXT'),
        (r'\bdatetime\(\d+\)\b', 'TIMESTAMP(3)'),
        (r'\bdatetime\b', 'TIMESTAMP'),
        (r'\btimestamp\b', 'TIMESTAMP'),
        (r'\bdate\b(?=\s|,|\)|\n)', 'DATE'),
        (r'\btime\b(?=\s|,|\)|\n)', 'TIME'),
        (r'decimal\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
        (r'numeric\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
        (r'double', 'DOUBLE PRECISION'),
        (r'float', 'REAL'),
        (r'enum\([^)]+\)', 'VARCHAR(50)'),
        (r'json', 'JSONB'),
        (r'blob', 'BYTEA'),
        (r'longblob', 'BYTEA'),
        # Company-specific: Handle large precision decimals for tax and serviceFee
        (r'decimal\(65,30\)', 'DECIMAL(65,30)'),
    ])
    
    postgres_ddl = mysql_ddl
    
    # Convert table name
    postgres_ddl = re.sub(
        r'CREATE TABLE `([^`]+)`',
        r'CREATE TABLE \1',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    # Apply Company-specific type mappings
    for mysql_pattern, postgres_type in company_type_mappings.items():
        postgres_ddl = re.sub(mysql_pattern, postgres_type, postgres_ddl, flags=re.IGNORECASE)
    
    # Remove MySQL-specific syntax
    postgres_ddl = re.sub(r'\s+unsigned\b', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s+zerofill\b', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'COLLATE [a-zA-Z0-9_]+', '', postgres_ddl)
    postgres_ddl = re.sub(r'CHARACTER SET [a-zA-Z0-9_]+', '', postgres_ddl)
    postgres_ddl = re.sub(r'\s*ENGINE\s*=\s*[a-zA-Z0-9_]+', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s*DEFAULT\s+CHARSET\s*=\s*[a-zA-Z0-9_]+', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s*AUTO_INCREMENT\s*=\s*\d+', '', postgres_ddl, flags=re.IGNORECASE)
    
    if not include_constraints:
        # For phase 1, completely rebuild DDL without constraints
        lines = postgres_ddl.split('\\n')  # Handle escaped newlines
        
        # Use proper table name based on case preservation
        target_table_name = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
        clean_lines = [f'CREATE TABLE {target_table_name} (']
        
        for line in lines:
            line = line.strip()
            
            # Skip constraint, key, and index lines, but keep column definitions
            if (line.startswith('KEY ') or 
                line.startswith('INDEX ') or 
                line.startswith('CONSTRAINT ') or
                line.startswith('UNIQUE KEY ') or
                line.startswith('PRIMARY KEY (') or
                line.startswith('CREATE TABLE') or
                line.startswith(')') or
                not line):
                continue
            
            # This is a column definition if it doesn't start with constraint keywords
            if not any(line.upper().startswith(kw) for kw in ['KEY ', 'INDEX ', 'CONSTRAINT ', 'FOREIGN ', 'UNIQUE ']):
                # Clean up the line - ensure proper comma
                clean_line = line.rstrip(',').strip()
                clean_lines.append('  ' + clean_line + ',')
        
        # Remove trailing comma from last line and close
        if clean_lines[-1].endswith(','):
            clean_lines[-1] = clean_lines[-1][:-1]
        
        # Add PRIMARY KEY constraint for id column (for Company table with preserved IDs)
        if TABLE_NAME == "Company":
            clean_lines.append(',')
            clean_lines.append('  PRIMARY KEY ("id")')
        
        clean_lines.append(')')
        
        postgres_ddl = '\n'.join(clean_lines)
    
    # Clean up PRIMARY KEY definitions that are already handled by SERIAL
    postgres_ddl = re.sub(r',\s*PRIMARY\s+KEY\s*\([^)]+\)', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove MySQL table options
    postgres_ddl = re.sub(r'\)\s*[A-Z_=\s\w\d]+$', ')', postgres_ddl, flags=re.IGNORECASE)
    
    # Handle backticks - preserve case if needed for Company columns
    if preserve_case:
        # Convert MySQL backticks to PostgreSQL double quotes to preserve case
        postgres_ddl = re.sub(r'`([^`]+)`', r'"\1"', postgres_ddl)
    else:
        # Remove backticks for case-insensitive mode
        postgres_ddl = re.sub(r'`([^`]+)`', r'\1', postgres_ddl)
    
    # Fix auto_increment - convert to SERIAL FIRST
    postgres_ddl = re.sub(r'\s+AUTO_INCREMENT\b', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Convert ONLY the id column but keep it as INTEGER (not SERIAL) to preserve original values
    postgres_ddl = re.sub(r'(\s*[`"]id[`"]?\s+)int(\s+NOT\s+NULL)', r'\1INTEGER\2', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'(\s*[`"]id[`"]?\s+)INTEGER(\s+NOT\s+NULL)', r'\1INTEGER\2', postgres_ddl, flags=re.IGNORECASE)
    
    # Fix timestamp types for Company
    postgres_ddl = re.sub(r'\bTIMESTAMP\(3\)\b', 'TIMESTAMP WITHOUT TIME ZONE', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\bDATETIME\(3\)\b', 'TIMESTAMP WITHOUT TIME ZONE', postgres_ddl, flags=re.IGNORECASE)
    
    # Fix boolean defaults for Company visibility fields
    postgres_ddl = re.sub(r"DEFAULT\s+'0'", "DEFAULT false", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'1'", "DEFAULT true", postgres_ddl, flags=re.IGNORECASE)
    
    # Fix invalid date defaults
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00 00:00:00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    
    # Clean up commas
    postgres_ddl = re.sub(r',\s*,', ',', postgres_ddl)
    postgres_ddl = re.sub(r',(\s*)\)', r'\1)', postgres_ddl)
    
    # Convert table name appropriately based on case preservation
    if preserve_case:
        target_table_name = f'"{TABLE_NAME}"'
    else:
        target_table_name = TABLE_NAME.lower()
    
    postgres_ddl = re.sub(
        rf'\bCREATE TABLE {TABLE_NAME}\b',
        f'CREATE TABLE {target_table_name}',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    return postgres_ddl

def create_company_indexes(indexes):
    """Create indexes for Company table"""
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
        
        print(f"🔧 Creating Company index: {index_name}")
        
        # Write to file and execute
        sql_file = f"create_company_index_{index_name}.sql"
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
            print(f"✅ Created Company index: {index_name}")
        else:
            print(f"❌ Failed to create Company index: {index_name}")
            if result:
                print(f"   Error: {result.stderr}")
                print(f"   SQL: {create_index_sql}")
            success = False
    
    return success

def check_company_referenced_table_exists(ref_table):
    """Check if referenced table exists in PostgreSQL for Company foreign keys"""
    # Company references: TwilioCredentials, MailgunCredential
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

def create_company_foreign_keys(foreign_keys):
    """Create foreign key constraints for Company table"""
    if not foreign_keys:
        print(f"ℹ️ No foreign keys to create for {TABLE_NAME}")
        return True
    
    print(f"🔗 Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    
    created_count = 0
    skipped_count = 0
    
    for fk in foreign_keys:
        ref_table = fk['ref_table']
        ref_table_name = f'"{ref_table}"' if PRESERVE_MYSQL_CASE else ref_table.lower()
        
        # Check if referenced table exists
        if not check_company_referenced_table_exists(ref_table):
            print(f"⚠️ Skipping Company FK {fk['name']}: Referenced table '{ref_table}' does not exist")
            skipped_count += 1
            continue
        
        constraint_name = f"fk_company_{fk['name']}"
        local_cols = fk['local_columns'].replace('`', '')
        ref_cols = fk['ref_columns'].replace('`', '')
        
        # Quote column names if preserving case
        if PRESERVE_MYSQL_CASE:
            local_cols = ', '.join([f'"{col.strip()}"' for col in local_cols.split(',')])
            ref_cols = ', '.join([f'"{col.strip()}"' for col in ref_cols.split(',')])
        
        # Convert MySQL actions to PostgreSQL
        on_delete = fk['on_delete'].upper()
        on_update = fk['on_update'].upper()
        
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
        
        print(f"🔧 Creating Company FK: {constraint_name} -> {ref_table}")
        
        # Write to file and execute
        sql_file = f"create_company_fk_{constraint_name}.sql"
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
            print(f"✅ Created Company FK: {constraint_name}")
            created_count += 1
        else:
            print(f"⚠️ Failed to create Company FK {constraint_name}: {result.stderr if result else 'Unknown error'}")
    
    print(f"🎯 Company Foreign Keys: {created_count} created, {skipped_count} skipped")
    return True

def migrate_company_phase1():
    """Phase 1: Create Company table and import data (no constraints)"""
    print(f"🚀 Phase 1: Creating Company table and importing data")
    
    # Get Company table info
    mysql_ddl, indexes, foreign_keys = get_company_table_info()
    if not mysql_ddl:
        return False
    
    # Convert DDL without constraints
    postgres_ddl = convert_company_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    
    print(f"📋 Generated PostgreSQL DDL for Company:")
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
        print("⚠️ Warning: Could not setup auto-increment sequence")
        print("⚠️ Warning: Could not setup auto-increment sequence")

    print(f"✅ Phase 1 complete for {TABLE_NAME}")
    return True

def migrate_company_phase2():
    """Phase 2: Create indexes for Company table"""
    print(f"📊 Phase 2: Creating indexes for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_company_table_info()
    if not mysql_ddl:
        return False
    
    return create_company_indexes(indexes)

def migrate_company_phase3():
    """Phase 3: Create foreign keys for Company table"""
    print(f"🔗 Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    mysql_ddl, indexes, foreign_keys = get_company_table_info()
    if not mysql_ddl:
        return False
    
    return create_company_foreign_keys(foreign_keys)

def verify_company_structure():
    """Verify that the Company table structure matches between MySQL and PostgreSQL"""
    return verify_table_structure(TABLE_NAME, preserve_case=PRESERVE_MYSQL_CASE)

def main():
    parser = argparse.ArgumentParser(description='Company table migration with constraints')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], help='Migration phase (1=table+data, 2=indexes, 3=foreign keys)')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify Company table structure matches between MySQL and PostgreSQL')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"🔍 Verifying table structure for {TABLE_NAME}")
        success = verify_company_structure()
        return success
    
    if args.full:
        print(f"🚀 Running full migration for {TABLE_NAME}")
        success = (
            migrate_company_phase1() and
            migrate_company_phase2() and
            migrate_company_phase3()
        )
    elif args.phase == 1:
        success = migrate_company_phase1()
    elif args.phase == 2:
        success = migrate_company_phase2()
    elif args.phase == 3:
        success = migrate_company_phase3()
    else:
        print("❌ Please specify --phase, --full, or --verify")
        return False
    
    if success:
        print("🎉 Operation completed successfully!")
    else:
        print("❌ Operation failed!")
    
    return success

if __name__ == "__main__":
    main()
