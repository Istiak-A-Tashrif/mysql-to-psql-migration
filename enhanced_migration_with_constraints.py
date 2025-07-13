#!/usr/bin/env python3
"""
Enhanced Appointment Migration Script with Foreign Keys and Indexes Support
==========================================================================

This script provides a complete 3-phase migration approach for the Appointment table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Usage: 
    python enhanced_migration_with_constraints.py --phase=1
    python enhanced_migration_with_constraints.py --phase=2
    python enhanced_migration_with_constraints.py --phase=3
    python enhanced_migration_with_constraints.py --full
    python enhanced_migration_with_constraints.py --verify
"""

import re
import os
import argparse
from collections import OrderedDict
from table_utils import (
    verify_table_structure,
    compare_table_structures,
    get_mysql_table_columns,
    get_postgresql_table_columns,
    normalize_mysql_type,
    run_command
)

def get_mysql_table_info(table_name):
    """Get complete table information from MySQL including constraints"""
    print(f"🔍 Getting complete table info for {table_name} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW CREATE TABLE `{table_name}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get table structure: {result.stderr if result else 'No result'}")
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
        print("❌ Could not find CREATE TABLE statement")
        return None, None, None
    
    # Extract different components
    indexes = extract_indexes_from_ddl(create_statement)
    foreign_keys = extract_foreign_keys_from_ddl(create_statement)
    
    print(f"✅ Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys")
    return create_statement, indexes, foreign_keys

def extract_indexes_from_ddl(ddl):
    """Extract index definitions from MySQL DDL"""
    indexes = []
    
    # Find all KEY definitions
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
                'original': match.group(0)
            })
    
    return indexes

def extract_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY
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
            'original': match.group(0)
        })
    
    return foreign_keys

def convert_mysql_to_postgresql_ddl(mysql_ddl, table_name, include_constraints=False):
    """Convert MySQL DDL to PostgreSQL DDL"""
    print(f"🔄 Converting MySQL DDL to PostgreSQL for {table_name} (constraints: {include_constraints})...")
    
    # Type mappings (same as original)
    type_mappings = OrderedDict([
        (r'\bint\(\d+\)\s+auto_increment\b', 'SERIAL PRIMARY KEY'),
        (r'\bbigint\(\d+\)\s+auto_increment\b', 'BIGSERIAL PRIMARY KEY'),
        (r'tinyint\(1\)', 'BOOLEAN'),
        (r'tinyint\(\d+\)', 'SMALLINT'),
        (r'smallint\(\d+\)', 'SMALLINT'),
        (r'mediumint\(\d+\)', 'INTEGER'),
        (r'int\(\d+\)', 'INTEGER'),
        (r'bigint\(\d+\)', 'BIGINT'),
        (r'\btinyint\b(?!\()', 'SMALLINT'),
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
    ])
    
    postgres_ddl = mysql_ddl
    
    # Convert table name
    postgres_ddl = re.sub(
        r'CREATE TABLE `([^`]+)`',
        r'CREATE TABLE \1',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    # Apply type mappings
    for mysql_pattern, postgres_type in type_mappings.items():
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
        # Remove indexes and constraints for phase 1
        postgres_ddl = re.sub(r',\s*KEY\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r',\s*INDEX\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r',\s*UNIQUE\s+KEY\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r',\s*CONSTRAINT\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Clean up PRIMARY KEY definitions that are already handled by SERIAL
    postgres_ddl = re.sub(r',\s*PRIMARY\s+KEY\s*\([^)]+\)', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove MySQL table options
    postgres_ddl = re.sub(r'\)\s*[A-Z_=\s\w\d]+$', ')', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove backticks
    postgres_ddl = re.sub(r'`([^`]+)`', r'\1', postgres_ddl)
    
    # Fix auto_increment
    postgres_ddl = re.sub(r'\s+AUTO_INCREMENT\b', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Fix boolean defaults
    postgres_ddl = re.sub(r"DEFAULT\s+'0'", "DEFAULT false", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'1'", "DEFAULT true", postgres_ddl, flags=re.IGNORECASE)
    
    # Fix invalid date defaults
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00 00:00:00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    
    # Clean up commas
    postgres_ddl = re.sub(r',\s*,', ',', postgres_ddl)
    postgres_ddl = re.sub(r',(\s*)\)', r'\1)', postgres_ddl)
    
    # Convert table name to lowercase
    table_lower = table_name.lower()
    postgres_ddl = re.sub(
        rf'\bCREATE TABLE {table_name}\b',
        f'CREATE TABLE {table_lower}',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    return postgres_ddl

def create_indexes(table_name, indexes):
    """Create indexes for a table"""
    if not indexes:
        print(f"ℹ️ No indexes to create for {table_name}")
        return True
    
    print(f"📊 Creating {len(indexes)} indexes for {table_name}...")
    
    for index in indexes:
        index_name = f"{table_name.lower()}_{index['name']}"
        columns = index['columns'].replace('`', '')  # Remove backticks
        unique_clause = "UNIQUE " if index['unique'] else ""
        
        create_index_sql = f"CREATE {unique_clause}INDEX {index_name} ON {table_name.lower()} ({columns});"
        
        print(f"🔧 Creating index: {index_name}")
        
        # Write to file and execute
        sql_file = f"create_index_{index_name}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(create_index_sql)
        
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
            print(f"✅ Created index: {index_name}")
        else:
            print(f"⚠️ Failed to create index {index_name}: {result.stderr if result else 'Unknown error'}")
    
    return True

def check_referenced_table_exists(ref_table):
    """Check if referenced table exists in PostgreSQL"""
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'{ref_table.lower()}\' AND table_schema = \'public\';"'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            count = int(result.stdout.strip())
            return count > 0
        except:
            return False
    return False

def create_foreign_keys(table_name, foreign_keys):
    """Create foreign key constraints for a table"""
    if not foreign_keys:
        print(f"ℹ️ No foreign keys to create for {table_name}")
        return True
    
    print(f"🔗 Creating {len(foreign_keys)} foreign keys for {table_name}...")
    
    created_count = 0
    skipped_count = 0
    
    for fk in foreign_keys:
        ref_table = fk['ref_table'].lower()
        
        # Check if referenced table exists
        if not check_referenced_table_exists(ref_table):
            print(f"⚠️ Skipping FK {fk['name']}: Referenced table '{ref_table}' does not exist")
            skipped_count += 1
            continue
        
        constraint_name = f"fk_{table_name.lower()}_{fk['name']}"
        local_cols = fk['local_columns'].replace('`', '')
        ref_cols = fk['ref_columns'].replace('`', '')
        
        # Convert MySQL actions to PostgreSQL
        on_delete = fk['on_delete'].upper()
        on_update = fk['on_update'].upper()
        
        if on_delete not in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
            on_delete = 'RESTRICT'
        if on_update not in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
            on_update = 'RESTRICT'
        
        alter_sql = f"""
ALTER TABLE {table_name.lower()} 
ADD CONSTRAINT {constraint_name} 
FOREIGN KEY ({local_cols}) 
REFERENCES {ref_table} ({ref_cols}) 
ON DELETE {on_delete} 
ON UPDATE {on_update};
"""
        
        print(f"🔧 Creating FK: {constraint_name} -> {ref_table}")
        
        # Write to file and execute
        sql_file = f"create_fk_{constraint_name}.sql"
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
            print(f"✅ Created FK: {constraint_name}")
            created_count += 1
        else:
            print(f"⚠️ Failed to create FK {constraint_name}: {result.stderr if result else 'Unknown error'}")
    
    print(f"🎯 Foreign Keys: {created_count} created, {skipped_count} skipped")
    return True

def migrate_table_phase1(table_name="Appointment"):
    """Phase 1: Create table and import data (no constraints)"""
    print(f"🚀 Phase 1: Creating table and importing data for {table_name}")
    
    # Get table info
    mysql_ddl, indexes, foreign_keys = get_mysql_table_info(table_name)
    if not mysql_ddl:
        return False
    
    # Convert DDL without constraints
    postgres_ddl = convert_mysql_to_postgresql_ddl(mysql_ddl, table_name, include_constraints=False)
    
    # Use your existing functions for table creation and data import
    from complete_appointment_migration import create_postgresql_table, export_and_clean_mysql_data, import_data_to_postgresql
    
    if not create_postgresql_table(table_name, postgres_ddl):
        return False
    
    cleaned_data = export_and_clean_mysql_data(table_name)
    if cleaned_data is None:
        return False
    
    if not import_data_to_postgresql(table_name, cleaned_data):
        return False
    
    print(f"✅ Phase 1 complete for {table_name}")
    return True

def migrate_table_phase2(table_name="Appointment"):
    """Phase 2: Create indexes"""
    print(f"📊 Phase 2: Creating indexes for {table_name}")
    
    mysql_ddl, indexes, foreign_keys = get_mysql_table_info(table_name)
    if not mysql_ddl:
        return False
    
    return create_indexes(table_name, indexes)

def migrate_table_phase3(table_name="Appointment"):
    """Phase 3: Create foreign keys"""
    print(f"🔗 Phase 3: Creating foreign keys for {table_name}")
    
    mysql_ddl, indexes, foreign_keys = get_mysql_table_info(table_name)
    if not mysql_ddl:
        return False
    
    return create_foreign_keys(table_name, foreign_keys)

# All utility functions for table comparison are now imported from table_utils.py

def verify_appointment_structure():
    """Verify that the appointment table structure matches between MySQL and PostgreSQL"""
    return verify_table_structure("Appointment")

# ...existing code...

def main():
    parser = argparse.ArgumentParser(description='Enhanced Appointment migration with constraints')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], help='Migration phase (1=table+data, 2=indexes, 3=foreign keys)')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify Appointment table structure matches between MySQL and PostgreSQL')
    
    args = parser.parse_args()
    
    table_name = "Appointment"  # Fixed table name for this script
    
    if args.verify:
        print(f"🔍 Verifying table structure for {table_name}")
        success = verify_appointment_structure()
        return success
    
    if args.full:
        print(f"🚀 Running full migration for {table_name}")
        success = (
            migrate_table_phase1(table_name) and
            migrate_table_phase2(table_name) and
            migrate_table_phase3(table_name)
        )
    elif args.phase == 1:
        success = migrate_table_phase1(table_name)
    elif args.phase == 2:
        success = migrate_table_phase2(table_name)
    elif args.phase == 3:
        success = migrate_table_phase3(table_name)
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
