#!/usr/bin/env python3
"""
NotificationSettingsV2 Table Migration Script
===============================================

This script provides a complete 3-phase migration approach specifically for the NotificationSettingsV2 table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles NotificationSettingsV2-specific data types and constraints
- Manages foreign key dependencies
- Creates appropriate indexes for NotificationSettingsV2 table

Usage: 
    python notificationsettingsv2_migration.py --phase=1
    python notificationsettingsv2_migration.py --phase=2
    python notificationsettingsv2_migration.py --phase=3
    python notificationsettingsv2_migration.py --full
    python notificationsettingsv2_migration.py --verify
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
TABLE_NAME = "NotificationSettingsV2"

def get_notificationsettingsv2_table_info():
    """Get MySQL table information for NotificationSettingsV2"""
    try:
        # Get CREATE TABLE statement
        result = run_command([
            'docker', 'exec', '-i', 'mysql_source', 
            'mysql', '-u', 'root', '-prootpass', 'source_db', 
            '-e', f"SHOW CREATE TABLE `NotificationSettingsV2`;"
        ])
        
        if result.returncode != 0:
            print(f"Error getting NotificationSettingsV2 table structure: {result.stderr}")
            return None, None, None
        
        # Parse CREATE TABLE statement
        create_table_lines = result.stdout.strip().split('\n')
        create_table_statement = '
'.join(create_table_lines[1:])  # Skip header
        
        # Extract indexes
        indexes = []
        index_result = run_command([
            'docker', 'exec', '-i', 'mysql_source', 
            'mysql', '-u', 'root', '-prootpass', 'source_db', 
            '-e', f"SHOW INDEX FROM `NotificationSettingsV2`;
        ])
        
        if index_result.returncode == 0:
            index_lines = index_result.stdout.strip().split('\n')
            for line in index_lines[1:]:  # Skip header
                if line.strip():
                    parts = line.split('	')
                    if len(parts) >= 5:
                        index_name = parts[2]
                        column_name = parts[4]
                        if index_name != 'PRIMARY':
                            indexes.append((index_name, column_name))
        
        # Extract foreign keys
        foreign_keys = []
        fk_result = run_command([
            'docker', 'exec', '-i', 'mysql_source', 
            'mysql', '-u', 'root', '-prootpass', 'source_db', 
            '-e', f"SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = 'source_db' AND TABLE_NAME = 'NotificationSettingsV2'"
        ])
        
        if fk_result.returncode == 0:
            fk_lines = fk_result.stdout.strip().split('\n')
            for line in fk_lines[1:]:  # Skip header
                if line.strip():
                    parts = line.split('	')
                    if len(parts) >= 4:
                        fk_name = parts[0]
                        column_name = parts[1]
                        referenced_table = parts[2]
                        referenced_column = parts[3]
                        foreign_keys.append((fk_name, column_name, referenced_table, referenced_column))
        
        return create_table_statement, indexes, foreign_keys
        
    except Exception as e:
        print(f"Error getting NotificationSettingsV2 table info: {e}")
        return None, None, None

def convert_notificationsettingsv2_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=True, preserve_case=True):
    """Convert MySQL DDL to PostgreSQL DDL for NotificationSettingsV2"""
    try:
        # Basic conversions
        postgres_ddl = mysql_ddl
        
        # Convert MySQL data types to PostgreSQL
        postgres_ddl = re.sub(r'int', 'INTEGER', postgres_ddl)
        postgres_ddl = re.sub(r'varchar\([^)]+\)', 'VARCHAR', postgres_ddl)
        postgres_ddl = re.sub(r'text', 'TEXT', postgres_ddl)
        postgres_ddl = re.sub(r'datetime', 'TIMESTAMP', postgres_ddl)
        postgres_ddl = re.sub(r'timestamp', 'TIMESTAMP', postgres_ddl)
        postgres_ddl = re.sub(r'decimal\([^)]+\)', 'DECIMAL', postgres_ddl)
        postgres_ddl = re.sub(r'tinyint\(1\)', 'BOOLEAN', postgres_ddl)
        postgres_ddl = re.sub(r'double', 'DOUBLE PRECISION', postgres_ddl)
        postgres_ddl = re.sub(r'float', 'REAL', postgres_ddl)
        
        # Convert ENUM to VARCHAR
        postgres_ddl = re.sub(r'enum\([^)]+\)', 'VARCHAR(50)', postgres_ddl)
        
        # Remove MySQL-specific options
        postgres_ddl = re.sub(r'ENGINE\s*=\s*\w+', '', postgres_ddl)
        postgres_ddl = re.sub(r'DEFAULT\s+CHARSET\s*=\s*\w+', '', postgres_ddl)
        postgres_ddl = re.sub(r'COLLATE\s+\w+', '', postgres_ddl)
        postgres_ddl = re.sub(r'AUTO_INCREMENT\s*=\s*\d+', '', postgres_ddl)
        
        # Handle case sensitivity
        if preserve_case:
            # Quote table name
            postgres_ddl = re.sub(r'CREATE TABLE `?NotificationSettingsV2`?', f"CREATE TABLE "NotificationSettingsV2"\', postgres_ddl)
            
            # Quote column names (basic approach)
            postgres_ddl = re.sub(r\'`([^`]+)`", r'""', postgres_ddl)
        
        # Remove constraints if not included
        if not include_constraints:
            postgres_ddl = re.sub(r',\s*CONSTRAINT\s+[^,]+', '', postgres_ddl)
            postgres_ddl = re.sub(r',\s*KEY\s+[^,]+', '', postgres_ddl)
            postgres_ddl = re.sub(r',\s*INDEX\s+[^,]+', '', postgres_ddl)
            postgres_ddl = re.sub(r',\s*UNIQUE\s+[^,]+', '', postgres_ddl)
            postgres_ddl = re.sub(r',\s*FOREIGN KEY\s+[^,]+', '', postgres_ddl)
        
        # Clean up extra commas
        postgres_ddl = re.sub(r',\s*\)', ')', postgres_ddl)
        
        return postgres_ddl
        
    except Exception as e:
        print(f"Error converting NotificationSettingsV2 DDL: {e}")
        return None

def create_notificationsettingsv2_table(mysql_ddl):
    """Create NotificationSettingsV2 table in PostgreSQL"""
    try:
        postgres_ddl = convert_notificationsettingsv2_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
        
        if not postgres_ddl:
            return False
        
        print(f"Generated PostgreSQL DDL for {TABLE_NAME}:")
        print("=" * 50)
        print(postgres_ddl)
        print("=" * 50)
        
        return create_postgresql_table(TABLE_NAME, postgres_ddl, preserve_case=PRESERVE_MYSQL_CASE)
        
    except Exception as e:
        print(f"Error creating NotificationSettingsV2 table: {e}")
        return False

def create_notificationsettingsv2_indexes(indexes):
    """Create indexes for NotificationSettingsV2 table"""
    try:
        if not indexes:
            print(f"No indexes to create for {TABLE_NAME}")
            return True
        
        print(f"Creating {len(indexes)} indexes for {TABLE_NAME}")
        
        for index_name, column_name in indexes:
            # Skip duplicate indexes
            if index_name.lower() in ['primary', 'id']:
                continue
                
            index_sql = f"CREATE INDEX "{index_name}" ON "{TABLE_NAME}" ("{column_name}");
            print(f"Creating index: {index_sql}")
            
            if not execute_postgresql_sql(index_sql):
                print(f"Warning: Could not create index {index_name}")
        
        return True
        
    except Exception as e:
        print(f"Error creating NotificationSettingsV2 indexes: {e}")
        return False

def create_notificationsettingsv2_foreign_keys(foreign_keys):
    """Create foreign keys for NotificationSettingsV2 table"""
    try:
        if not foreign_keys:
            print(f"No foreign keys to create for {TABLE_NAME}")
            return True
        
        print(f"Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}")
        
        for fk_name, column_name, referenced_table, referenced_column in foreign_keys:
            # Check if referenced table exists
            check_sql = f\'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = "{referenced_table.lower()}');'
            result = execute_postgresql_sql(check_sql)
            
            if result and result[0][0]:
                fk_sql = f"ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ("{column_name}") REFERENCES "{referenced_table}" ("{referenced_column}") ON DELETE CASCADE ON UPDATE CASCADE;
                print(f"Creating foreign key: {fk_name}")
                
                if not execute_postgresql_sql(fk_sql):
                    print(f"Warning: Could not create foreign key {fk_name}")
            else:
                print(f"Warning: Referenced table {referenced_table} does not exist, skipping foreign key {fk_name}")
        
        return True
        
    except Exception as e:
        print(f"Error creating NotificationSettingsV2 foreign keys: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description=f\'Migrate {TABLE_NAME} table from MySQL to PostgreSQL")
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Run specific phase')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure')
    args = parser.parse_args()
    
    if args.verify:
        print(f"Verifying table structure for {TABLE_NAME}")
        verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return
    
    if not any([args.phase, args.full]):
        print("Please specify --phase, --full, or --verify")
        return
    
    # Get table information
    mysql_ddl, indexes, foreign_keys = get_notificationsettingsv2_table_info()
    if not mysql_ddl:
        return
    
    success = True
    
    if args.phase == '1' or args.full:
        print(f"Phase 1: Creating {TABLE_NAME} table and importing data")
        if not create_notificationsettingsv2_table(mysql_ddl):
            success = False
        else:
            data_indicator = export_and_clean_mysql_data(TABLE_NAME)
            import_data_to_postgresql(TABLE_NAME, data_indicator, PRESERVE_MYSQL_CASE, include_id=True)
            add_primary_key_constraint(TABLE_NAME, PRESERVE_MYSQL_CASE)
            setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE)
            print(f"Phase 1 complete for {TABLE_NAME}")
    
    if args.phase == '2' or args.full:
        print(f"Phase 2: Creating indexes for {TABLE_NAME}")
        if not create_notificationsettingsv2_indexes(indexes):
            success = False
    
    if args.phase == '3' or args.full:
        print(f"Phase 3: Creating foreign keys for {TABLE_NAME}")
        if not create_notificationsettingsv2_foreign_keys(foreign_keys):
            success = False
    
    if success:
        print("Operation completed successfully!")
    else:
        print("Operation completed with errors!")

if __name__ == "__main__":
    main()
