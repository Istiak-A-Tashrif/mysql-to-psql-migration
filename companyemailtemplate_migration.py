#!/usr/bin/env python3
"""
CompanyEmailTemplate Table Migration Script
==========================================

This script migrates the companyEmailTemplate table from MySQL to PostgreSQL.
Note: The MySQL table name is 'companyEmailTemplate' (lowercase 'c').
"""

import argparse
from table_utils import (
    create_postgresql_table,
    robust_export_and_import_data,
    validate_migration_success,
    run_command,
    execute_postgresql_sql,
    setup_auto_increment_sequence
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "companyEmailTemplate"  # Note: lowercase 'c' in MySQL

def create_companyemailtemplate_table():
    """Create companyEmailTemplate table in PostgreSQL"""
    print(f"Creating {TABLE_NAME} table in PostgreSQL...")
    
    # Define PostgreSQL DDL based on MySQL structure
    ddl = '''
CREATE TABLE "companyEmailTemplate" (
    "id" SERIAL PRIMARY KEY,
    "subject" VARCHAR(191) NOT NULL,
    "message" TEXT,
    "company_id" INTEGER NOT NULL,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
'''
    
    # Create table
    success = create_postgresql_table(TABLE_NAME, ddl, PRESERVE_MYSQL_CASE)
    return success

def phase1_create_table_and_data():
    """Phase 1: Create table and import data"""
    print(f"Phase 1: Creating {TABLE_NAME} table and importing data")
    
    # Create table
    if not create_companyemailtemplate_table():
        return False
    
    # Import data using robust method
    if not robust_export_and_import_data(TABLE_NAME, PRESERVE_MYSQL_CASE, include_id=True):
        return False
    
    # Setup auto-increment sequence
    if not setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE):
        print(f"Warning: Could not setup auto-increment sequence for {TABLE_NAME}")
    
    return True

def phase2_create_indexes():
    """Phase 2: Create indexes for companyEmailTemplate"""
    print(f"Phase 2: Creating indexes for {TABLE_NAME}")
    
    # Create indexes for companyEmailTemplate
    indexes = [
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_company_id" ON "{TABLE_NAME}" ("company_id");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_created_at" ON "{TABLE_NAME}" ("created_at");'
    ]
    
    for index_sql in indexes:
        success, result = execute_postgresql_sql(index_sql, f"Index creation for {TABLE_NAME}")
        if not success:
            print(f"Warning: Failed to create index: {index_sql}")
            print(f"Error: {result.stderr if result else 'Unknown error'}")
    
    print(f"Phase 2 complete for {TABLE_NAME}")
    return True

def phase3_create_foreign_keys():
    """Phase 3: Create foreign key constraints"""
    print(f"Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    # Create foreign key constraints for companyEmailTemplate
    foreign_keys = [
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_company_id" FOREIGN KEY ("company_id") REFERENCES "Company" ("id") ON DELETE CASCADE;'
    ]
    
    for fk_sql in foreign_keys:
        success, result = execute_postgresql_sql(fk_sql, f"Foreign key creation for {TABLE_NAME}")
        if not success:
            print(f"Warning: Failed to create foreign key: {fk_sql}")
            print(f"Error: {result.stderr if result else 'Unknown error'}")
    
    print(f"Phase 3 complete for {TABLE_NAME}")
    return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Migrate companyEmailTemplate table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], default='1', help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"Verifying {TABLE_NAME} migration...")
        success = validate_migration_success(TABLE_NAME, PRESERVE_MYSQL_CASE, "companyEmailTemplate migration")
        return success
    
    if args.full:
        print(f"Running full migration for {TABLE_NAME}...")
        if not phase1_create_table_and_data():
            print(f"Phase 1 failed for {TABLE_NAME}")
            return False
        if not phase2_create_indexes():
            print(f"Phase 2 failed for {TABLE_NAME}")
            return False
        if not phase3_create_foreign_keys():
            print(f"Phase 3 failed for {TABLE_NAME}")
            return False
        print(f"Full migration completed for {TABLE_NAME}")
        return True
    
    if args.phase == '1':
        return phase1_create_table_and_data()
    elif args.phase == '2':
        return phase2_create_indexes()
    elif args.phase == '3':
        return phase3_create_foreign_keys()
    
    print(f"Phase {args.phase} not implemented yet")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
