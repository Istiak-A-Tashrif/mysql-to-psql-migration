#!/usr/bin/env python3
"""
InvoiceAutomationRule Table Migration Script
============================================

This script migrates the InvoiceAutomationRule table from MySQL to PostgreSQL.
"""

import argparse
from table_utils import (
    create_postgresql_table,
    robust_import_with_serial_id,
    validate_migration_success,
    execute_postgresql_sql
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "InvoiceAutomationRule"

def create_invoiceautomationrule_table():
    """Create InvoiceAutomationRule table in PostgreSQL"""
    print(f"Creating {TABLE_NAME} table in PostgreSQL...")
    
    # Create enum type first
    enum_sql = '''
DO $$ BEGIN
    CREATE TYPE communication_type_enum AS ENUM ('SMS', 'EMAIL', 'BOTH');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
'''
    
    success, result = execute_postgresql_sql(enum_sql, "Communication type enum creation")
    if not success:
        print(f"Failed to create communication type enum: {result.stderr if result else 'Unknown error'}")
        return False
    
    print("Created communication type enum")
    
    # Define PostgreSQL DDL based on MySQL structure
    ddl = '''
CREATE TABLE "InvoiceAutomationRule" (
    "id" SERIAL PRIMARY KEY,
    "communicationType" communication_type_enum NOT NULL,
    "companyId" INTEGER NOT NULL,
    "emailBody" VARCHAR(191) NOT NULL,
    "emailSubject" VARCHAR(191) NOT NULL,
    "invoiceStatus" VARCHAR(191) NOT NULL,
    "isPaused" BOOLEAN NOT NULL DEFAULT false,
    "smsBody" VARCHAR(191) NOT NULL,
    "timeDelay" INTEGER
);
'''
    
    # Create table
    success = create_postgresql_table(TABLE_NAME, ddl, PRESERVE_MYSQL_CASE)
    return success

def phase1_create_table_and_data():
    """Phase 1: Create table and import data"""
    print(f"Phase 1: Creating {TABLE_NAME} table and importing data")
    
    # Create table
    if not create_invoiceautomationrule_table():
        return False
    
    # Import data using robust method
    if not robust_import_with_serial_id(TABLE_NAME, PRESERVE_MYSQL_CASE):
        return False
    
    return True

def phase2_create_indexes():
    """Phase 2: Create indexes for InvoiceAutomationRule"""
    print(f"Phase 2: Creating indexes for {TABLE_NAME}")
    
    # Create indexes for InvoiceAutomationRule
    indexes = [
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_companyId" ON "{TABLE_NAME}" ("companyId");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_invoiceStatus" ON "{TABLE_NAME}" ("invoiceStatus");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_isPaused" ON "{TABLE_NAME}" ("isPaused");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_communicationType" ON "{TABLE_NAME}" ("communicationType");'
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
    
    # Create foreign key constraints for InvoiceAutomationRule
    foreign_keys = [
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_companyId" FOREIGN KEY ("companyId") REFERENCES "Company" ("id") ON DELETE CASCADE;'
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
    parser = argparse.ArgumentParser(description='Migrate InvoiceAutomationRule table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], default='1', help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"Verifying {TABLE_NAME} migration...")
        success = validate_migration_success(TABLE_NAME, PRESERVE_MYSQL_CASE, "InvoiceAutomationRule migration")
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
