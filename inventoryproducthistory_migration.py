#!/usr/bin/env python3
"""
InventoryProductHistory Table Migration Script
==============================================

This script migrates the InventoryProductHistory table from MySQL to PostgreSQL.
"""

import argparse
from table_utils import (
    create_postgresql_table,
    robust_export_and_import_data,
    validate_migration_success,
    execute_postgresql_sql,
    setup_auto_increment_sequence
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "InventoryProductHistory"

def create_inventoryproducthistory_table():
    """Create InventoryProductHistory table in PostgreSQL"""
    print(f"Creating {TABLE_NAME} table in PostgreSQL...")
    
    # Create enum type first
    enum_sql = '''
DO $$ BEGIN
    CREATE TYPE inventory_history_type_enum AS ENUM ('Purchase', 'Sale');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
'''
    
    success, result = execute_postgresql_sql(enum_sql, "Inventory history type enum creation")
    if not success:
        print(f"Failed to create inventory history type enum: {result.stderr if result else 'Unknown error'}")
        return False
    
    print("Created inventory history type enum")
    
    # Define PostgreSQL DDL based on MySQL structure
    ddl = '''
CREATE TABLE "InventoryProductHistory" (
    "id" SERIAL PRIMARY KEY,
    "price" DECIMAL(65,30) DEFAULT 0.000000000000000000000000000000,
    "quantity" DECIMAL(10,2) NOT NULL,
    "date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "notes" VARCHAR(191),
    "type" inventory_history_type_enum NOT NULL,
    "inventory_id" INTEGER NOT NULL,
    "invoice_id" VARCHAR(191),
    "vendor_id" INTEGER,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "company_id" INTEGER NOT NULL,
    "is_lost" BOOLEAN DEFAULT false
);
'''
    
    # Create table
    success = create_postgresql_table(TABLE_NAME, ddl, PRESERVE_MYSQL_CASE)
    return success

def phase1_create_table_and_data():
    """Phase 1: Create table and import data"""
    print(f"Phase 1: Creating {TABLE_NAME} table and importing data")
    
    # Create table
    if not create_inventoryproducthistory_table():
        return False
    
    # Import data using robust method
    if not robust_export_and_import_data(TABLE_NAME, PRESERVE_MYSQL_CASE, include_id=True):
        return False
    
    # Setup auto-increment sequence
    if not setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE):
        print(f"Warning: Could not setup auto-increment sequence for {TABLE_NAME}")
    
    return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Migrate InventoryProductHistory table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], default='1', help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"Verifying {TABLE_NAME} migration...")
        success = validate_migration_success(TABLE_NAME, PRESERVE_MYSQL_CASE, "InventoryProductHistory migration")
        return success
    
    if args.full:
        print(f"Running full migration for {TABLE_NAME}...")
        if not phase1_create_table_and_data():
            print(f"Phase 1 failed for {TABLE_NAME}")
            return False
        print(f"Full migration completed for {TABLE_NAME}")
        return True
    
    if args.phase == '1':
        return phase1_create_table_and_data()
    
    print(f"Phase {args.phase} not implemented yet")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
