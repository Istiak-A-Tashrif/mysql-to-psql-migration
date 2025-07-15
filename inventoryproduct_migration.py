#!/usr/bin/env python3
"""
InventoryProduct Table Migration Script
=======================================

This script migrates the InventoryProduct table from MySQL to PostgreSQL.
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
TABLE_NAME = "InventoryProduct"

def create_inventoryproduct_table():
    """Create InventoryProduct table in PostgreSQL"""
    print(f"Creating {TABLE_NAME} table in PostgreSQL...")
    
    # Create enum type first
    enum_sql = '''
DO $$ BEGIN
    CREATE TYPE inventory_type_enum AS ENUM ('Supply', 'Product');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
'''
    
    success, result = execute_postgresql_sql(enum_sql, "Inventory type enum creation")
    if not success:
        print(f"Failed to create inventory type enum: {result.stderr if result else 'Unknown error'}")
        return False
    
    print("Created inventory type enum")
    
    # Define PostgreSQL DDL based on MySQL structure
    ddl = '''
CREATE TABLE "InventoryProduct" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(191) NOT NULL,
    "description" TEXT,
    "category_id" INTEGER,
    "quantity" DECIMAL(10,2) DEFAULT 1.00,
    "price" DECIMAL(65,30) DEFAULT 0.000000000000000000000000000000,
    "unit" VARCHAR(191) DEFAULT 'pc',
    "lot" VARCHAR(191),
    "vendor_id" INTEGER,
    "user_id" INTEGER,
    "type" inventory_type_enum NOT NULL,
    "receipt" VARCHAR(191),
    "low_inventory_alert" INTEGER,
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
    if not create_inventoryproduct_table():
        return False
    
    # Import data using robust method
    if not robust_export_and_import_data(TABLE_NAME, PRESERVE_MYSQL_CASE, include_id=True):
        return False
    
    # Setup auto-increment sequence
    if not setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE):
        print(f"Warning: Could not setup auto-increment sequence for {TABLE_NAME}")
    
    return True

def phase2_create_indexes():
    """Phase 2: Create indexes for performance"""
    print(f"Phase 2: Creating indexes for {TABLE_NAME}")
    
    # Create indexes for InventoryProduct
    indexes = [
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_company_id" ON "{TABLE_NAME}" ("company_id");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_category_id" ON "{TABLE_NAME}" ("category_id");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_vendor_id" ON "{TABLE_NAME}" ("vendor_id");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_user_id" ON "{TABLE_NAME}" ("user_id");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_type" ON "{TABLE_NAME}" ("type");',
        f'CREATE INDEX IF NOT EXISTS "idx_{TABLE_NAME}_name" ON "{TABLE_NAME}" ("name");'
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
    
    # Create foreign key constraints for InventoryProduct
    foreign_keys = [
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_company_id" FOREIGN KEY ("company_id") REFERENCES "Company" ("id") ON DELETE CASCADE;',
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_category_id" FOREIGN KEY ("category_id") REFERENCES "Category" ("id") ON DELETE SET NULL;',
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_vendor_id" FOREIGN KEY ("vendor_id") REFERENCES "Vendor" ("id") ON DELETE SET NULL;',
        f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT "fk_{TABLE_NAME}_user_id" FOREIGN KEY ("user_id") REFERENCES "User" ("id") ON DELETE SET NULL;'
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
    parser = argparse.ArgumentParser(description='Migrate InventoryProduct table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], default='1', help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f"Verifying {TABLE_NAME} migration...")
        success = validate_migration_success(TABLE_NAME, PRESERVE_MYSQL_CASE, "InventoryProduct migration")
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
