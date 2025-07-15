#!/usr/bin/env python3
"""
CommunicationStage Table Migration Script
========================================

This script migrates the CommunicationStage table from MySQL to PostgreSQL.
"""

import re
import os
import argparse
from table_utils import (
    verify_table_structure,
    run_command,
    create_postgresql_table,
    robust_export_and_import_data,
    add_primary_key_constraint,
    setup_auto_increment_sequence
)

PRESERVE_MYSQL_CASE = True
TABLE_NAME = "CommunicationStage"

def create_communicationstage_table():
    """Create CommunicationStage table in PostgreSQL with proper schema"""
    
    # Define the PostgreSQL DDL based on the MySQL structure
    postgres_ddl = '''CREATE TABLE "CommunicationStage" (
  "id" SERIAL NOT NULL,
  "communicationRuleId" INTEGER NOT NULL,
  "columnId" INTEGER NOT NULL
)'''
    
    print(f" Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    # Create the table
    success = create_postgresql_table(TABLE_NAME, postgres_ddl, preserve_case=PRESERVE_MYSQL_CASE)
    return success

def phase1_create_table_and_data():
    """Phase 1: Create table structure and import data"""
    print(f" Phase 1: Creating {TABLE_NAME} table and importing data")
    
    # Create table
    if not create_communicationstage_table():
        return False
    
    # Import data preserving original IDs
    if not robust_export_and_import_data(TABLE_NAME, PRESERVE_MYSQL_CASE, include_id=True):
        return False
    
    # Add PRIMARY KEY constraint
    success = add_primary_key_constraint(TABLE_NAME, PRESERVE_MYSQL_CASE)
    if not success:
        print(f"Warning: Could not add PRIMARY KEY constraint to {TABLE_NAME}")
    
    # Setup auto-increment sequence
    success = setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE)
    if not success:
        print(f"Warning: Could not setup auto-increment sequence for {TABLE_NAME}")
    
    print(f" Phase 1 complete for {TABLE_NAME}")
    return True

def phase2_create_indexes():
    """Phase 2: Create indexes for performance"""
    print(f" Phase 2: Creating indexes for {TABLE_NAME}")
    print(f" Phase 2 complete for {TABLE_NAME}")
    return True

def phase3_create_foreign_keys():
    """Phase 3: Create foreign key constraints"""
    print(f" Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    from table_utils import execute_postgresql_sql
    
    # Create foreign key constraints for CommunicationStage
    foreign_keys = [
        'ALTER TABLE "CommunicationStage" ADD CONSTRAINT "CommunicationStage_columnId_fkey" FOREIGN KEY ("columnId") REFERENCES "Column" ("id") ON DELETE CASCADE;',
        'ALTER TABLE "CommunicationStage" ADD CONSTRAINT "CommunicationStage_communicationRuleId_fkey" FOREIGN KEY ("communicationRuleId") REFERENCES "CommunicationAutomationRule" ("id") ON DELETE CASCADE;'
    ]
    
    for fk_sql in foreign_keys:
        success, result = execute_postgresql_sql(fk_sql, f"Foreign key creation for {TABLE_NAME}")
        if not success:
            print(f"Warning: Failed to create foreign key: {fk_sql}")
            if result:
                print(f"Error: {result.stderr}")
        else:
            print(f"Created foreign key successfully")
    
    print(f" Phase 3 complete for {TABLE_NAME}")
    return True

def main():
    parser = argparse.ArgumentParser(description=f'Migrate {TABLE_NAME} table from MySQL to PostgreSQL')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], help='Run specific migration phase')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure consistency')
    
    args = parser.parse_args()
    
    if args.verify:
        print(f" Verifying table structure for {TABLE_NAME}")
        success = verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return 0 if success else 1
    
    if not args.phase and not args.full:
        print("Please specify --phase, --full, or --verify")
        return 1
    
    if args.full:
        phases = [1, 2, 3]
    else:
        phases = [args.phase]
    
    for phase in phases:
        success = False
        if phase == 1:
            success = phase1_create_table_and_data()
        elif phase == 2:
            success = phase2_create_indexes()
        elif phase == 3:
            success = phase3_create_foreign_keys()
        
        if not success:
            print(f" Phase {phase} failed for {TABLE_NAME}")
            return 1
    
    print(f" Operation completed successfully!")
    return 0

if __name__ == "__main__":
    exit(main())
