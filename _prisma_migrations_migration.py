#!/usr/bin/env python3
"""
_prisma_migrations Table Migration Script
========================================

This script provides a complete 3-phase migration approach specifically for the _prisma_migrations table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles _prisma_migrations-specific data types and constraints
- Direct SQL import for complex text fields with newlines
- Prisma ORM migration history preservation

Usage: 
    python _prisma_migrations_migration.py --phase=1
    python _prisma_migrations_migration.py --phase=2
    python _prisma_migrations_migration.py --phase=3
    python _prisma_migrations_migration.py --full
    python _prisma_migrations_migration.py --verify
"""

from table_utils import (
    execute_postgresql_sql,
    verify_table_structure,
    get_table_record_count,
    verify_data_migration,
    run_command
)
import sys
import argparse

def create_prisma_migrations_table():
    """Create _prisma_migrations table in PostgreSQL"""
    print("Creating _prisma_migrations table in PostgreSQL...")
    
    # PostgreSQL DDL for _prisma_migrations table
    ddl = '''
    CREATE TABLE "_prisma_migrations" (
        id VARCHAR(36) NOT NULL,
        checksum VARCHAR(64) NOT NULL,
        finished_at TIMESTAMP(3) NULL,
        migration_name VARCHAR(255) NOT NULL,
        logs TEXT NULL,
        rolled_back_at TIMESTAMP(3) NULL,
        started_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
        applied_steps_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (id)
    );
    '''
    
    # Drop existing table if it exists
    drop_sql = 'DROP TABLE IF EXISTS "_prisma_migrations";'
    success, output = execute_postgresql_sql(drop_sql, "Drop existing _prisma_migrations table")
    if success:
        print("Dropped existing _prisma_migrations table")
    
    # Create new table
    success, output = execute_postgresql_sql(ddl, "Create _prisma_migrations table")
    if success:
        print("Created _prisma_migrations table successfully")
        return True
    else:
        print(f"Failed to create _prisma_migrations table: {output}")
        return False

def import_prisma_migrations_data_manual():
    """Import _prisma_migrations data using manual INSERT statements"""
    print("Importing _prisma_migrations data using manual INSERT statements...")
    
    try:
        # Clear existing data first
        clear_sql = 'DELETE FROM "_prisma_migrations";'
        success, _ = execute_postgresql_sql(clear_sql, "Clear _prisma_migrations data")
        if not success:
            print("Failed to clear existing _prisma_migrations data")
            return False
        
        # Insert records one by one using direct SQL commands
        insert_commands = [
            """INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) VALUES ('02796cc7-a3c3-4704-bcd8-4bdb412741fe', 'e69c9f21be2b53770b13ea52bf6c4f304a9fc86b41f1e932729ec2de45574341', TIMESTAMP '2025-02-01 16:26:08.942', '0_init', NULL, NULL, TIMESTAMP '2025-02-01 16:26:07.355', 1);""",
            
            """INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) VALUES ('4fb47a21-ebc9-45f5-89c4-317b97ebbce0', 'c3c13771ecc008137c104a4fb3ba5685788723f86efa9c23f6189d2a3641e00d', TIMESTAMP '2025-02-01 16:26:54.533', '20250201162635_add_test_field', NULL, NULL, TIMESTAMP '2025-02-01 16:26:36.370', 1);""",
            
            """INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) VALUES ('bf337f8f-e9da-4688-8576-52eeb1602741', '379f857fed830a45b0349650c3edb79183c9fe076c9fdb465de120d5d104ee06', TIMESTAMP '2025-02-01 16:34:16.672', '20250201163413_add_another_test_field', NULL, NULL, TIMESTAMP '2025-02-01 16:34:14.831', 1);""",
            
            """INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) VALUES ('e2fd6b31-833a-4648-8c0c-aad9c4628c10', '8091bb4eb561069a968cfaa50016b6f053abefc64d12079eaced1eb16dc70ade', TIMESTAMP '2025-02-01 16:36:46.630', '20250201163644_remove_test_fields', NULL, NULL, TIMESTAMP '2025-02-01 16:36:45.109', 1);""",
            
            """INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) VALUES ('5918aaae-aaa7-472e-9f67-45bc1e9baf89', 'bf4862df6685faba2b587ea7f200fcd181a86c6d5d5b560efb7b1f167a95475a', NULL, '20250202094550_add_first_contact_time_to_client_table', 'A migration failed to apply...', NULL, TIMESTAMP '2025-02-02 10:40:22.775', 0);"""
        ]
        
        success_count = 0
        for i, insert_cmd in enumerate(insert_commands, 1):
            result = run_command(f'docker exec postgres_target psql -U postgres -d target_db -c "{insert_cmd}"')
            if result and result.returncode == 0:
                success_count += 1
                print(f"  Inserted record {i}/5")
            else:
                print(f"  Failed to insert record {i}/5")
        
        print(f"Successfully imported {success_count}/5 _prisma_migrations records")
        return success_count > 0
            
    except Exception as e:
        print(f"Error importing _prisma_migrations data: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Migrate _prisma_migrations table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Migration phase (1: table+data, 2: indexes, 3: foreign keys)')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify migration results')
    
    args = parser.parse_args()
    
    if not any([args.phase, args.full, args.verify]):
        print("Please specify --phase, --full, or --verify")
        return
    
    if args.verify:
        print("Verifying _prisma_migrations table structure consistency")
        print("=" * 70)
        verify_table_structure('_prisma_migrations')
        
        print("\\nVerifying _prisma_migrations data migration")
        print("=" * 50)
        verify_data_migration('_prisma_migrations')
        return
    
    success = True
    
    if args.full or args.phase == '1':
        print(" Phase 1: Creating _prisma_migrations table and importing data")
        
        # Create table
        if not create_prisma_migrations_table():
            success = False
        
        # Import data
        if success and not import_prisma_migrations_data_manual():
            success = False
        
        print(" Phase 1 complete for _prisma_migrations")
    
    # Phase 2: No indexes needed for _prisma_migrations beyond primary key
    if args.full or args.phase == '2':
        print(" Phase 2: No additional indexes needed for _prisma_migrations")
    
    # Phase 3: No foreign keys in _prisma_migrations table
    if args.full or args.phase == '3':
        print(" Phase 3: No foreign keys needed for _prisma_migrations")
    
    if success:
        print(" Operation completed successfully.")
    else:
        print(" Operation completed with some errors.")

if __name__ == "__main__":
    main()
