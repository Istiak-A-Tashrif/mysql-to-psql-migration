#!/usr/bin/env python3

import os
import subprocess

def get_mysql_tables():
    """Get all tables from MySQL database"""
    try:
        result = subprocess.run([
            'docker', 'exec', '-i', 'mysql_source', 
            'mysql', '-u', 'root', '-prootpass', 'source_db', 
            '-e', 'SHOW TABLES;'
        ], capture_output=True, text=True, check=True)
        
        tables = []
        for line in result.stdout.strip().split('\n'):
            if line and not line.startswith('Tables_in'):
                tables.append(line.strip())
        return sorted(tables)
    except subprocess.CalledProcessError as e:
        print(f"Error getting MySQL tables: {e}")
        return []

def get_existing_migrations():
    """Get list of existing migration scripts"""
    migrations = []
    for file in os.listdir('.'):
        if file.endswith('_migration.py'):
            # Extract table name from migration script name
            table_name = file.replace('_migration.py', '')
            migrations.append(table_name)
    return sorted(migrations)

def main():
    mysql_tables = get_mysql_tables()
    existing_migrations = get_existing_migrations()
    
    print(f"Total MySQL tables: {len(mysql_tables)}")
    print(f"Existing migration scripts: {len(existing_migrations)}")
    
    # Create mapping from table names to migration script names
    table_to_migration = {}
    for table in mysql_tables:
        # Convert table name to migration script name format
        migration_name = table.lower()
        table_to_migration[table] = migration_name
    
    # Find missing tables
    missing_tables = []
    for table in mysql_tables:
        migration_name = table_to_migration[table]
        if migration_name not in existing_migrations:
            missing_tables.append(table)
    
    print(f"\nMissing migration scripts for {len(missing_tables)} tables:")
    for table in missing_tables:
        print(f"  - {table} -> {table_to_migration[table]}_migration.py")
    
    return missing_tables

if __name__ == "__main__":
    missing_tables = main() 