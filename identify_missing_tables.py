#!/usr/bin/env python3
"""
Identify Missing Migration Scripts
==================================

This script identifies which MySQL tables still need migration scripts
by comparing existing migration scripts with the MySQL tables list.
"""

import os
import glob

def get_existing_scripts():
    """Get list of existing migration scripts"""
    scripts = []
    for script in glob.glob("*_migration.py"):
        # Remove _migration.py suffix and get table name
        table_name = script.replace("_migration.py", "")
        scripts.append(table_name)
    return scripts

def get_mysql_tables():
    """Get list of MySQL tables from mysql_tables.txt"""
    with open("mysql_tables.txt", "r") as f:
        tables = [line.strip() for line in f.readlines() if line.strip()]
    return tables

def main():
    existing_scripts = get_existing_scripts()
    mysql_tables = get_mysql_tables()
    
    print(f"📊 Found {len(existing_scripts)} existing migration scripts")
    print(f"📊 Found {len(mysql_tables)} MySQL tables")
    
    # Find missing tables
    missing_tables = []
    for table in mysql_tables:
        # Skip system tables and special tables
        if table.startswith("_"):
            continue
        if table in ["_prisma_migrations", "_UserGroups"]:
            continue
            
        # Check if migration script exists
        script_name = f"{table.lower()}_migration.py"
        if not os.path.exists(script_name):
            missing_tables.append(table)
    
    print(f"\n Missing migration scripts for {len(missing_tables)} tables:")
    for i, table in enumerate(missing_tables, 1):
        print(f"{i:2d}. {table}")
    
    print(f"\n📝 Total missing: {len(missing_tables)} tables")
    return missing_tables

if __name__ == "__main__":
    missing_tables = main() 