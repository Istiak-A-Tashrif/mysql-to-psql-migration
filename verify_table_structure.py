#!/usr/bin/env python3
"""
Generic Table Structure Verification Script
==========================================

This script compares any table structure between MySQL source 
and PostgreSQL target databases.

Usage: python verify_table_structure.py <table_name>
Example: python verify_table_structure.py Appointment
"""

import sys
from table_utils import (
    check_docker_containers,
    get_mysql_table_columns,
    get_postgresql_table_columns, 
    compare_table_structures,
    count_table_records,
    run_command,
    analyze_column_differences
)

def get_mysql_table_structure(table_name):
    """Get table structure from MySQL"""
    print(f" Getting MySQL {table_name} table structure...")
    
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "DESCRIBE {table_name};"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f" Failed to get MySQL structure: {result.stderr if result else 'Connection failed'}")
        return None
    
    print(f" MySQL {table_name} table structure:")
    print(result.stdout)
    return result.stdout

def get_postgresql_table_structure(table_name):
    """Get table structure from PostgreSQL"""
    print(f" Getting PostgreSQL {table_name.lower()} table structure...")
    
    # Use information_schema for detailed column info
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -c "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = \'{table_name.lower()}\' ORDER BY ordinal_position;"'
    
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f" Failed to get PostgreSQL structure: {result.stderr if result else 'Connection failed'}")
        return None
    
    if not result.stdout or 'column_name' not in result.stdout:
        print(f" Table '{table_name.lower()}' does not exist in PostgreSQL")
        return None
    
    print(f" PostgreSQL {table_name.lower()} table structure:")
    print(result.stdout)
    return result.stdout

def quick_data_sample(table_name):
    """Get a quick sample of data from both tables"""
    print(f" Getting data samples from {table_name}...")
    
    # Get first few columns for sample (assuming id exists)
    mysql_cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SELECT * FROM {table_name} LIMIT 3;"'
    mysql_result = run_command(mysql_cmd)
    
    postgres_cmd = f'docker exec postgres_target psql -U postgres -d target_db -c "SELECT * FROM {table_name.lower()} LIMIT 3;"'
    postgres_result = run_command(postgres_cmd)
    
    if mysql_result and mysql_result.returncode == 0:
        print(f" MySQL {table_name} sample data:")
        print(mysql_result.stdout)
    else:
        print(f" Could not get MySQL {table_name} sample data")
    
    if postgres_result and postgres_result.returncode == 0:
        print(f" PostgreSQL {table_name.lower()} sample data:")
        print(postgres_result.stdout)
    else:
        print(f" Could not get PostgreSQL {table_name.lower()} sample data")

def main():
    """Main verification function"""
    if len(sys.argv) != 2:
        print(" Usage: python verify_table_structure.py <table_name>")
        print("   Example: python verify_table_structure.py Appointment")
        return False
    
    table_name = sys.argv[1]
    
    print(f" {table_name} Table Structure Verification")
    print("=" * 50)
    
    # Check Docker containers
    if not check_docker_containers():
        return False
    
    print("\n" + "=" * 50)
    
    # Get table structures
    mysql_structure = get_mysql_table_structure(table_name)
    print("\n" + "-" * 30)
    postgres_structure = get_postgresql_table_structure(table_name)
    
    if not mysql_structure:
        print(f" Could not get MySQL {table_name} table structure")
        return False
    
    if not postgres_structure:
        print(f" Could not get PostgreSQL {table_name.lower()} table structure")
        print("💡 Have you run the migration script yet?")
        return False
    
    # Use the utility function for detailed comparison
    print("\n" + "=" * 50)
    structure_match = compare_table_structures(table_name)
    
    print("\n" + "=" * 50)
    
    # Count records
    count_success, mysql_count, postgres_count = count_table_records(table_name)
    
    print("\n" + "=" * 50)
    
    # Get data samples
    quick_data_sample(table_name)
    
    # Analyze differences and suggest fixes
    analyze_column_differences(table_name)
    
    print("\n" + "=" * 50)
    print(" SUMMARY:")
    print(" Both tables exist and are accessible")
    print(f" Record counts: MySQL={mysql_count}, PostgreSQL={postgres_count}")
    
    if mysql_count == postgres_count and mysql_count != "Error":
        print(" Basic verification passed!")
    else:
        print(" Issues detected - investigate further")
    
    overall_success = structure_match and count_success
    
    print("\n" + "=" * 50)
    if overall_success:
        print(" COMPLETE VERIFICATION PASSED!")
    else:
        print(" Some issues found - review details above")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
