#!/usr/bin/env python3
"""
ID Preservation Verification Script
===================================

This script checks all migrated tables to ensure that:
1. Original MySQL ID values are preserved in PostgreSQL
2. ID sequences are properly set to continue from the max ID
3. No ID values were lost or modified during migration

Usage: python verify_id_preservation.py
"""

import subprocess
import re
from table_utils import run_command

def get_mysql_tables():
    """Get list of all tables from MySQL"""
    cmd = 'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW TABLES;" --batch --raw --skip-column-names'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        tables = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        return tables
    else:
        print("Failed to get MySQL tables")
        return []

def get_postgresql_tables():
    """Get list of all tables from PostgreSQL"""
    cmd = 'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_type = \'BASE TABLE\';"'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        tables = [line.strip().strip('"') for line in result.stdout.strip().split('\n') if line.strip()]
        return tables
    else:
        print("Failed to get PostgreSQL tables")
        return []

def table_has_id_column(table_name, database='mysql'):
    """Check if a table has an ID column"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "DESCRIBE {table_name};" --batch --raw --skip-column-names'
    else:
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT column_name FROM information_schema.columns WHERE table_name = \'{table_name.lower()}\' AND column_name = \'id\';"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        if database == 'mysql':
            for line in result.stdout.strip().split('\n'):
                if line.strip() and line.split('\t')[0].lower() == 'id':
                    return True
        else:
            return bool(result.stdout.strip())
    return False

def get_id_values_sample(table_name, database='mysql', limit=10):
    """Get a sample of ID values from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT id FROM {table_name} ORDER BY id LIMIT {limit};" --batch --raw --skip-column-names'
    else:
        # Handle case-sensitive table names in PostgreSQL
        quoted_table = f'"{table_name}"' if table_name[0] == '_' or any(c.isupper() for c in table_name) else table_name
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT id FROM {quoted_table} ORDER BY id LIMIT {limit};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        ids = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    ids.append(int(line.strip()))
                except ValueError:
                    continue
        return ids
    return []

def get_max_id(table_name, database='mysql'):
    """Get the maximum ID value from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT MAX(id) FROM {table_name};" --batch --raw --skip-column-names'
    else:
        # Handle case-sensitive table names in PostgreSQL
        quoted_table = f'"{table_name}"' if table_name[0] == '_' or any(c.isupper() for c in table_name) else table_name
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT MAX(id) FROM {quoted_table};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            max_id = result.stdout.strip()
            if max_id and max_id != 'NULL':
                return int(max_id)
        except ValueError:
            pass
    return None

def get_record_count(table_name, database='mysql'):
    """Get the total record count from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT COUNT(*) FROM {table_name};" --batch --raw --skip-column-names'
    else:
        # Handle case-sensitive table names in PostgreSQL
        quoted_table = f'"{table_name}"' if table_name[0] == '_' or any(c.isupper() for c in table_name) else table_name
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM {quoted_table};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            return int(result.stdout.strip())
        except ValueError:
            pass
    return 0

def check_sequence_value(table_name):
    """Check the current value of the ID sequence for a table"""
    # Try different sequence naming patterns
    sequence_names = [
        f'{table_name}_id_seq',
        f'"{table_name}_id_seq"',
        f'{table_name.lower()}_id_seq'
    ]
    
    for seq_name in sequence_names:
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT currval(\'{seq_name}\');" 2>/dev/null'
        result = run_command(cmd)
        
        if result and result.returncode == 0:
            try:
                return int(result.stdout.strip())
            except ValueError:
                continue
    
    return None

def main():
    print("ID Preservation Verification Report")
    print("=" * 60)
    print(f"Date: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}")
    print()
    
    # Get tables from both databases
    mysql_tables = get_mysql_tables()
    postgresql_tables = get_postgresql_tables()
    
    print(f"MySQL tables found: {len(mysql_tables)}")
    print(f"PostgreSQL tables found: {len(postgresql_tables)}")
    print()
    
    # Find common tables
    common_tables = set(mysql_tables) & set(postgresql_tables)
    
    # Also check case-sensitive variations
    mysql_table_map = {t.lower(): t for t in mysql_tables}
    pg_table_map = {t.lower(): t for t in postgresql_tables}
    
    all_common = set(mysql_table_map.keys()) & set(pg_table_map.keys())
    
    print(f"Common tables to check: {len(all_common)}")
    print()
    
    # Results tracking
    tables_with_id = []
    id_preservation_issues = []
    sequence_issues = []
    perfect_migrations = []
    
    print("Checking ID preservation for each table...")
    print("-" * 60)
    
    for table_lower in sorted(all_common):
        mysql_table = mysql_table_map[table_lower]
        pg_table = pg_table_map[table_lower]
        
        # Skip ClientSMS as it was intentionally skipped
        if mysql_table == 'ClientSMS':
            continue
        
        # Check if table has ID column
        mysql_has_id = table_has_id_column(mysql_table, 'mysql')
        
        if not mysql_has_id:
            continue
        
        tables_with_id.append(mysql_table)
        
        # Get record counts
        mysql_count = get_record_count(mysql_table, 'mysql')
        pg_count = get_record_count(pg_table, 'postgresql')
        
        # Get max IDs
        mysql_max_id = get_max_id(mysql_table, 'mysql')
        pg_max_id = get_max_id(pg_table, 'postgresql')
        
        # Get sample ID values
        mysql_ids = get_id_values_sample(mysql_table, 'mysql', 5)
        pg_ids = get_id_values_sample(pg_table, 'postgresql', 5)
        
        # Check sequence value
        sequence_val = check_sequence_value(pg_table)
        
        print(f"Table: {mysql_table}")
        print(f"  Records: MySQL={mysql_count}, PostgreSQL={pg_count}")
        print(f"  Max ID: MySQL={mysql_max_id}, PostgreSQL={pg_max_id}")
        print(f"  Sample IDs: MySQL={mysql_ids[:3]}, PostgreSQL={pg_ids[:3]}")
        
        # Check for issues
        issues = []
        
        # Check record count match
        if mysql_count != pg_count:
            issues.append(f"Record count mismatch: {mysql_count} vs {pg_count}")
        
        # Check max ID preservation
        if mysql_max_id != pg_max_id:
            issues.append(f"Max ID mismatch: {mysql_max_id} vs {pg_max_id}")
            id_preservation_issues.append(mysql_table)
        
        # Check sample ID preservation
        if mysql_ids and pg_ids and mysql_ids[:len(pg_ids)] != pg_ids[:len(mysql_ids)]:
            issues.append(f"Sample IDs don't match")
            if mysql_table not in id_preservation_issues:
                id_preservation_issues.append(mysql_table)
        
        # Check sequence setup
        if sequence_val is not None and mysql_max_id is not None:
            expected_seq = mysql_max_id + 1
            if sequence_val < expected_seq:
                issues.append(f"Sequence too low: {sequence_val}, expected >= {expected_seq}")
                sequence_issues.append(mysql_table)
            print(f"  Sequence: {sequence_val} (expected >= {expected_seq})")
        elif mysql_max_id is not None:
            issues.append("No sequence found")
            sequence_issues.append(mysql_table)
            print(f"  Sequence: NOT FOUND")
        
        if issues:
            print(f"  Issues: {'; '.join(issues)}")
        else:
            print(f"  Status: ✅ PERFECT")
            perfect_migrations.append(mysql_table)
        
        print()
    
    # Summary Report
    print("=" * 60)
    print("SUMMARY REPORT")
    print("=" * 60)
    print(f"Tables with ID columns checked: {len(tables_with_id)}")
    print(f"Perfect migrations: {len(perfect_migrations)}")
    print(f"ID preservation issues: {len(id_preservation_issues)}")
    print(f"Sequence setup issues: {len(sequence_issues)}")
    print()
    
    if id_preservation_issues:
        print("❌ TABLES WITH ID PRESERVATION ISSUES:")
        for table in id_preservation_issues:
            print(f"   - {table}")
        print()
    
    if sequence_issues:
        print("⚠️  TABLES WITH SEQUENCE ISSUES:")
        for table in sequence_issues:
            print(f"   - {table}")
        print()
    
    if perfect_migrations:
        print("✅ PERFECTLY MIGRATED TABLES:")
        for table in perfect_migrations[:10]:  # Show first 10
            print(f"   - {table}")
        if len(perfect_migrations) > 10:
            print(f"   ... and {len(perfect_migrations) - 10} more")
    
    print()
    success_rate = len(perfect_migrations) / len(tables_with_id) * 100 if tables_with_id else 0
    print(f"ID Preservation Success Rate: {success_rate:.1f}%")
    
    if id_preservation_issues or sequence_issues:
        print("\n⚠️  ISSUES FOUND - Review required!")
        return False
    else:
        print("\n✅ ALL ID VALUES PERFECTLY PRESERVED!")
        return True

if __name__ == "__main__":
    main()
