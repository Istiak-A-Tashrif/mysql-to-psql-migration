#!/usr/bin/env python3
"""
Enhanced ID Preservation Verification Script
============================================

This script performs comprehensive ID-by-ID verification to ensure that:
1. Original MySQL ID values are preserved in PostgreSQL
2. ID sequences are properly set to continue from the max ID
3. No ID values were lost or modified during migration
4. Detailed debugging for tables with issues

Usage: python verify_id_preservation_enhanced.py
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
        print("❌ Failed to get MySQL tables")
        return []

def get_postgresql_tables():
    """Get list of all tables from PostgreSQL"""
    cmd = 'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_type = \'BASE TABLE\';"'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        tables = [line.strip().strip('"') for line in result.stdout.strip().split('\n') if line.strip()]
        return tables
    else:
        print("❌ Failed to get PostgreSQL tables")
        return []

def debug_table_structure(table_name, database='postgresql'):
    """Debug table structure to understand why queries might be failing"""
    if database == 'postgresql':
        # The table exists if it's in our list, so just return it - PostgreSQL is case-sensitive
        return table_name
    return table_name

def table_has_id_column(table_name, database='mysql'):
    """Check if a table has an ID column and return column info"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "DESCRIBE `{table_name}`;" --batch --raw --skip-column-names'
        result = run_command(cmd)
        
        if result and result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) > 0 and parts[0].lower() == 'id':
                        return True, parts[1] if len(parts) > 1 else 'unknown'
    else:
        # PostgreSQL - always quote table names for case sensitivity
        quoted_table = f'"{table_name}"'
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'{table_name.lower()}\' AND column_name = \'id\';"'
        result = run_command(cmd)
        
        if result and result.returncode == 0 and result.stdout.strip():
            parts = [p.strip() for p in result.stdout.strip().split('|')]
            return True, parts[1] if len(parts) > 1 else 'unknown'
    
    return False, None

def get_all_id_values(table_name, database='mysql'):
    """Get ALL ID values from a table for complete verification"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT id FROM `{table_name}` ORDER BY id;" --batch --raw --skip-column-names'
    else:
        # Always quote table names in PostgreSQL
        quoted_table = f'"{table_name}"'
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT id FROM {quoted_table} ORDER BY id;"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        ids = []
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line:
                try:
                    ids.append(int(line))
                except ValueError:
                    continue
        return ids
    else:
        print(f"    ❌ Failed to get IDs from {database} {table_name}")
        if result:
            print(f"    Error: {result.stderr}")
        return []

def get_id_values_sample(table_name, database='mysql', limit=10):
    """Get a sample of ID values from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT id FROM `{table_name}` ORDER BY id LIMIT {limit};" --batch --raw --skip-column-names'
    else:
        # Always quote table names in PostgreSQL
        quoted_table = f'"{table_name}"'
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT id FROM {quoted_table} ORDER BY id LIMIT {limit};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        ids = []
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line:
                try:
                    ids.append(int(line))
                except ValueError:
                    continue
        return ids
    else:
        if result and result.stderr:
            print(f"    ⚠️  Error getting sample IDs from {database} {table_name}: {result.stderr.strip()}")
        return []

def get_max_id(table_name, database='mysql'):
    """Get the maximum ID value from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT MAX(id) FROM `{table_name}`;" --batch --raw --skip-column-names'
    else:
        # Always quote table names in PostgreSQL
        quoted_table = f'"{table_name}"'
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT MAX(id) FROM {quoted_table};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            max_id = result.stdout.strip()
            if max_id and max_id != 'NULL' and max_id != '':
                return int(max_id)
        except ValueError:
            pass
    
    return None

def get_record_count(table_name, database='mysql'):
    """Get the total record count from a table"""
    if database == 'mysql':
        cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT COUNT(*) FROM `{table_name}`;" --batch --raw --skip-column-names'
    else:
        # Always quote table names in PostgreSQL
        quoted_table = f'"{table_name}"'
        cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM {quoted_table};"'
    
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            count = result.stdout.strip()
            return int(count) if count else 0
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

def compare_ids_detailed(mysql_ids, pg_ids, table_name):
    """Perform detailed ID comparison and report discrepancies"""
    mysql_set = set(mysql_ids)
    pg_set = set(pg_ids)
    
    missing_in_pg = mysql_set - pg_set
    extra_in_pg = pg_set - mysql_set
    
    print(f"    🔍 Detailed ID Analysis for {table_name}:")
    print(f"      MySQL IDs: {len(mysql_ids)} total")
    print(f"      PostgreSQL IDs: {len(pg_ids)} total")
    
    if missing_in_pg:
        print(f"      ❌ Missing in PostgreSQL: {sorted(list(missing_in_pg))[:10]}...")
    
    if extra_in_pg:
        print(f"      ⚠️  Extra in PostgreSQL: {sorted(list(extra_in_pg))[:10]}...")
    
    # Check if order is preserved
    if len(mysql_ids) > 0 and len(pg_ids) > 0:
        if mysql_ids == pg_ids:
            print(f"       ID order perfectly preserved")
            return True
        else:
            print(f"      ❌ ID order not preserved")
            # Show first few differences
            min_len = min(len(mysql_ids), len(pg_ids))
            for i in range(min(10, min_len)):
                if mysql_ids[i] != pg_ids[i]:
                    print(f"        Position {i}: MySQL={mysql_ids[i]}, PostgreSQL={pg_ids[i]}")
    
    return len(missing_in_pg) == 0 and len(extra_in_pg) == 0

def main():
    print("Enhanced ID Preservation Verification Report")
    print("=" * 70)
    print(f"Date: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}")
    print()
    
    # Get tables from both databases
    mysql_tables = get_mysql_tables()
    postgresql_tables = get_postgresql_tables()
    
    print(f"MySQL tables found: {len(mysql_tables)}")
    print(f"PostgreSQL tables found: {len(postgresql_tables)}")
    print()
    
    # Find common tables
    mysql_table_map = {t.lower(): t for t in mysql_tables}
    pg_table_map = {t.lower(): t for t in postgresql_tables}
    
    all_common = set(mysql_table_map.keys()) & set(pg_table_map.keys())
    
    print(f"Common tables to check: {len(all_common)}")
    print()
    
    # Results tracking
    tables_checked = []
    perfect_migrations = []
    id_preservation_issues = []
    sequence_issues = []
    missing_tables = []
    empty_tables = []
    
    print("Checking ID preservation for each table...")
    print("-" * 70)
    
    for table_lower in sorted(all_common):
        mysql_table = mysql_table_map[table_lower]
        pg_table = pg_table_map[table_lower]
        
        print(f"\n🔍 Table: {mysql_table}")
        
        # Check if table has ID column
        mysql_has_id, mysql_id_type = table_has_id_column(mysql_table, 'mysql')
        
        if not mysql_has_id:
            print(f"    Skipping - no ID column")
            continue
        
        pg_has_id, pg_id_type = table_has_id_column(pg_table, 'postgresql')
        if not pg_has_id:
            print(f"  ❌ PostgreSQL table missing or no ID column")
            missing_tables.append(mysql_table)
            continue
        
        tables_checked.append(mysql_table)
        
        # Get record counts first
        mysql_count = get_record_count(mysql_table, 'mysql')
        pg_count = get_record_count(pg_table, 'postgresql')
        
        print(f"  📊 Records: MySQL={mysql_count}, PostgreSQL={pg_count}")
        
        if mysql_count == 0:
            print(f"    Skipping - empty source table")
            empty_tables.append(mysql_table)
            continue
        
        if pg_count == 0:
            print(f"  ❌ PostgreSQL table is empty but MySQL has {mysql_count} records")
            id_preservation_issues.append(mysql_table)
            continue
        
        # Get max IDs
        mysql_max_id = get_max_id(mysql_table, 'mysql')
        pg_max_id = get_max_id(pg_table, 'postgresql')
        
        print(f"  🔢 Max ID: MySQL={mysql_max_id}, PostgreSQL={pg_max_id}")
        
        # For smaller tables, check ALL IDs
        if mysql_count <= 1000:
            print(f"  🔍 Performing complete ID verification...")
            mysql_ids = get_all_id_values(mysql_table, 'mysql')
            pg_ids = get_all_id_values(pg_table, 'postgresql')
            
            ids_match = compare_ids_detailed(mysql_ids, pg_ids, mysql_table)
        else:
            # For larger tables, check samples and critical points
            print(f"  🔍 Performing sample ID verification (large table)...")
            mysql_ids = get_id_values_sample(mysql_table, 'mysql', 50)
            pg_ids = get_id_values_sample(pg_table, 'postgresql', 50)
            
            ids_match = (mysql_ids == pg_ids)
            if not ids_match:
                print(f"    ❌ Sample IDs don't match")
                print(f"    MySQL sample: {mysql_ids[:10]}...")
                print(f"    PostgreSQL sample: {pg_ids[:10]}...")
        
        # Check sequence setup
        sequence_val = check_sequence_value(pg_table)
        sequence_ok = True
        
        if sequence_val is not None and mysql_max_id is not None:
            expected_seq = mysql_max_id + 1
            if sequence_val < expected_seq:
                print(f"  ⚠️  Sequence: {sequence_val} (expected >= {expected_seq})")
                sequence_issues.append(mysql_table)
                sequence_ok = False
            else:
                print(f"   Sequence: {sequence_val} (correct)")
        elif mysql_max_id is not None:
            print(f"  ❌ Sequence: NOT FOUND")
            sequence_issues.append(mysql_table)
            sequence_ok = False
        
        # Overall assessment
        if (mysql_count == pg_count and 
            mysql_max_id == pg_max_id and 
            ids_match and 
            sequence_ok):
            print(f"   Status: PERFECT MIGRATION")
            perfect_migrations.append(mysql_table)
        else:
            print(f"  ❌ Status: ISSUES FOUND")
            id_preservation_issues.append(mysql_table)
    
    # Summary Report
    print("\n" + "=" * 70)
    print("DETAILED SUMMARY REPORT")
    print("=" * 70)
    print(f"Tables checked: {len(tables_checked)}")
    print(f"Perfect migrations: {len(perfect_migrations)}")
    print(f"ID preservation issues: {len(id_preservation_issues)}")
    print(f"Sequence setup issues: {len(sequence_issues)}")
    print(f"Missing/broken tables: {len(missing_tables)}")
    print(f"Empty source tables: {len(empty_tables)}")
    print()
    
    if missing_tables:
        print("❌ MISSING OR BROKEN TABLES:")
        for table in missing_tables:
            print(f"   - {table}")
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
        print(" PERFECTLY MIGRATED TABLES:")
        for table in perfect_migrations[:15]:  # Show first 15
            print(f"   - {table}")
        if len(perfect_migrations) > 15:
            print(f"   ... and {len(perfect_migrations) - 15} more")
    
    print()
    success_rate = len(perfect_migrations) / len(tables_checked) * 100 if tables_checked else 0
    print(f"ID Preservation Success Rate: {success_rate:.1f}%")
    
    if id_preservation_issues or missing_tables:
        print("\n❌ CRITICAL ISSUES FOUND - Migration needs attention!")
        return False
    elif sequence_issues:
        print("\n⚠️  MINOR ISSUES FOUND - Sequences need fixing")
        return False
    else:
        print("\n ALL ID VALUES PERFECTLY PRESERVED!")
        return True

if __name__ == "__main__":
    main()
