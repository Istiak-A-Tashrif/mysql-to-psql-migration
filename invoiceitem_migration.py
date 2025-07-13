#!/usr/bin/env python3
"""
InvoiceItem Table Migration Script
==================================

This script provides a complete 3-phase migration approach specifically for the InvoiceItem table:
1. Phase 1: Table + Data (without constraints)
2. Phase 2: Indexes (after data import for performance)
3. Phase 3: Foreign Keys (after all tables exist)

Features:
- Preserves MySQL case sensitivity for table and column names
- Handles InvoiceItem-specific data types and constraints
- Manages foreign key dependencies (Invoice, Service, Labor)
- Creates appropriate indexes for InvoiceItem table
- Medium table with 1,202+ invoice item records
- Handles invoice line items with service and labor references

Usage: 
    python invoiceitem_migration.py --phase=1
    python invoiceitem_migration.py --phase=2
    python invoiceitem_migration.py --phase=3
    python invoiceitem_migration.py --full
    python invoiceitem_migration.py --verify
"""

import re
import os
import argparse
from collections import OrderedDict
from table_utils import (
    verify_table_structure,
    run_command,
    create_postgresql_table,
    export_and_clean_mysql_data,
    import_data_to_postgresql,
    add_primary_key_constraint,
    setup_auto_increment_sequence,
    execute_postgresql_sql
)

# Configuration: Set to True to preserve MySQL naming convention in PostgreSQL
PRESERVE_MYSQL_CASE = True
TABLE_NAME = "InvoiceItem"

def get_invoiceitem_table_info():
    """Get complete InvoiceItem table information from MySQL including constraints"""
    print(f"🔍 Getting complete table info for {TABLE_NAME} from MySQL...")
    
    # Get CREATE TABLE statement
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{TABLE_NAME}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get {TABLE_NAME} table info from MySQL")
        return None, [], []
    
    # Extract DDL
    lines = result.stdout.strip().split("\n")
    ddl_line = None
    for line in lines:
        # Look for lines containing CREATE TABLE (could be in second column)
        if "CREATE TABLE" in line:
            # Split by tab and take the part with CREATE TABLE
            parts = line.split("\t")
            for part in parts:
                if "CREATE TABLE" in part:
                    ddl_line = part
                    break
            if ddl_line:
                break
    
    if not ddl_line:
        print(f"❌ Could not find CREATE TABLE statement for {TABLE_NAME}")
        print("Debug: MySQL output:")
        print(result.stdout)
        return None, [], []
    
    mysql_ddl = ddl_line.strip()
    
    # Extract indexes and foreign keys
    indexes = extract_invoiceitem_indexes_from_ddl(mysql_ddl)
    foreign_keys = extract_invoiceitem_foreign_keys_from_ddl(mysql_ddl)
    
    print(f"✅ Found {len(indexes)} indexes and {len(foreign_keys)} foreign keys for {TABLE_NAME} table")
    return mysql_ddl, indexes, foreign_keys

def extract_invoiceitem_indexes_from_ddl(ddl):
    """Extract index definitions from InvoiceItem table MySQL DDL"""
    indexes = []
    
    # Pattern for KEY definitions
    key_pattern = r'(?:UNIQUE\s+)?KEY\s+`([^`]+)`\s*\(([^)]+)\)'
    
    matches = re.finditer(key_pattern, ddl, re.IGNORECASE)
    for match in matches:
        index_name = match.group(1)
        columns = match.group(2)
        is_unique = 'UNIQUE' in match.group(0).upper()
        
        indexes.append({
            'name': index_name,
            'columns': columns,
            'unique': is_unique,
            'original': match.group(0),
            'table': 'InvoiceItem'
        })
    
    return indexes

def extract_invoiceitem_foreign_keys_from_ddl(ddl):
    """Extract foreign key definitions from InvoiceItem table MySQL DDL"""
    foreign_keys = []
    
    # Pattern for CONSTRAINT FOREIGN KEY specific to InvoiceItem - handle multi-word actions like "SET NULL"
    fk_pattern = r'CONSTRAINT\s+`([^`]+)`\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`([^`]+)`\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+([A-Z][A-Z\s]*?)(?=\s+ON|\s*$))?(?:\s+ON\s+UPDATE\s+([A-Z][A-Z\s]*?)(?=\s*$|\s*,))?'
    
    matches = re.finditer(fk_pattern, ddl, re.IGNORECASE)
    for match in matches:
        constraint_name = match.group(1)
        local_columns = match.group(2)
        ref_table = match.group(3)
        ref_columns = match.group(4)
        on_delete = match.group(5).strip() if match.group(5) else 'RESTRICT'
        on_update = match.group(6).strip() if match.group(6) else 'RESTRICT'
        
        foreign_keys.append({
            'name': constraint_name,
            'local_columns': local_columns,
            'ref_table': ref_table,
            'ref_columns': ref_columns,
            'on_delete': on_delete,
            'on_update': on_update,
            'original': match.group(0),
            'table': 'InvoiceItem'
        })
    
    return foreign_keys

def convert_invoiceitem_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=True):
    """Convert InvoiceItem table MySQL DDL to PostgreSQL DDL with InvoiceItem-specific optimizations"""
    print(f"🔄 Converting InvoiceItem table MySQL DDL to PostgreSQL (constraints: {include_constraints}, preserve_case: {preserve_case})...")
    
    # Fix literal \n characters to actual newlines first
    postgres_ddl = mysql_ddl.replace('\\n', '\n')
    
    # Extract just the column definitions part
    create_match = re.search(r'CREATE TABLE `[^`]+`\s*\((.*?)\)\s*ENGINE', postgres_ddl, re.DOTALL)
    if not create_match:
        print(f"❌ Could not parse CREATE TABLE statement for {TABLE_NAME}")
        return None
    
    columns_part = create_match.group(1)
    
    # Parse individual columns, indexes, and constraints
    lines = []
    for line in columns_part.split(',\n'):
        line = line.strip()
        if not line:
            continue
            
        # Skip constraints for now if include_constraints is False
        if not include_constraints and (
            line.startswith('PRIMARY KEY') or 
            line.startswith('KEY') or 
            line.startswith('UNIQUE KEY') or 
            line.startswith('CONSTRAINT')
        ):
            continue
            
        # Process column definitions
        if not (line.startswith('PRIMARY KEY') or line.startswith('KEY') or 
                line.startswith('UNIQUE KEY') or line.startswith('CONSTRAINT')):
            # This is a column definition
            processed_line = process_invoiceitem_column_definition(line, preserve_case)
            if processed_line:
                lines.append(processed_line)
    
    # Build the PostgreSQL DDL
    table_name_pg = f'"{TABLE_NAME}"' if preserve_case else TABLE_NAME.lower()
    postgres_ddl = f"CREATE TABLE {table_name_pg} (\n"
    postgres_ddl += ",\n".join([f"  {line}" for line in lines])
    postgres_ddl += "\n)"
    
    return postgres_ddl

def process_invoiceitem_column_definition(line, preserve_case):
    """Process a single column definition for InvoiceItem table"""
    # Remove backticks and handle MySQL-specific types
    line = line.replace('`', '"' if preserve_case else '')
    
    # MySQL to PostgreSQL type conversions for InvoiceItem
    conversions = [
        (r'\btinyint\(1\)\b', 'BOOLEAN'),
        (r'\btinyint\([^)]+\)\b', 'SMALLINT'),
        (r'\bsmallint\([^)]+\)\b', 'SMALLINT'),
        (r'\bmediumint\([^)]+\)\b', 'INTEGER'),
        (r'\bint\([^)]+\)\b', 'INTEGER'),
        (r'\bbigint\([^)]+\)\b', 'BIGINT'),
        (r'\bint\b', 'INTEGER'),
        (r'\bvarchar\([^)]+\)\b', 'VARCHAR'),
        (r'\btext\b', 'TEXT'),
        (r'\blongtext\b', 'TEXT'),
        (r'\bmediumtext\b', 'TEXT'),
        (r'\btinytext\b', 'TEXT'),
        (r'\bdatetime\([^)]+\)\b', 'TIMESTAMP'),
        (r'\bdatetime\b', 'TIMESTAMP'),
        (r'\btimestamp\([^)]+\)\b', 'TIMESTAMP'),
        (r'\btimestamp\b', 'TIMESTAMP'),
        (r'\bdate\b', 'DATE'),
        (r'\btime\b', 'TIME'),
        (r'\bdouble\b', 'DOUBLE PRECISION'),
        (r'\bfloat\b', 'REAL'),
        (r'\bdecimal\([^)]+\)\b', 'DECIMAL'),
        (r'\bjson\b', 'JSON'),
        (r'\bblob\b', 'BYTEA'),
        (r'\blongblob\b', 'BYTEA'),
        (r'\bmediumblob\b', 'BYTEA'),
        (r'\btinyblob\b', 'BYTEA'),
    ]
    
    for pattern, replacement in conversions:
        line = re.sub(pattern, replacement, line, flags=re.IGNORECASE)
    
    # Additional manual fixes for common issues
    line = line.replace("tinyint(1)", "BOOLEAN")  # Force tinyint(1) to BOOLEAN
    line = line.replace("tinyint", "SMALLINT")    # Any other tinyint to SMALLINT
    
    # Handle AUTO_INCREMENT
    line = re.sub(r'\bAUTO_INCREMENT\b', '', line, flags=re.IGNORECASE)
    
    # Handle MySQL DEFAULT expressions
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP\(\d*\)", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    line = re.sub(r"DEFAULT\s+CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP", line, flags=re.IGNORECASE)
    
    # Handle MySQL character set and collation
    line = re.sub(r'\s+CHARACTER\s+SET\s+[^\s]+', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s+COLLATE\s+[^\s]+', '', line, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    line = re.sub(r'\s+', ' ', line).strip()
    
    return line

def create_invoiceitem_table(mysql_ddl):
    """Create InvoiceItem table in PostgreSQL"""
    postgres_ddl = convert_invoiceitem_mysql_to_postgresql_ddl(mysql_ddl, include_constraints=False, preserve_case=PRESERVE_MYSQL_CASE)
    if not postgres_ddl:
        return False
    
    print(f"📋 Generated PostgreSQL DDL for {TABLE_NAME}:")
    print("=" * 50)
    print(postgres_ddl)
    print("=" * 50)
    
    return create_postgresql_table(TABLE_NAME, postgres_ddl, PRESERVE_MYSQL_CASE)

def create_invoiceitem_indexes(indexes):
    """Create indexes for InvoiceItem table"""
    if not indexes:
        print(f"ℹ️ No indexes to create for {TABLE_NAME}")
        return True
    
    print(f"📊 Creating {len(indexes)} indexes for {TABLE_NAME}...")
    
    success = True
    for index in indexes:
        index_name = f"{TABLE_NAME.lower()}_{index['name']}"
        columns = index['columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        table_name = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        
        # Check if index already exists
        table_name_for_check = TABLE_NAME if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT indexname FROM pg_indexes WHERE tablename = \'{table_name_for_check}\' AND indexname = \'{index_name}\';"'
        check_result = run_command(check_cmd)
        
        if check_result and check_result.returncode == 0 and check_result.stdout.strip():
            print(f"⏭️ Skipping existing index: {index_name}")
            continue
        
        unique_clause = "UNIQUE " if index.get('unique', False) else ""
        index_sql = f'CREATE {unique_clause}INDEX "{index_name}" ON {table_name} ({columns});'
        
        print(f"🔧 Creating {TABLE_NAME} index: {index['name']}")
        success_flag, result = execute_postgresql_sql(index_sql, f"{TABLE_NAME} index {index['name']}")
        
        if success_flag and result and "CREATE INDEX" in result.stdout:
            print(f"✅ Created {TABLE_NAME} index: {index['name']}")
        else:
            error_msg = result.stderr if result else "No result"
            print(f"❌ Failed to create {TABLE_NAME} index {index['name']}: {error_msg}")
            success = False
    
    return success

def create_invoiceitem_foreign_keys(foreign_keys):
    """Create foreign keys for InvoiceItem table"""
    if not foreign_keys:
        print(f"ℹ️ No foreign keys to create for {TABLE_NAME}")
        return True
    
    print(f"🔗 Creating {len(foreign_keys)} foreign keys for {TABLE_NAME}...")
    
    created_count = 0
    skipped_count = 0
    
    for fk in foreign_keys:
        constraint_name = f"{TABLE_NAME}_{fk['name']}"
        local_columns = fk['local_columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        ref_table = f'"{fk["ref_table"]}"' if PRESERVE_MYSQL_CASE else fk['ref_table'].lower()
        ref_columns = fk['ref_columns'].replace('`', '"' if PRESERVE_MYSQL_CASE else '')
        table_name = f'"{TABLE_NAME}"' if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        
        # Check if foreign key already exists
        table_name_for_check = TABLE_NAME if PRESERVE_MYSQL_CASE else TABLE_NAME.lower()
        check_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = \'{table_name_for_check}\' AND constraint_type = \'FOREIGN KEY\' AND constraint_name = \'{constraint_name}\';"'
        check_result = run_command(check_cmd)
        
        if check_result and check_result.returncode == 0 and check_result.stdout.strip():
            print(f"⏭️ Skipping existing FK: {constraint_name}")
            skipped_count += 1
            continue
        
        # Create the foreign key constraint
        fk_sql = f'ALTER TABLE {table_name} ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ({local_columns}) REFERENCES {ref_table} ({ref_columns});'
        
        print(f"🔧 Creating {TABLE_NAME} FK: {constraint_name} -> {fk['ref_table']}")
        success, result = execute_postgresql_sql(fk_sql, f"{TABLE_NAME} FK {constraint_name}")
        
        if success and result and "ALTER TABLE" in result.stdout:
            print(f"✅ Created {TABLE_NAME} FK: {constraint_name}")
            created_count += 1
        else:
            error_msg = result.stderr if result else "No result"
            print(f"❌ Failed to create {TABLE_NAME} FK {constraint_name}: {error_msg}")
    
    print(f"🎯 {TABLE_NAME} Foreign Keys: {created_count} created, {skipped_count} skipped")
    return True

def import_invoiceitem_data_custom():
    """Custom import for InvoiceItem data"""
    print("📥 Importing InvoiceItem data using custom method...")
    
    # Drop existing data
    drop_cmd = 'docker exec postgres_target psql -U postgres -d target_db -c "DELETE FROM \"InvoiceItem\";"'
    result = run_command(drop_cmd)
    
    # Export using basic tab-separated format
    print("🔄 Exporting InvoiceItem data with proper escaping...")
    
    export_cmd = f'''docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT id, invoice_id, service_id, labor_id, created_at, updated_at, service_desc FROM InvoiceItem" -B --skip-column-names'''
    result = run_command(export_cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to export InvoiceItem data: {result.stderr if result else 'No result'}")
        return False
    
    # Process the tab-separated data and convert to proper CSV
    try:
        # Read the raw output and process it correctly
        raw_data = result.stdout
        
        # Split by lines first, then reconstruct rows properly
        lines = raw_data.splitlines()
        csv_lines = []
        current_row = []
        field_count = 7  # InvoiceItem has 7 fields
        
        for line in lines:
            if not line.strip():
                continue
                
            # Split by tab
            fields = line.split('\t')
            
            if len(fields) == field_count:
                # Complete row
                csv_lines.append(process_csv_row(fields))
            elif len(fields) < field_count:
                # Incomplete row - accumulate
                current_row.extend(fields)
                if len(current_row) == field_count:
                    csv_lines.append(process_csv_row(current_row))
                    current_row = []
            else:
                # Too many fields - this shouldn't happen
                print(f"⚠️ Skipping malformed row with {len(fields)} fields")
        
        # Handle any remaining fields
        if current_row and len(current_row) == field_count:
            csv_lines.append(process_csv_row(current_row))
        
        print(f"📊 Processed {len(csv_lines)} rows from export")
        
        # Write processed CSV
        with open('InvoiceItem_processed.csv', 'w', encoding='utf-8') as f:
            f.write('\n'.join(csv_lines))
        
        # Copy to PostgreSQL container
        copy_cmd = 'docker cp InvoiceItem_processed.csv postgres_target:/tmp/InvoiceItem_import.csv'
        result = run_command(copy_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to copy processed CSV: {result.stderr if result else 'No result'}")
            return False
        
        # Import using COPY command
        copy_sql = '''COPY "InvoiceItem" ("id", "invoice_id", "service_id", "labor_id", "created_at", "updated_at", "service_desc") FROM '/tmp/InvoiceItem_import.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '"', NULL '');'''
        
        with open('import_invoiceitem.sql', 'w', encoding='utf-8') as f:
            f.write(copy_sql)
        
        # Copy SQL file to container
        copy_sql_cmd = 'docker cp import_invoiceitem.sql postgres_target:/tmp/import_invoiceitem.sql'
        result = run_command(copy_sql_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to copy SQL file: {result.stderr if result else 'No result'}")
            return False
        
        # Execute the import
        import_cmd = 'docker exec postgres_target psql -U postgres -d target_db -f /tmp/import_invoiceitem.sql'
        result = run_command(import_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to import InvoiceItem data: {result.stderr if result else 'No result'}")
            if result:
                print(f"🔍 Import command stdout: {result.stdout}")
            return False
        
        print(f"✅ Successfully imported InvoiceItem data")
        return True
        
    finally:
        # Clean up temporary files
        cleanup_cmds = [
            'rm -f InvoiceItem_processed.csv',
            'rm -f import_invoiceitem.sql',
            'docker exec postgres_target rm -f /tmp/InvoiceItem_import.csv',
            'docker exec postgres_target rm -f /tmp/import_invoiceitem.sql'
        ]
        
        for cmd in cleanup_cmds:
            run_command(cmd)

def process_csv_row(fields):
    """Process a row of fields and convert to proper CSV format"""
    # service_id and labor_id are nullable integers (indices 2 and 3)
    out_fields = []
    for i, field in enumerate(fields):
        if field == 'NULL' or field.strip() == '':
            if i in [2, 3]:  # service_id and labor_id
                out_fields.append('')  # unquoted for nullable integers
            else:
                out_fields.append('""')  # quoted empty for others
        else:
            # Clean up the field and handle newlines and quotes
            field = field.strip().replace('"', '""').replace('\n', '\\n').replace('\r', '\\r')
            out_fields.append(f'"{field}"')
    return ','.join(out_fields)

def phase1_create_table_and_data():
    """Phase 1: Create InvoiceItem table and import data"""
    print(f"🚀 Phase 1: Creating {TABLE_NAME} table and importing data")
    
    # Get table info from MySQL
    mysql_ddl, indexes, foreign_keys = get_invoiceitem_table_info()
    if not mysql_ddl:
        return False
    
    # Create table
    if not create_invoiceitem_table(mysql_ddl):
        return False
    
    # Import data
    if not import_invoiceitem_data_custom():
        return False
    
    # Add primary key constraint
    if not add_primary_key_constraint(TABLE_NAME, PRESERVE_MYSQL_CASE):
        return False
    
    # Setup auto-increment sequence
    if not setup_auto_increment_sequence(TABLE_NAME, PRESERVE_MYSQL_CASE):
        return False
    
    print(f"✅ Phase 1 complete for {TABLE_NAME}")
    return True

def phase2_create_indexes():
    """Phase 2: Create indexes for InvoiceItem table"""
    print(f"📊 Phase 2: Creating indexes for {TABLE_NAME}")
    
    # Get indexes from MySQL
    mysql_ddl, indexes, foreign_keys = get_invoiceitem_table_info()
    if mysql_ddl is None:
        return False
    
    return create_invoiceitem_indexes(indexes)

def phase3_create_foreign_keys():
    """Phase 3: Create foreign keys for InvoiceItem table"""
    print(f"🔗 Phase 3: Creating foreign keys for {TABLE_NAME}")
    
    # Get foreign keys from MySQL
    mysql_ddl, indexes, foreign_keys = get_invoiceitem_table_info()
    if mysql_ddl is None:
        return False
    
    return create_invoiceitem_foreign_keys(foreign_keys)

def main():
    parser = argparse.ArgumentParser(description=f'Migrate {TABLE_NAME} table from MySQL to PostgreSQL')
    parser.add_argument('--phase', choices=['1', '2', '3'], help='Migration phase to run')
    parser.add_argument('--full', action='store_true', help='Run all phases')
    parser.add_argument('--verify', action='store_true', help='Verify table structure and data')
    
    args = parser.parse_args()
    
    if args.verify:
        mysql_ddl, indexes, foreign_keys = get_invoiceitem_table_info()
        if mysql_ddl:
            verify_table_structure(TABLE_NAME, PRESERVE_MYSQL_CASE)
        return
    
    if args.full:
        success = (phase1_create_table_and_data() and 
                  phase2_create_indexes() and 
                  phase3_create_foreign_keys())
        if success:
            print("🎉 Operation completed successfully!")
        else:
            print("❌ Operation failed!")
            exit(1)
        return
    
    if args.phase == '1':
        if not phase1_create_table_and_data():
            exit(1)
    elif args.phase == '2':
        if not phase2_create_indexes():
            exit(1)
    elif args.phase == '3':
        if not phase3_create_foreign_keys():
            exit(1)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 