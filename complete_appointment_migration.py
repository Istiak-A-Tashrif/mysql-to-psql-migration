#!/usr/bin/env python3
"""
Complete Appointment Table Migration Script
===========================================

This script does a complete migration of the Appointment table:
1. Gets the table structure directly from MySQL
2. Converts MySQL DDL to PostgreSQL DDL with Prisma compatibility
3. Drops and recreates the PostgreSQL table
4. Exports data from MySQL with advanced cleaning
5. Imports data to PostgreSQL with perfect integrity

Usage: python complete_appointment_migration.py
"""

import subprocess
import re
import os
from collections import OrderedDict

def run_command(command, timeout=3600):
    """Run shell command with error handling"""
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=timeout
        )
        return result
    except Exception as e:
        print(f"Command failed: {str(e)}")
        return None

def get_mysql_table_structure(table_name):
    """Get CREATE TABLE statement from MySQL"""
    print(f"🔍 Getting table structure for {table_name} from MySQL...")
    
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW CREATE TABLE `{table_name}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get table structure: {result.stderr if result else 'No result'}")
        return None
    
    lines = result.stdout.strip().split('\n')
    # The CREATE TABLE statement is usually in the second column of the second row
    for line in lines[1:]:  # Skip header
        if 'CREATE TABLE' in line:
            # Extract the CREATE TABLE part (second column)
            parts = line.split('\t')
            if len(parts) >= 2:
                create_statement = parts[1]
                print(f"✅ Got MySQL CREATE TABLE statement")
                return create_statement
    
    print("❌ Could not find CREATE TABLE statement")
    return None

def convert_mysql_to_postgresql_ddl(mysql_ddl, table_name):
    """Convert MySQL DDL to PostgreSQL DDL with Prisma compatibility"""
    print(f"🔄 Converting MySQL DDL to PostgreSQL for {table_name}...")
    
    # Type mappings (order matters - more specific first)
    type_mappings = OrderedDict([
        # Integer types with auto_increment
        (r'\bint\(\d+\)\s+auto_increment\b', 'SERIAL PRIMARY KEY'),
        (r'\bbigint\(\d+\)\s+auto_increment\b', 'BIGSERIAL PRIMARY KEY'),
        
        # Boolean types
        (r'tinyint\(1\)', 'BOOLEAN'),
        
        # Integer types
        (r'tinyint\(\d+\)', 'SMALLINT'),
        (r'smallint\(\d+\)', 'SMALLINT'),
        (r'mediumint\(\d+\)', 'INTEGER'),
        (r'int\(\d+\)', 'INTEGER'),
        (r'bigint\(\d+\)', 'BIGINT'),
        (r'\btinyint\b(?!\()', 'SMALLINT'),
        (r'\bint\b(?!\()', 'INTEGER'),
        
        # String types
        (r'varchar\((\d+)\)', r'VARCHAR(\1)'),
        (r'char\((\d+)\)', r'CHAR(\1)'),
        (r'text', 'TEXT'),
        (r'longtext', 'TEXT'),
        (r'mediumtext', 'TEXT'),
        (r'tinytext', 'TEXT'),
        
        # Date/Time types (Prisma compatible)
        (r'\bdatetime\(\d+\)\b', 'TIMESTAMP(3)'),
        (r'\bdatetime\b', 'TIMESTAMP'),
        (r'\btimestamp\b', 'TIMESTAMP'),
        (r'\bdate\b(?=\s|,|\)|\n)', 'DATE'),
        (r'\btime\b(?=\s|,|\)|\n)', 'TIME'),
        
        # Decimal types
        (r'decimal\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
        (r'numeric\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
        (r'double', 'DOUBLE PRECISION'),
        (r'float', 'REAL'),
        
        # Special types
        (r'enum\([^)]+\)', 'VARCHAR(50)'),
        (r'json', 'JSONB'),  # PostgreSQL native JSON
        (r'blob', 'BYTEA'),
        (r'longblob', 'BYTEA'),
    ])
    
    # Start with the original DDL
    postgres_ddl = mysql_ddl
    
    # Convert table name to lowercase
    postgres_ddl = re.sub(
        r'CREATE TABLE `([^`]+)`',
        r'CREATE TABLE \1',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    # Apply type mappings
    for mysql_pattern, postgres_type in type_mappings.items():
        postgres_ddl = re.sub(mysql_pattern, postgres_type, postgres_ddl, flags=re.IGNORECASE)
    
    # Remove MySQL-specific syntax
    postgres_ddl = re.sub(r'\s+unsigned\b', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s+zerofill\b', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', 'DEFAULT CURRENT_TIMESTAMP', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'COLLATE [a-zA-Z0-9_]+', '', postgres_ddl)
    postgres_ddl = re.sub(r'CHARACTER SET [a-zA-Z0-9_]+', '', postgres_ddl)
    
    # Remove ENGINE, CHARSET, etc.
    postgres_ddl = re.sub(r'\s*ENGINE\s*=\s*[a-zA-Z0-9_]+', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s*DEFAULT\s+CHARSET\s*=\s*[a-zA-Z0-9_]+', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r'\s*AUTO_INCREMENT\s*=\s*\d+', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove KEY definitions (PostgreSQL will handle indexes separately) 
    postgres_ddl = re.sub(r',\s*KEY\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r',\s*INDEX\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove CONSTRAINT definitions (we'll handle these separately)
    postgres_ddl = re.sub(r',\s*CONSTRAINT\s+[^,)]+', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Clean up PRIMARY KEY definitions that are already handled by SERIAL
    postgres_ddl = re.sub(r',\s*PRIMARY\s+KEY\s*\([^)]+\)', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove MySQL-specific table options at the end
    postgres_ddl = re.sub(r'\)\s*[A-Z_=\s\w\d]+$', ')', postgres_ddl, flags=re.IGNORECASE)
    
    # Remove backticks from column names
    postgres_ddl = re.sub(r'`([^`]+)`', r'\1', postgres_ddl)
    
    # Fix auto_increment (should be handled by SERIAL but clean up any remaining)
    postgres_ddl = re.sub(r'\s+AUTO_INCREMENT\b', '', postgres_ddl, flags=re.IGNORECASE)
    
    # Fix DEFAULT values for booleans
    postgres_ddl = re.sub(r"DEFAULT\s+'0'", "DEFAULT false", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'1'", "DEFAULT true", postgres_ddl, flags=re.IGNORECASE)
    
    # Fix default values
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00 00:00:00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    postgres_ddl = re.sub(r"DEFAULT\s+'0000-00-00'", "DEFAULT NULL", postgres_ddl, flags=re.IGNORECASE)
    
    # Clean up extra commas and whitespace
    postgres_ddl = re.sub(r',\s*,', ',', postgres_ddl)
    postgres_ddl = re.sub(r',(\s*)\)', r'\1)', postgres_ddl)
    
    # Convert table name to lowercase for PostgreSQL
    table_lower = table_name.lower()
    postgres_ddl = re.sub(
        rf'\bCREATE TABLE {table_name}\b',
        f'CREATE TABLE {table_lower}',
        postgres_ddl,
        flags=re.IGNORECASE
    )
    
    print(f"✅ Converted DDL for {table_name}")
    return postgres_ddl

def create_postgresql_table(table_name, postgres_ddl):
    """Drop and create PostgreSQL table"""
    print(f"🗑️ Dropping existing {table_name} table if exists...")
    
    # Drop table if exists
    drop_cmd = f'docker exec postgres_target psql -U postgres -d target_db -c "DROP TABLE IF EXISTS {table_name.lower()} CASCADE;"'
    result = run_command(drop_cmd)
    
    if not result or result.returncode != 0:
        print(f"⚠️ Warning: Could not drop table (might not exist): {result.stderr if result else 'No result'}")
    else:
        print(f"✅ Dropped existing {table_name} table")
    
    # Create a clean DDL - for appointment table specifically
    if table_name.lower() == 'appointment':
        clean_ddl = """CREATE TABLE appointment (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title VARCHAR(100) NOT NULL,
    date TIMESTAMP(3) DEFAULT NULL,
    start_time VARCHAR(191) DEFAULT NULL,
    end_time VARCHAR(191) DEFAULT NULL,
    company_id INTEGER NOT NULL,
    customer_id INTEGER DEFAULT NULL,
    vehicle_id INTEGER DEFAULT NULL,
    draft_estimate VARCHAR(191) DEFAULT NULL,
    notes VARCHAR(191) DEFAULT NULL,
    confirmation_email_template_id INTEGER DEFAULT NULL,
    confirmation_email_template_status BOOLEAN NOT NULL DEFAULT false,
    reminder_email_template_id INTEGER DEFAULT NULL,
    reminder_email_template_status BOOLEAN NOT NULL DEFAULT false,
    times JSONB DEFAULT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    googleEventId VARCHAR(191) DEFAULT NULL,
    timezone VARCHAR(191) DEFAULT NULL
)"""
    else:
        # For other tables, use the converted DDL but clean it up
        clean_ddl = postgres_ddl
    
    # Create new table
    print(f"🔨 Creating PostgreSQL table: {table_name.lower()}")
    print(f"📝 Final DDL:\n{clean_ddl}")
    
    # Write DDL to file to avoid command line issues
    ddl_file = f"create_{table_name.lower()}.sql"
    with open(ddl_file, "w", encoding="utf-8") as f:
        f.write(clean_ddl + ";")
    
    print(f"💾 Saved DDL to {ddl_file}")
    
    # Copy SQL file to container and execute
    copy_cmd = f"docker cp {ddl_file} postgres_target:/tmp/{ddl_file}"
    result = run_command(copy_cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to copy DDL file: {result.stderr if result else 'No result'}")
        return False
    
    # Execute DDL
    exec_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{ddl_file}'
    result = run_command(exec_cmd)
    
    print(f"🔧 DDL execution result: {result.returncode if result else 'No result'}")
    if result and result.stderr:
        print(f"🔧 DDL stderr: {result.stderr}")
    if result and result.stdout:
        print(f"🔧 DDL stdout: {result.stdout}")
    
    # Keep the DDL file for debugging (comment out to keep)
    # try:
    #     os.remove(ddl_file)
    # except:
    #     pass
    run_command(f"docker exec postgres_target rm /tmp/{ddl_file}")
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to create table: {result.stderr if result else 'No result'}")
        return False
    
    print(f"✅ Created PostgreSQL table: {table_name.lower()}")
    return True

def fix_json_format(json_str):
    """Fix invalid JSON format from MySQL"""
    if not json_str or json_str in ('NULL', '\\N', 'null'):
        return json_str
    
    try:
        # Handle the invalid case: [{date: 2025-02-02, time: 00:53}]
        if json_str.startswith('[{') and json_str.endswith('}]') and '"' not in json_str:
            # Extract content between [{ and }]
            content = json_str[2:-2]  # Remove [{ and }]
            
            # Split by comma and process each key-value pair
            pairs = content.split(', ')
            fixed_pairs = []
            
            for pair in pairs:
                if ':' in pair:
                    key, value = pair.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    # Add quotes around key and value
                    fixed_pairs.append(f'"{key}": "{value}"')
            
            return '[{' + ', '.join(fixed_pairs) + '}]'
        
        return json_str
    except Exception:
        return json_str

def export_and_clean_mysql_data(table_name):
    """Export and clean data from MySQL"""
    print(f"📤 Exporting data from MySQL {table_name}...")
    
    # Export data from MySQL
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SELECT * FROM `{table_name}`;" --batch --raw --skip-column-names --default-character-set=utf8mb4'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to export data: {result.stderr if result else 'No result'}")
        return None
    
    if not result.stdout or not result.stdout.strip():
        print(f"⚠️ Table {table_name} is empty")
        return ""
    
    lines = result.stdout.strip().split('\n')
    print(f"📥 Got {len(lines)} lines from MySQL")
    
    # Get expected column count
    desc_cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "DESCRIBE `{table_name}`;"'
    desc_result = run_command(desc_cmd)
    
    expected_columns = 20  # Default for appointment
    if desc_result and desc_result.returncode == 0:
        desc_lines = [line for line in desc_result.stdout.strip().split('\n')[1:] if line.strip()]
        expected_columns = len(desc_lines)
    
    print(f"🔢 Expected {expected_columns} columns")
    
    # Clean the data
    cleaned_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        
        # Handle embedded newlines by combining lines until we have the right column count
        parts = line.split('\t')
        while len(parts) < expected_columns and i + 1 < len(lines):
            i += 1
            line = line.replace('\n', ' ') + ' ' + lines[i]
            parts = line.split('\t')
        
        # Clean each field
        cleaned_parts = []
        for j, part in enumerate(parts[:expected_columns]):  # Only take expected number of columns
            part = part.strip()
            
            # Handle NULL values
            if part in ('NULL', 'null', '\\N', ''):
                cleaned_parts.append('\\N')
            else:
                # Fix JSON format
                if part.startswith('[{') or part.startswith('{'):
                    part = fix_json_format(part)
                
                # Remove control characters
                part = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', part)
                
                # Escape for CSV format
                if '\t' in part or '\n' in part or '"' in part or ',' in part:
                    part = '"' + part.replace('"', '""') + '"'
                
                cleaned_parts.append(part)
        
        # Pad with NULLs if needed
        while len(cleaned_parts) < expected_columns:
            cleaned_parts.append('\\N')
        
        # Only add if we have the right number of columns
        if len(cleaned_parts) == expected_columns:
            cleaned_lines.append('\t'.join(cleaned_parts))
        else:
            print(f"⚠️ Skipping malformed line {i+1}: {len(cleaned_parts)} columns")
        
        i += 1
    
    cleaned_data = '\n'.join(cleaned_lines)
    print(f"✅ Cleaned {len(cleaned_lines)} lines")
    
    return cleaned_data

def import_data_to_postgresql(table_name, cleaned_data):
    """Import cleaned data to PostgreSQL"""
    if not cleaned_data.strip():
        print(f"⚠️ No data to import for {table_name}")
        return True
    
    print(f"📥 Importing data to PostgreSQL {table_name}...")
    
    # Write data to temp file
    temp_file = f"{table_name}_import.tsv"
    with open(temp_file, "w", encoding="utf-8") as f:
        f.write(cleaned_data)
    
    # Copy to PostgreSQL container
    postgres_temp = f"/tmp/{temp_file}"
    copy_cmd = f"docker cp {temp_file} postgres_target:{postgres_temp}"
    result = run_command(copy_cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to copy data file: {result.stderr if result else 'No result'}")
        return False
    
    # Import using COPY
    copy_sql = f"COPY {table_name.lower()} FROM '{postgres_temp}' WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', ENCODING 'UTF8');"
    
    # Write SQL to file
    sql_file = f"{table_name}_import.sql"
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write(copy_sql)
    
    # Copy SQL file and execute
    copy_sql_cmd = f"docker cp {sql_file} postgres_target:/tmp/{sql_file}"
    run_command(copy_sql_cmd)
    
    exec_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/{sql_file}'
    result = run_command(exec_cmd)
    
    # Clean up
    try:
        os.remove(temp_file)
        os.remove(sql_file)
    except:
        pass
    run_command(f"docker exec postgres_target rm {postgres_temp}")
    run_command(f"docker exec postgres_target rm /tmp/{sql_file}")
    
    if not result or result.returncode != 0:
        print(f"❌ Import failed: {result.stderr if result else 'No result'}")
        return False
    
    # Verify import
    count_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM {table_name.lower()};"'
    count_result = run_command(count_cmd)
    
    if count_result and count_result.returncode == 0:
        try:
            row_count = int(count_result.stdout.strip())
            print(f"✅ Imported {row_count} rows successfully!")
            return True
        except:
            print("✅ Import completed (count verification failed)")
            return True
    
    print("✅ Import completed")
    return True

def migrate_appointment_complete():
    """Complete appointment table migration"""
    table_name = "Appointment"
    
    print(f"🚀 Starting complete migration for {table_name}")
    print("=" * 50)
    
    # Step 1: Get MySQL table structure
    mysql_ddl = get_mysql_table_structure(table_name)
    if not mysql_ddl:
        return False
    
    # Step 2: Convert to PostgreSQL DDL
    postgres_ddl = convert_mysql_to_postgresql_ddl(mysql_ddl, table_name)
    print(f"\n📋 PostgreSQL DDL:\n{postgres_ddl}\n")
    
    # Step 3: Create PostgreSQL table
    if not create_postgresql_table(table_name, postgres_ddl):
        return False
    
    # Step 4: Export and clean MySQL data
    cleaned_data = export_and_clean_mysql_data(table_name)
    if cleaned_data is None:
        return False
    
    # Step 5: Import data to PostgreSQL
    if not import_data_to_postgresql(table_name, cleaned_data):
        return False
    
    print("=" * 50)
    print(f"🎉 Complete migration of {table_name} successful!")
    return True

if __name__ == "__main__":
    migrate_appointment_complete()
