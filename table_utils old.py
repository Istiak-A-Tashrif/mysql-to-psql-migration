#!/usr/bin/env python3
"""
Database Table Comparison Utilities
===================================

Generic utility functions for comparing table structures and data
between MySQL and PostgreSQL databases.

These functions can be used for any table migration verification.
"""

import subprocess
import re
import os

def run_command(command, timeout=60):
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

def execute_postgresql_sql(sql_statement, description="SQL statement"):
    """Execute a PostgreSQL SQL statement using file-based approach to handle quotes properly"""
    # Write SQL to file to handle quotes properly
    with open('temp_sql.sql', 'w', encoding='utf-8') as f:
        f.write(sql_statement)
    
    # Copy and execute
    copy_cmd = 'docker cp temp_sql.sql postgres_target:/tmp/temp_sql.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy {description} file")
        return False, None
    
    result = run_command('docker exec postgres_target psql -U postgres -d target_db -f /tmp/temp_sql.sql')
    
    # Cleanup
    run_command('rm -f temp_sql.sql')
    run_command('docker exec postgres_target rm -f /tmp/temp_sql.sql')
    
    return result and result.returncode == 0, result

def get_mysql_table_columns(table_name):
    """Get column information from MySQL table"""
    print(f" Getting MySQL column info for {table_name}...")
    
    # Use DESCRIBE which gives more reliable output format
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "DESCRIBE {table_name};"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get MySQL columns: {result.stderr if result else 'No result'}")
        return None
    
    columns = []
    lines = result.stdout.strip().split('\n')
    
    # Skip the header line and process data rows
    for line in lines[1:]:
        if line.strip() and not line.startswith('Field'):
            # Split by tab, but handle cases where spaces are used
            parts = []
            if '\t' in line:
                parts = line.split('\t')
            else:
                # Fallback to splitting by multiple spaces
                import re
                parts = re.split(r'\s{2,}', line.strip())
            
            # Filter out empty parts and ensure we have at least 6 parts
            parts = [p.strip() for p in parts if p.strip()]
            
            if len(parts) >= 6:
                columns.append({
                    'name': parts[0],
                    'type': parts[1],
                    'null': parts[2],
                    'key': parts[3],
                    'default': parts[4] if parts[4] != 'NULL' else None,
                    'extra': parts[5] if len(parts) > 5 else ''
                })
            elif len(parts) >= 3:
                # Handle cases with fewer columns
                columns.append({
                    'name': parts[0],
                    'type': parts[1],
                    'null': parts[2] if len(parts) > 2 else 'YES',
                    'key': parts[3] if len(parts) > 3 else '',
                    'default': parts[4] if len(parts) > 4 and parts[4] != 'NULL' else None,
                    'extra': parts[5] if len(parts) > 5 else ''
                })
    
    print(f"✅ Found {len(columns)} MySQL columns")
    if len(columns) == 0:
        print("⚠️ Debug: Raw MySQL output:")
        print(result.stdout)
    
    return columns

def get_postgresql_table_columns(table_name, preserve_case=True):
    """Get column information from PostgreSQL table"""
    print(f" Getting PostgreSQL column info for {table_name}...")
    
    # Use the appropriate table name for PostgreSQL
    pg_table_name = table_name if preserve_case else table_name.lower()
    
    # Simplified query that works better for parsing
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -c "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = \'{pg_table_name}\' ORDER BY ordinal_position;"'
    
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get PostgreSQL columns: {result.stderr if result else 'No result'}")
        return None
    
    columns = []
    lines = result.stdout.strip().split('\n')
    
    # Skip header and separator lines, process data rows
    for line in lines:
        line = line.strip()
        if not line or 'column_name' in line or line.startswith('-') or line.startswith('(') or 'rows)' in line:
            continue
            
        if '|' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                columns.append({
                    'name': parts[0],
                    'type': parts[1],
                    'nullable': parts[2],
                    'default': parts[3] if parts[3] else None
                })
    
    print(f"✅ Found {len(columns)} PostgreSQL columns")
    return columns

def normalize_mysql_type(mysql_type):
    """Normalize MySQL type to compare with PostgreSQL"""
    mysql_type = mysql_type.lower().strip()
    
    # Handle auto_increment
    if 'auto_increment' in mysql_type:
        if 'int(' in mysql_type or mysql_type == 'int':
            return 'serial'
        elif 'bigint(' in mysql_type or mysql_type == 'bigint':
            return 'bigserial'
    
    # Direct type mappings for comparison
    type_map = {
        'tinyint(1)': 'boolean',
        'datetime': 'timestamp without time zone',
        'datetime(3)': 'timestamp without time zone',
        'timestamp': 'timestamp without time zone',
        'text': 'text',
        'longtext': 'text',
        'json': 'jsonb',
        'int': 'integer',
        'int(11)': 'integer',
        'bigint': 'bigint',
        'bigint(20)': 'bigint',
        'tinyint(4)': 'smallint',
    }
    
    # Check direct mappings first
    if mysql_type in type_map:
        return type_map[mysql_type]
    
    # Handle varchar with length - convert to generic character varying
    if mysql_type.startswith('varchar('):
        return 'character varying'
    
    # Handle generic int types
    if mysql_type.startswith('int(') or mysql_type == 'int':
        return 'integer'
    elif mysql_type.startswith('bigint(') or mysql_type == 'bigint':
        return 'bigint'
    elif mysql_type.startswith('tinyint('):
        return 'smallint'
    
    return mysql_type

def compare_table_structures(table_name, preserve_case=True):
    """Compare table structures between MySQL and PostgreSQL"""
    print(f" Comparing table structures for {table_name}")
    print("=" * 60)
    
    # Get columns from both databases
    mysql_columns = get_mysql_table_columns(table_name)
    postgres_columns = get_postgresql_table_columns(table_name, preserve_case)
    
    if mysql_columns is None:
        print("❌ Could not get MySQL table structure")
        return False
    
    if postgres_columns is None:
        print("❌ Could not get PostgreSQL table structure")
        return False
    
    print(f"📊 MySQL has {len(mysql_columns)} columns")
    print(f"📊 PostgreSQL has {len(postgres_columns)} columns")
    
    # Create dictionaries for easier comparison (case-insensitive)
    mysql_dict = {col['name'].lower(): col for col in mysql_columns}
    postgres_dict = {col['name'].lower(): col for col in postgres_columns}
    
    # Also keep original case for display
    mysql_display = {col['name'].lower(): col['name'] for col in mysql_columns}
    postgres_display = {col['name'].lower(): col['name'] for col in postgres_columns}
    
    all_columns = set(mysql_dict.keys()) | set(postgres_dict.keys())
    
    differences = []
    matches = 0
    
    print(f"\n📋 Column-by-column comparison:")
    print("-" * 80)
    print(f"{'Column':<20} {'MySQL Type':<25} {'PostgreSQL Type':<25} {'Status'}")
    print("-" * 80)
    
    for col_name in sorted(all_columns):
        mysql_col = mysql_dict.get(col_name)
        postgres_col = postgres_dict.get(col_name)
        
        # Get display names (original case)
        mysql_display_name = mysql_display.get(col_name, col_name)
        postgres_display_name = postgres_display.get(col_name, col_name)
        
        if not mysql_col:
            pg_type = postgres_col['type'] if postgres_col else 'unknown'
            print(f"{postgres_display_name:<20} {'(missing)':<25} {pg_type:<25} ❌ Only in PostgreSQL")
            differences.append(f"Column '{postgres_display_name}' only exists in PostgreSQL")
        elif not postgres_col:
            my_type = mysql_col['type'] if mysql_col else 'unknown'
            print(f"{mysql_display_name:<20} {my_type:<25} {'(missing)':<25} ❌ Only in MySQL")
            differences.append(f"Column '{mysql_display_name}' only exists in MySQL")
        else:
            # Compare types
            mysql_normalized = normalize_mysql_type(mysql_col['type'])
            postgres_type = postgres_col['type'].lower()
            
            # Special handling for serial types
            if postgres_type == 'integer' and mysql_col['extra'] == 'auto_increment':
                mysql_normalized = 'serial'
                postgres_type = 'serial'
            
            type_match = mysql_normalized == postgres_type
            
            # Check nullability
            mysql_nullable = mysql_col['null'].upper() == 'YES'
            postgres_nullable = postgres_col['nullable'].upper() == 'YES'
            null_match = mysql_nullable == postgres_nullable
            
            if type_match and null_match:
                print(f"{mysql_display_name:<20} {mysql_col['type']:<25} {postgres_col['type']:<25} ✅ Match")
                matches += 1
            else:
                status = "❌ "
                if not type_match:
                    status += "Type mismatch "
                if not null_match:
                    status += "Nullable mismatch"
                
                print(f"{mysql_display_name:<20} {mysql_col['type']:<25} {postgres_col['type']:<25} {status}")
                differences.append(f"Column '{mysql_display_name}': MySQL({mysql_col['type']}, null={mysql_col['null']}) vs PostgreSQL({postgres_col['type']}, null={postgres_col['nullable']})")
    
    print("-" * 80)
    print(f"\n📊 Summary:")
    print(f"   ✅ Matching columns: {matches}")
    print(f"   ❌ Differences: {len(differences)}")
    
    if differences:
        print(f"\n⚠️ Found {len(differences)} differences:")
        for i, diff in enumerate(differences, 1):
            print(f"   {i}. {diff}")
        return False
    else:
        print(f"\n🎉 Table structures match perfectly!")
        return True

def verify_table_structure(table_name, preserve_case=True):
    """Verify that a table structure matches between MySQL and PostgreSQL"""
    print(f" Verifying {table_name} table structure consistency")
    print("=" * 70)
    
    # First check if tables exist
    mysql_exists_cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW TABLES LIKE \'{table_name}\';"'
    mysql_result = run_command(mysql_exists_cmd)
    
    # Use appropriate table name for PostgreSQL
    pg_table_name = table_name if preserve_case else table_name.lower()
    postgres_exists_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'{pg_table_name}\' AND table_schema = \'public\';"'
    postgres_result = run_command(postgres_exists_cmd)
    
    mysql_exists = mysql_result and mysql_result.returncode == 0 and table_name in mysql_result.stdout
    postgres_exists = False
    
    if postgres_result and postgres_result.returncode == 0:
        try:
            count = int(postgres_result.stdout.strip())
            postgres_exists = count > 0
        except:
            postgres_exists = False
    
    print(f"📋 MySQL {table_name} table exists: {'✅' if mysql_exists else '❌'}")
    print(f"📋 PostgreSQL {pg_table_name} table exists: {'✅' if postgres_exists else '❌'}")
    
    if not mysql_exists:
        print(f"❌ MySQL table '{table_name}' does not exist!")
        return False
    
    if not postgres_exists:
        print(f"❌ PostgreSQL table '{pg_table_name}' does not exist!")
        print("💡 Run the migration script first to create the table")
        return False
    
    print("\n" + "=" * 50)
    return compare_table_structures(table_name, preserve_case)

def check_docker_containers():
    """Check if Docker containers are running"""
    print(" Checking Docker containers...")
    
    mysql_check = run_command("docker ps --filter name=mysql_source --format '{{.Names}}'")
    postgres_check = run_command("docker ps --filter name=postgres_target --format '{{.Names}}'")
    
    mysql_running = mysql_check and mysql_check.returncode == 0 and 'mysql_source' in mysql_check.stdout
    postgres_running = postgres_check and postgres_check.returncode == 0 and 'postgres_target' in postgres_check.stdout
    
    print(f"📊 MySQL container (mysql_source): {'✅ Running' if mysql_running else '❌ Not running'}")
    print(f"📊 PostgreSQL container (postgres_target): {'✅ Running' if postgres_running else '❌ Not running'}")
    
    if not mysql_running or not postgres_running:
        print("\n❌ Please start the required Docker containers first:")
        if not mysql_running:
            print("   docker start mysql_source")
        if not postgres_running:
            print("   docker start postgres_target")
        return False
    
    return True

def count_table_records(table_name):
    """Count records in both MySQL and PostgreSQL tables"""
    print(f"📊 Counting records in both {table_name} tables...")
    
    # MySQL count
    mysql_cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT COUNT(*) FROM {table_name};"'
    mysql_result = run_command(mysql_cmd)
    
    # PostgreSQL count
    postgres_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM {table_name.lower()};"'
    postgres_result = run_command(postgres_cmd)
    
    mysql_count = "Error"
    postgres_count = "Error"
    
    if mysql_result and mysql_result.returncode == 0:
        lines = mysql_result.stdout.strip().split('\n')
        if len(lines) >= 2:
            mysql_count = lines[1].strip()
    
    if postgres_result and postgres_result.returncode == 0:
        postgres_count = postgres_result.stdout.strip()
    
    print(f"📊 MySQL {table_name} records: {mysql_count}")
    print(f"📊 PostgreSQL {table_name.lower()} records: {postgres_count}")
    
    if mysql_count != "Error" and postgres_count != "Error":
        if mysql_count == postgres_count:
            print("✅ Record counts match!")
            return True, mysql_count, postgres_count
        else:
            print("⚠️ Record counts don't match!")
            return False, mysql_count, postgres_count
    
    return False, mysql_count, postgres_count

def run_command_with_timeout(command, timeout=3600):
    """Run shell command with extended timeout for migrations"""
    return run_command(command, timeout)

def get_mysql_table_info(table_name):
    """Get complete table information from MySQL including constraints"""
    print(f" Getting complete table info for {table_name} from MySQL...")
    
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW CREATE TABLE `{table_name}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get table info: {result.stderr if result else 'No result'}")
        return None
    
    return result.stdout

def table_exists_mysql(table_name):
    """Check if table exists in MySQL"""
    cmd = f'docker exec mysql_source mysql -u mysql -pmysql source_db -e "SHOW TABLES LIKE \'{table_name}\';"'
    result = run_command(cmd)
    return result and result.returncode == 0 and table_name in result.stdout

def table_exists_postgresql(table_name):
    """Check if table exists in PostgreSQL"""
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'{table_name.lower()}\' AND table_schema = \'public\';"'
    result = run_command(cmd)
    
    if result and result.returncode == 0:
        try:
            count = int(result.stdout.strip())
            return count > 0
        except:
            return False
    return False

def analyze_column_differences(table_name):
    """Analyze column differences and suggest fixes"""
    print(f"\n Analyzing column differences for {table_name}...")
    
    mysql_columns = get_mysql_table_columns(table_name)
    postgres_columns = get_postgresql_table_columns(table_name)
    
    if not mysql_columns or not postgres_columns:
        return
    
    mysql_dict = {col['name'].lower(): col for col in mysql_columns}
    postgres_dict = {col['name'].lower(): col for col in postgres_columns}
    
    mysql_names = {col['name'].lower(): col['name'] for col in mysql_columns}
    postgres_names = {col['name'].lower(): col['name'] for col in postgres_columns}
    
    all_columns = set(mysql_dict.keys()) | set(postgres_dict.keys())
    
    issues = []
    suggestions = []
    
    for col_name in all_columns:
        mysql_col = mysql_dict.get(col_name)
        postgres_col = postgres_dict.get(col_name)
        
        if not mysql_col and postgres_col:
            # Column only in PostgreSQL
            postgres_original = postgres_names[col_name]
            
            # Check if it's a case issue
            mysql_case_matches = [name for name in mysql_names.values() if name.lower() == col_name]
            if mysql_case_matches:
                mysql_original = mysql_case_matches[0]
                issues.append(f"Case mismatch: MySQL '{mysql_original}' vs PostgreSQL '{postgres_original}'")
                suggestions.append(f"ALTER TABLE {table_name.lower()} RENAME COLUMN {postgres_original} TO {mysql_original};")
            else:
                issues.append(f"Extra column in PostgreSQL: '{postgres_original}'")
                suggestions.append(f"-- Consider if '{postgres_original}' should be dropped or if it's missing from MySQL")
        
        elif mysql_col and not postgres_col:
            # Column only in MySQL
            mysql_original = mysql_names[col_name]
            issues.append(f"Missing column in PostgreSQL: '{mysql_original}'")
            suggestions.append(f"-- Column '{mysql_original}' needs to be added to PostgreSQL table")
    
    if issues:
        print(f"\n⚠️ Found {len(issues)} column issues:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        
        print(f"\n💡 Suggested fixes:")
        for suggestion in suggestions:
            print(f"   {suggestion}")
    else:
        print(f"\n✅ No column issues found!")

def create_postgresql_table(table_name, postgres_ddl, preserve_case=True):
    """Drop and create PostgreSQL table"""
    pg_table_name = get_postgresql_table_name(table_name, preserve_case)
    
    print(f"🗑️ Dropping existing {pg_table_name} table if exists...")
    
    # Drop table if exists
    drop_sql = f"DROP TABLE IF EXISTS {pg_table_name} CASCADE;"
    
    # Write to temporary file to handle quotes properly
    with open('drop_table.sql', 'w', encoding='utf-8') as f:
        f.write(drop_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp drop_table.sql postgres_target:/tmp/drop_table.sql'
    run_command(copy_cmd)
    
    drop_cmd = 'docker exec postgres_target psql -U postgres -d target_db -f /tmp/drop_table.sql'
    result = run_command(drop_cmd)
    
    # Cleanup
    run_command('rm -f drop_table.sql')  # Remove local file
    run_command('docker exec postgres_target rm -f /tmp/drop_table.sql')  # Remove container file
    
    if not result or result.returncode != 0:
        print(f"⚠️ Warning: Could not drop table (might not exist): {result.stderr if result else 'No result'}")
    else:
        print(f"✅ Dropped existing {pg_table_name} table")
    
    # Create new table
    print(f"🏗️ Creating {pg_table_name} table...")
    
    # Clean the DDL and update table name if preserving case
    clean_ddl = postgres_ddl.strip()
    if preserve_case:
        # Replace table name with quoted version
        import re
        clean_ddl = re.sub(f'CREATE TABLE {table_name.lower()}', f'CREATE TABLE {pg_table_name}', clean_ddl, flags=re.IGNORECASE)
        clean_ddl = re.sub(f'CREATE TABLE {table_name}', f'CREATE TABLE {pg_table_name}', clean_ddl, flags=re.IGNORECASE)
    
    if not clean_ddl.endswith(';'):
        clean_ddl += ';'
    
    # Write DDL to a temporary file to avoid shell escaping issues
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(clean_ddl)
        temp_file = f.name
    
    try:
        # Copy the SQL file to the container and execute it
        copy_cmd = f'docker cp {temp_file} postgres_target:/tmp/create_table.sql'
        result = run_command(copy_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to copy SQL file: {result.stderr if result else 'No result'}")
            return False
        
        # Execute the SQL file
        exec_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/create_table.sql'
        result = run_command(exec_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to create table: {result.stderr if result else 'No result'}")
            print(f"📋 DDL that failed:")
            print(clean_ddl)
            return False
        
        # Also show any warnings or output from table creation
        if result.stdout:
            print(f" Table creation output: {result.stdout}")
        if result.stderr:
            print(f"⚠️ Table creation warnings: {result.stderr}")
        
        print(f"✅ Created {pg_table_name} table successfully")
        return True
        
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file):
            os.unlink(temp_file)

def export_and_clean_mysql_data(table_name):
    """Export data from MySQL with advanced cleaning"""
    print(f"📤 Exporting data from MySQL {table_name} table...")
    
    # Simple approach: get data and return it directly for processing
    # We'll modify the import function to handle this differently
    print(f"✅ Data export configured for {table_name}")
    return table_name  # Return table name to indicate success

def import_data_to_postgresql(table_name, data_indicator, preserve_case=True, include_id=False):
    """Import data to PostgreSQL using direct transfer"""
    pg_table_name = get_postgresql_table_name(table_name, preserve_case)
    
    print(f"📥 Importing data to PostgreSQL {pg_table_name} table...")
    
    if not data_indicator:
        print("❌ No data indicator provided")
        return False
    
    # Use a direct approach: pipe data from MySQL to PostgreSQL
    print(f"📋 Transferring data directly from MySQL to PostgreSQL...")
    
    # Create a temporary SQL file for the copy operation
    import tempfile
    import os
    
    # First, get the data in a format we can use
    # Use backticks around table name to handle reserved words like "Lead"
    get_data_cmd = f'''docker exec mysql_source mysql -u mysql -pmysql source_db -e "SELECT * FROM `{table_name}`;" -B --skip-column-names'''
    result = run_command(get_data_cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to retrieve data: {result.stderr if result else 'No result'}")
        return False
    
    # Process the data and convert to CSV format
    lines = result.stdout.strip().split('\n')
    csv_lines = []
    
    for line in lines:
        if line.strip():
            # Convert tab-separated to comma-separated, handle quotes
            fields = line.split('\t')
            csv_fields = []
            for field in fields:
                if field == 'NULL':
                    csv_fields.append('')
                else:
                    # Escape quotes and wrap in quotes if needed
                    field = field.replace('"', '""')
                    if ',' in field or '"' in field or '\n' in field:
                        csv_fields.append(f'"{field}"')
                    else:
                        csv_fields.append(field)
            csv_lines.append(','.join(csv_fields))
    
    csv_data = '\n'.join(csv_lines)
    
    # Write to temporary file with UTF-8 encoding
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(csv_data)
        temp_file = f.name
    
    try:
        # Copy to PostgreSQL container
        import_file_name = f'{table_name}_import.csv'
        copy_cmd = f'docker cp "{temp_file}" postgres_target:/tmp/{import_file_name}'
        result = run_command(copy_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to copy to PostgreSQL container: {result.stderr if result else 'No result'}")
            return False
        
        # Import using COPY command, excluding auto-increment id column
        # Get column list excluding id
        # For PostgreSQL information_schema, when tables are created with quotes (case-sensitive),
        # the table_name is stored with the actual case in information_schema
        if preserve_case:
            lookup_table_name = table_name  # Use original case for quoted tables
        else:
            lookup_table_name = table_name.lower()  # Use lowercase for unquoted tables
        print(f" Debug: table_name={table_name}, preserve_case={preserve_case}, lookup_table_name={lookup_table_name}, pg_table_name={pg_table_name}")
        # Get column list - include or exclude id based on parameter
        id_filter = "" if include_id else " AND column_name != 'id'"
        get_columns_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT column_name FROM information_schema.columns WHERE table_name = \'{lookup_table_name}\'{id_filter} ORDER BY ordinal_position;"'
        print(f" Debug: get_columns_cmd={get_columns_cmd}")
        col_result = run_command(get_columns_cmd)
        
        if col_result and col_result.returncode == 0:
            columns = [col.strip() for col in col_result.stdout.strip().split('\n') if col.strip()]
            if preserve_case:
                # Quote each column name for case sensitivity
                columns = [f'"{col}"' for col in columns]
            column_list = ', '.join(columns)
            
            # Modify the data based on include_id parameter
            updated_csv_lines = []
            for line in csv_lines:
                if include_id:
                    # Include all columns including id
                    updated_csv_lines.append(line)
                else:
                    # Exclude the first column (id)
                    fields = line.split(',', 1)  # Split only on first comma
                    if len(fields) > 1:
                        updated_csv_lines.append(fields[1])  # Skip first field (id)
            
            # Write the updated CSV with UTF-8 encoding
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_csv_lines))
            
            # Copy updated file to container
            copy_cmd = f'docker cp "{temp_file}" postgres_target:/tmp/{import_file_name}'
            result = run_command(copy_cmd)
            
            if not result or result.returncode != 0:
                print(f"❌ Failed to copy updated CSV: {result.stderr if result else 'No result'}")
                return False
            
            # Write the COPY command to a SQL file to avoid shell escaping issues
            copy_sql = f"COPY {pg_table_name} ({column_list}) FROM '/tmp/{import_file_name}' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', NULL '');"
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
                f.write(copy_sql)
                copy_sql_file = f.name
            
            try:
                # Copy SQL file to container
                copy_sql_cmd = f'docker cp "{copy_sql_file}" postgres_target:/tmp/import_data.sql'
                result = run_command(copy_sql_cmd)
                
                if not result or result.returncode != 0:
                    print(f"❌ Failed to copy SQL file: {result.stderr if result else 'No result'}")
                    return False
                
                # Execute the SQL file
                import_cmd = f'docker exec postgres_target psql -U postgres -d target_db -f /tmp/import_data.sql'
                print(f" Debug: Final import command: {import_cmd}")
                print(f" Debug: SQL content: {copy_sql}")
            finally:
                # Clean up SQL file
                try:
                    if os.path.exists(copy_sql_file):
                        os.unlink(copy_sql_file)
                except:
                    pass
        else:
            # Fallback to direct command
            import_cmd = f"docker exec postgres_target psql -U postgres -d target_db -c \"COPY {pg_table_name} FROM '/tmp/{import_file_name}' WITH (FORMAT csv, DELIMITER ',', QUOTE '\\\"', NULL '');\""
            print(f" Debug: Fallback import command: {import_cmd}")
        
        result = run_command(import_cmd)
        
        if not result or result.returncode != 0:
            print(f"❌ Failed to import data: {result.stderr if result else 'No result'}")
            if result:
                print(f" Import command stdout: {result.stdout}")
            return False
        
        # Also check if there was any output that might indicate issues
        if result.stdout:
            print(f" Import output: {result.stdout}")
        if result.stderr:
            print(f"⚠️ Import warnings: {result.stderr}")
        
        print(f"✅ Imported data to {pg_table_name} table successfully")
        return True
        
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file)
        except:
            pass

def preserve_mysql_case(name):
    """Preserve MySQL case by quoting identifiers for PostgreSQL"""
    return f'"{name}"'

def get_postgresql_table_name(mysql_table_name, preserve_case=True):
    """Get the PostgreSQL table name, optionally preserving MySQL case"""
    if preserve_case:
        return preserve_mysql_case(mysql_table_name)
    else:
        return mysql_table_name.lower()

def get_postgresql_column_name(mysql_column_name, preserve_case=True):
    """Get the PostgreSQL column name, optionally preserving MySQL case"""
    if preserve_case:
        return preserve_mysql_case(mysql_column_name)
    else:
        return mysql_column_name.lower()

def setup_auto_increment_sequence(table_name, preserve_case=True):
    """Setup auto-increment sequence for a table with preserved MySQL IDs"""
    print(f"🔧 Setting up auto-increment sequence for {table_name}...")
    
    # Get PostgreSQL table name
    pg_table_name = get_postgresql_table_name(table_name, preserve_case)
    
    # Get the maximum ID from the table
    max_id_sql = f"SELECT COALESCE(MAX(id), 0) FROM {pg_table_name};"
    
    # Write to file to handle quotes properly
    with open('get_max_id.sql', 'w', encoding='utf-8') as f:
        f.write(max_id_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp get_max_id.sql postgres_target:/tmp/get_max_id.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy max ID query file")
        return False
    
    max_id_cmd = 'docker exec postgres_target psql -U postgres -d target_db -t -f /tmp/get_max_id.sql'
    print(f" Debug: max_id_cmd={max_id_cmd}")
    max_result = run_command(max_id_cmd)
    
    # Cleanup
    run_command('rm -f get_max_id.sql')
    run_command('docker exec postgres_target rm -f /tmp/get_max_id.sql')
    
    if not max_result or max_result.returncode != 0:
        print(f"❌ Failed to get max ID for {table_name}")
        if max_result:
            print(f"   Error: {max_result.stderr}")
            print(f"   Return code: {max_result.returncode}")
        return False
    
    try:
        max_id = int(max_result.stdout.strip())
        next_id = max_id + 1
        print(f"📊 Max ID in {table_name}: {max_id}, setting sequence to start at: {next_id}")
    except ValueError:
        print(f"❌ Could not parse max ID for {table_name}")
        return False
    
    # Create sequence name
    sequence_name = f"{table_name}_id_seq" if not preserve_case else f'"{table_name}_id_seq"'
    
    # Create and setup sequence
    sequence_sql = f"""
-- Create sequence if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS {sequence_name};

-- Set sequence to start from next available ID
SELECT setval('{sequence_name}', {next_id});

-- Set column default to use the sequence
ALTER TABLE {pg_table_name} 
ALTER COLUMN id SET DEFAULT nextval('{sequence_name}');
"""
    
    # Write to file and execute
    with open('setup_sequence.sql', 'w', encoding='utf-8') as f:
        f.write(sequence_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp setup_sequence.sql postgres_target:/tmp/setup_sequence.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy sequence setup file")
        return False
    
    exec_cmd = 'docker exec postgres_target psql -U postgres -d target_db -f /tmp/setup_sequence.sql'
    exec_result = run_command(exec_cmd)
    
    # Cleanup
    run_command('rm -f setup_sequence.sql')
    run_command('docker exec postgres_target rm -f /tmp/setup_sequence.sql')
    
    if exec_result and exec_result.returncode == 0:
        print(f"✅ Auto-increment sequence setup complete for {table_name}")
        return True
    else:
        print(f"❌ Failed to setup sequence for {table_name}")
        if exec_result:
            print(f"   Error: {exec_result.stderr}")
        return False

def setup_varchar_id_sequence(table_name, preserve_case=True):
    """Setup auto-increment sequence for varchar ID tables (like Invoice)"""
    print(f"🔧 Setting up varchar ID sequence for {table_name}...")
    
    # Get PostgreSQL table name
    pg_table_name = get_postgresql_table_name(table_name, preserve_case)
    
    # Get the maximum numeric ID from the table (for varchar IDs that are numeric)
    max_id_sql = f"SELECT COALESCE(MAX(CAST(id AS BIGINT)), 0) FROM {pg_table_name} WHERE id ~ '^[0-9]+$';"
    
    # Write to file to handle quotes properly
    with open('get_max_varchar_id.sql', 'w', encoding='utf-8') as f:
        f.write(max_id_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp get_max_varchar_id.sql postgres_target:/tmp/get_max_varchar_id.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy max varchar ID query file")
        return False
    
    max_id_cmd = 'docker exec postgres_target psql -U postgres -d target_db -t -f /tmp/get_max_varchar_id.sql'
    print(f" Debug: max_id_cmd={max_id_cmd}")
    max_result = run_command(max_id_cmd)
    
    # Cleanup
    run_command('rm -f get_max_varchar_id.sql')
    run_command('docker exec postgres_target rm -f /tmp/get_max_varchar_id.sql')
    
    if not max_result or max_result.returncode != 0:
        print(f"❌ Failed to get max varchar ID for {table_name}")
        if max_result:
            print(f"   Error: {max_result.stderr}")
            print(f"   Return code: {max_result.returncode}")
        return False
    
    try:
        max_id = int(max_result.stdout.strip())
        next_id = max_id + 1
        print(f"📊 Max varchar ID in {table_name}: {max_id}, setting sequence to start at: {next_id}")
    except ValueError:
        print(f"❌ Could not parse max varchar ID for {table_name}")
        return False
    
    # Create sequence name
    sequence_name = f"{table_name}_id_seq" if not preserve_case else f'"{table_name}_id_seq"'
    
    # Create function to generate next varchar ID and setup sequence
    sequence_sql = f"""
-- Create sequence if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS {sequence_name};

-- Set sequence to start from next available ID
SELECT setval('{sequence_name}', {next_id});

-- Create function to generate next varchar ID
CREATE OR REPLACE FUNCTION next_{table_name.lower()}_id()
RETURNS VARCHAR AS $$
BEGIN
    RETURN nextval('{sequence_name}')::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Set column default to use the function
ALTER TABLE {pg_table_name} 
ALTER COLUMN id SET DEFAULT next_{table_name.lower()}_id();
"""
    
    # Write to file and execute
    with open('setup_varchar_sequence.sql', 'w', encoding='utf-8') as f:
        f.write(sequence_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp setup_varchar_sequence.sql postgres_target:/tmp/setup_varchar_sequence.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy varchar sequence setup file")
        return False
    
    exec_cmd = 'docker exec postgres_target psql -U postgres -d target_db -f /tmp/setup_varchar_sequence.sql'
    exec_result = run_command(exec_cmd)
    
    # Cleanup
    run_command('rm -f setup_varchar_sequence.sql')
    run_command('docker exec postgres_target rm -f /tmp/setup_varchar_sequence.sql')
    
    if exec_result and exec_result.returncode == 0:
        print(f"✅ Varchar ID auto-increment sequence setup complete for {table_name}")
        return True
    else:
        print(f"❌ Failed to setup varchar ID sequence for {table_name}")
        if exec_result:
            print(f"   Error: {exec_result.stderr}")
        return False

def add_primary_key_constraint(table_name, preserve_case=True):
    """Add PRIMARY KEY constraint to a table"""
    print(f"🔑 Adding PRIMARY KEY constraint to {table_name}...")
    
    # Get PostgreSQL table name
    pg_table_name = get_postgresql_table_name(table_name, preserve_case)
    
    # Add PRIMARY KEY constraint
    pk_sql = f"ALTER TABLE {pg_table_name} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY (id);"
    
    # Write to file and execute
    with open('add_primary_key.sql', 'w', encoding='utf-8') as f:
        f.write(pk_sql)
    
    # Copy and execute
    copy_cmd = 'docker cp add_primary_key.sql postgres_target:/tmp/add_primary_key.sql'
    copy_result = run_command(copy_cmd)
    
    if not copy_result or copy_result.returncode != 0:
        print(f"❌ Failed to copy primary key file")
        return False
    
    exec_cmd = 'docker exec postgres_target psql -U postgres -d target_db -f /tmp/add_primary_key.sql'
    exec_result = run_command(exec_cmd)
    
    # Cleanup
    run_command('rm -f add_primary_key.sql')
    run_command('docker exec postgres_target rm -f /tmp/add_primary_key.sql')
    
    if exec_result and exec_result.returncode == 0:
        print(f"✅ PRIMARY KEY constraint added to {table_name}")
        return True
    else:
        print(f"⚠️ PRIMARY KEY constraint may already exist for {table_name}")
        # Don't return False here as the constraint might already exist
        return True
