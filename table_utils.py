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

def get_mysql_table_columns(table_name):
    """Get column information from MySQL table"""
    print(f"🔍 Getting MySQL column info for {table_name}...")
    
    # Use DESCRIBE which gives more reliable output format
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "DESCRIBE {table_name};"'
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

def get_postgresql_table_columns(table_name):
    """Get column information from PostgreSQL table"""
    print(f"🔍 Getting PostgreSQL column info for {table_name}...")
    
    # Simplified query that works better for parsing
    cmd = f'docker exec postgres_target psql -U postgres -d target_db -c "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = \'{table_name.lower()}\' ORDER BY ordinal_position;"'
    
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

def compare_table_structures(table_name):
    """Compare table structures between MySQL and PostgreSQL"""
    print(f"🔍 Comparing table structures for {table_name}")
    print("=" * 60)
    
    # Get columns from both databases
    mysql_columns = get_mysql_table_columns(table_name)
    postgres_columns = get_postgresql_table_columns(table_name)
    
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

def verify_table_structure(table_name):
    """Verify that a table structure matches between MySQL and PostgreSQL"""
    print(f"🔍 Verifying {table_name} table structure consistency")
    print("=" * 70)
    
    # First check if tables exist
    mysql_exists_cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW TABLES LIKE \'{table_name}\';"'
    mysql_result = run_command(mysql_exists_cmd)
    
    postgres_exists_cmd = f'docker exec postgres_target psql -U postgres -d target_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'{table_name.lower()}\' AND table_schema = \'public\';"'
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
    print(f"📋 PostgreSQL {table_name.lower()} table exists: {'✅' if postgres_exists else '❌'}")
    
    if not mysql_exists:
        print(f"❌ MySQL table '{table_name}' does not exist!")
        return False
    
    if not postgres_exists:
        print(f"❌ PostgreSQL table '{table_name.lower()}' does not exist!")
        print("💡 Run the migration script first to create the table")
        return False
    
    print("\n" + "=" * 50)
    return compare_table_structures(table_name)

def check_docker_containers():
    """Check if Docker containers are running"""
    print("🔍 Checking Docker containers...")
    
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
    mysql_cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SELECT COUNT(*) FROM {table_name};"'
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
    print(f"🔍 Getting complete table info for {table_name} from MySQL...")
    
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW CREATE TABLE `{table_name}`;"'
    result = run_command(cmd)
    
    if not result or result.returncode != 0:
        print(f"❌ Failed to get table info: {result.stderr if result else 'No result'}")
        return None
    
    return result.stdout

def table_exists_mysql(table_name):
    """Check if table exists in MySQL"""
    cmd = f'docker exec mysql_source mysql -u root -prootpass -D source_db -e "SHOW TABLES LIKE \'{table_name}\';"'
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
    print(f"\n🔍 Analyzing column differences for {table_name}...")
    
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
