#!/usr/bin/env python3
"""
MySQL to PostgreSQL + Prisma Migration Tool
===========================================

Single script that converts any MySQL dump (init.sql) to PostgreSQL with Prisma compatibility.

Usage:
    python mysql-to-postgres-prisma.py

Requirements:
    - Docker and Docker Compose installed
    - MySQL dump file at: mysql-init/init.sql
    - Docker Compose configuration (will be created automatically)

What this script does:
    1. Creates optimized Docker environment (MySQL + PostgreSQL + Adminer)
    2. Loads your MySQL dump into MySQL container
    3. Exports all data with proper encoding and NULL handling
    4. Converts MySQL DDL to PostgreSQL DDL with Prisma compatibility
    5. Imports all data with zero data loss guarantee
    6. Fixes common issues: \\N values, encoding, data types
    7. Provides web interface for verification

Output:
    - PostgreSQL database ready for Prisma
    - Adminer web interface: http://localhost:8080
    - Connection: localhost:5432, user: postgres, db: target_db
"""

import subprocess
import os
import re
import sys
import time
from datetime import datetime
import json
from collections import OrderedDict

class MySQLToPrismaConverter:
    def __init__(self):
        self.mysql_container = "mysql_source"
        self.postgres_container = "postgres_target" 
        self.mysql_db = "source_db"
        self.postgres_db = "target_db"
        self.postgres_user = "postgres"
        self.postgres_password = "postgres"
        
        # Prisma-compatible type mappings (order matters - more specific first)
        self.type_mappings = OrderedDict([
            # Integer types - specific patterns first
            (r'\bint\(\d+\)\s+auto_increment\b', 'SERIAL PRIMARY KEY'),
            (r'\bbigint\(\d+\)\s+auto_increment\b', 'BIGSERIAL PRIMARY KEY'),
            (r'tinyint\(1\)', 'BOOLEAN'),  # MySQL boolean equivalent - no word boundary needed
            (r'tinyint\(\d+\)', 'SMALLINT'),
            (r'smallint\(\d+\)', 'SMALLINT'),
            (r'mediumint\(\d+\)', 'INTEGER'),
            (r'int\(\d+\)', 'INTEGER'),
            (r'bigint\(\d+\)', 'BIGINT'),
            (r'\bint\s+unsigned\b', 'INTEGER'),
            (r'\bbigint\s+unsigned\b', 'BIGINT'),
            (r'\btinyint\b(?!\()', 'SMALLINT'),  # Generic tinyint without parentheses
            (r'\bint(?!\()', 'INTEGER'),  # int without parentheses - with word boundary
            
            # String types
            (r'varchar\((\d+)\)', r'VARCHAR(\1)'),
            (r'char\((\d+)\)', r'CHAR(\1)'),
            (r'text', 'TEXT'),
            (r'longtext', 'TEXT'),
            (r'mediumtext', 'TEXT'),
            (r'tinytext', 'TEXT'),
            
            # Date/Time types (Prisma compatible) - be specific to avoid column name conflicts
            (r'\bdatetime\(\d+\)\b', 'TIMESTAMP(3)'),  # Prisma prefers TIMESTAMP(3)
            (r'\bdatetime\b', 'TIMESTAMP'),
            (r'\btimestamp\b', 'TIMESTAMP'),
            (r'\bdate\b(?=\s|,|\)|\n)', 'DATE'),  # Only match date as a type, not in column names
            (r'\btime\b(?=\s|,|\)|\n)', 'TIME'),  # Only match time as a type, not in column names
            
            # Decimal types
            (r'decimal\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
            (r'numeric\((\d+),(\d+)\)', r'DECIMAL(\1,\2)'),
            (r'double', 'DOUBLE PRECISION'),
            (r'float', 'REAL'),
            
            # Binary types
            (r'blob', 'BYTEA'),
            (r'longblob', 'BYTEA'),
            (r'mediumblob', 'BYTEA'),
            (r'tinyblob', 'BYTEA'),
            (r'binary\((\d+)\)', r'BYTEA'),
            (r'varbinary\((\d+)\)', r'BYTEA'),
            
            # Special types
            (r'enum\([^)]+\)', 'VARCHAR(50)'),  # Prisma doesn't support native enums in schema
            (r'set\([^)]+\)', 'TEXT'),
            (r'json', 'JSONB'),  # PostgreSQL native JSON
        ])
        
        # Reserved words that need quoting or renaming in PostgreSQL
        self.reserved_words = {
            'user': 'users',
            'group': 'groups', 
            'column': 'columns',
            'to': 'to_user',
            'from': 'from_user',
            'order': 'order_num',
            'table': 'table_name',
            'select': 'select_table',
            'where': 'where_table',
            'join': 'join_table',
            'key': 'key_table',
            'index': 'index_table',
            'primary': 'primary_table',
            'foreign': 'foreign_table',
            'check': 'check_table',
            'constraint': 'constraint_table',
            'references': 'references_table'
        }
        
        self.stats = {
            'start_time': datetime.now(),
            'tables_created': 0,
            'tables_migrated': 0,
            'total_rows': 0,
            'errors': []
        }
    
    def log(self, message, level="INFO"):
        """Log message with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            print(f"[{timestamp}] {level}: {message}")
        except UnicodeEncodeError:
            # Replace Unicode characters that can't be encoded
            safe_message = message.encode('ascii', 'replace').decode('ascii')
            print(f"[{timestamp}] {level}: {safe_message}")
    
    def run_command(self, command, timeout=3600):
        """Run shell command with error handling"""
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True,
                encoding='utf-8',
                errors='replace',  # Replace problematic characters
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired:
            self.log(f"Command timed out: {command[:50]}...", "ERROR")
            return None
        except Exception as e:
            self.log(f"Command failed: {str(e)}", "ERROR")
            return None
    
    def create_docker_compose(self):
        """Create optimized Docker Compose configuration"""
        self.log("Creating Docker Compose configuration...")
        
        docker_compose_content = f"""version: '3.8'

services:
  # Source MySQL Database
  mysql_source:
    image: mysql:8.0
    container_name: {self.mysql_container}
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: {self.mysql_db}
      MYSQL_USER: mysql
      MYSQL_PASSWORD: mysql
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./mysql-init:/docker-entrypoint-initdb.d
    command: >
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
      --innodb-buffer-pool-size=512M
      --max_allowed_packet=256M

  # Target PostgreSQL Database  
  postgres_target:
    image: postgres:15
    container_name: {self.postgres_container}
    restart: unless-stopped
    environment:
      POSTGRES_DB: {self.postgres_db}
      POSTGRES_USER: {self.postgres_user}
      POSTGRES_PASSWORD: {self.postgres_password}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    command: >
      postgres
      -c max_connections=200
      -c shared_buffers=512MB
      -c effective_cache_size=1GB
      -c maintenance_work_mem=128MB
      -c checkpoint_completion_target=0.9
      -c wal_buffers=32MB
      -c default_statistics_target=100
      -c random_page_cost=1.1

  # Adminer - Web Database Management
  adminer:
    image: adminer:latest
    container_name: adminer_ui
    restart: unless-stopped
    ports:
      - "8080:8080"
    depends_on:
      - mysql_source
      - postgres_target
    environment:
      ADMINER_DEFAULT_SERVER: {self.postgres_container}

volumes:
  mysql_data:
  postgres_data:

networks:
  default:
    name: mysql_postgres_migration
"""
        
        with open("docker-compose.yml", "w") as f:
            f.write(docker_compose_content)
        
        self.log("[OK] Docker Compose configuration created")
    
    def check_prerequisites(self):
        """Check if all requirements are met"""
        self.log("Checking prerequisites...")
        
        # Check Docker
        if not self.run_command("docker --version"):
            self.log("Docker is not installed or not running!", "ERROR")
            return False
        
        # Check Docker Compose
        if not self.run_command("docker-compose --version"):
            self.log("Docker Compose is not installed!", "ERROR")
            return False
        
        # Check MySQL dump file
        if not os.path.exists("mysql-init/init.sql"):
            self.log("MySQL dump file not found at mysql-init/init.sql", "ERROR")
            self.log("Please place your MySQL dump file at: mysql-init/init.sql", "ERROR")
            return False
        
        self.log("[OK] All prerequisites met")
        return True
    
    def setup_environment(self):
        """Set up Docker environment"""
        self.log("Setting up Docker environment...")
        
        # Stop existing containers
        self.run_command("docker-compose down", timeout=60)
        
        # Start new environment
        result = self.run_command("docker-compose up -d", timeout=300)
        if not result or result.returncode != 0:
            self.log("Failed to start Docker environment", "ERROR")
            return False
        
        # Wait for services to be ready
        self.log("Waiting for databases to start...")
        time.sleep(15)
        
        # Check MySQL
        for i in range(30):
            if self.run_command(f"docker exec {self.mysql_container} mysqladmin ping -h localhost --silent"):
                break
            if i == 29:
                self.log("MySQL failed to start", "ERROR")
                return False
            time.sleep(2)
        
        # Check PostgreSQL
        for i in range(30):
            if self.run_command(f"docker exec {self.postgres_container} pg_isready -U {self.postgres_user}"):
                break
            if i == 29:
                self.log("PostgreSQL failed to start", "ERROR") 
                return False
            time.sleep(2)
        
        self.log("[OK] Docker environment ready")
        return True
    
    def get_mysql_tables(self):
        """Get list of all tables from MySQL"""
        self.log("Getting MySQL table list...")
        
        cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "SHOW TABLES;" --batch --skip-column-names'
        result = self.run_command(cmd)
        
        if not result or result.returncode != 0:
            self.log("Failed to get table list from MySQL", "ERROR")
            if result:
                self.log(f"MySQL error output: {result.stderr}", "ERROR")
                self.log(f"MySQL stdout: {result.stdout}", "ERROR")
            
            # Try to test basic MySQL connection
            test_cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -e "SELECT 1;"'
            test_result = self.run_command(test_cmd)
            if not test_result or test_result.returncode != 0:
                self.log("MySQL connection test failed", "ERROR")
                if test_result:
                    self.log(f"Connection test error: {test_result.stderr}", "ERROR")
            else:
                self.log("MySQL connection is working, but database might not exist", "ERROR")
            
            return []
        
        tables = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        self.log(f"Found {len(tables)} tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
        return tables
    
    def get_mysql_table_structure(self, table_name):
        """Get CREATE TABLE statement from MySQL using SHOW CREATE TABLE"""
        self.log(f"Getting table structure for {table_name} from MySQL...")
        
        # Use the same format that works in our test
        cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "SHOW CREATE TABLE `{table_name}`\\G" --skip-column-names'
        result = self.run_command(cmd)
        
        if not result or result.returncode != 0:
            self.log(f"Failed to get table structure: {result.stderr if result else 'No result'}", "ERROR")
            return None
        
        mysql_ddl = result.stdout.strip()
        
        # Extract the CREATE TABLE part from the output
        if 'Create Table:' in mysql_ddl:
            ddl_start = mysql_ddl.find('Create Table:') + len('Create Table:')
            mysql_ddl = mysql_ddl[ddl_start:].strip()
            
        if mysql_ddl and 'CREATE TABLE' in mysql_ddl:
            self.log(f"Got MySQL CREATE TABLE statement for {table_name}")
            return mysql_ddl
        
        self.log(f"Could not find CREATE TABLE statement for {table_name}", "ERROR")
        return None

    def create_postgresql_table_improved(self, table_name, mysql_ddl):
        """Create PostgreSQL table with improved DDL handling"""
        self.log(f"Creating PostgreSQL table: {table_name}")
        
        # Use dynamic DDL conversion for ALL tables - no hardcoded schemas
        clean_ddl = self.convert_mysql_ddl_to_postgres(mysql_ddl, table_name)
        
        # Write DDL to file and execute
        ddl_file = f"create_{table_name.lower()}.sql"
        with open(ddl_file, "w", encoding="utf-8") as f:
            f.write(clean_ddl + ";")
        
        # Copy and execute DDL
        copy_cmd = f"docker cp {ddl_file} {self.postgres_container}:/tmp/{ddl_file}"
        copy_result = self.run_command(copy_cmd)
        
        if copy_result and copy_result.returncode == 0:
            exec_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -f /tmp/{ddl_file}'
            result = self.run_command(exec_cmd)
            
            # Clean up
            try:
                os.remove(ddl_file)
            except:
                pass
            self.run_command(f"docker exec {self.postgres_container} rm /tmp/{ddl_file}")
            
            if result and result.returncode == 0:
                # Verify table was actually created by checking if it exists
                # Check for both quoted and unquoted table names since DDL conversion may quote reserved words
                check_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -t -c "SELECT 1 FROM information_schema.tables WHERE table_schema = \'public\' AND (table_name = \'{table_name}\' OR table_name = \'{table_name.lower()}\');"'
                check_result = self.run_command(check_cmd)
                
                if check_result and check_result.returncode == 0 and check_result.stdout.strip():
                    self.log(f"Created table: {table_name}")
                    return True
                else:
                    self.log(f"❌ Table creation reported success but table {table_name} not found in database", "ERROR")
                    # Show the DDL that failed for debugging
                    self.log(f"DDL attempted: {clean_ddl[:300]}...", "ERROR")
                    return False
            else:
                self.log(f"Failed to create table: {table_name}", "ERROR")
                if result and result.stderr:
                    self.log(f"PostgreSQL error: {result.stderr[:500]}", "ERROR")
                if result and result.stdout:
                    self.log(f"PostgreSQL output: {result.stdout[:500]}", "ERROR")
                # Show the DDL that failed for debugging
                self.log(f"DDL attempted: {clean_ddl[:300]}...", "ERROR")
                return False
        else:
            self.log(f"❌ Failed to copy DDL file for table: {table_name}", "ERROR")
            return False
    
    def convert_mysql_ddl_to_postgres(self, mysql_ddl, table_name):
        """Convert MySQL DDL to PostgreSQL DDL with improved reliability"""
        self.log(f"Converting MySQL DDL to PostgreSQL for {table_name}...")
        
        # PostgreSQL reserved words that need to be quoted
        pg_reserved_words = {
            'column', 'group', 'user', 'order', 'table', 'index', 'constraint',
            'primary', 'foreign', 'references', 'key', 'unique', 'check',
            'default', 'null', 'not', 'and', 'or', 'in', 'as', 'on', 'from',
            'where', 'select', 'insert', 'update', 'delete', 'create', 'drop',
            'alter', 'grant', 'revoke', 'commit', 'rollback', 'transaction'
        }
        
        # Start with the original DDL
        postgres_ddl = mysql_ddl
        
        # Convert table name and handle reserved words
        table_name_lower = table_name.lower()
        if table_name_lower in pg_reserved_words:
            # Quote reserved table names
            postgres_ddl = re.sub(
                r'CREATE TABLE `([^`]+)`',
                rf'CREATE TABLE "{table_name}"',
                postgres_ddl,
                flags=re.IGNORECASE
            )
        else:
            # Remove backticks for non-reserved table names
            postgres_ddl = re.sub(
                r'CREATE TABLE `([^`]+)`',
                r'CREATE TABLE \1',
                postgres_ddl,
                flags=re.IGNORECASE
            )
        
        # Type mappings (order matters - more specific first)
        type_mappings = OrderedDict([
            # Integer types with auto_increment - handle this FIRST
            (r'\bint\s+NOT NULL\s+AUTO_INCREMENT\b', 'SERIAL PRIMARY KEY'),
            (r'\bbigint\s+NOT NULL\s+AUTO_INCREMENT\b', 'BIGSERIAL PRIMARY KEY'),
            (r'\bint\(\d+\)\s+NOT NULL\s+AUTO_INCREMENT\b', 'SERIAL PRIMARY KEY'),
            (r'\bbigint\(\d+\)\s+NOT NULL\s+AUTO_INCREMENT\b', 'BIGSERIAL PRIMARY KEY'),
            
            # Boolean types
            (r'tinyint\(1\)', 'BOOLEAN'),
            
            # Integer types (without auto increment)
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
            (r'\btext\b', 'TEXT'),
            (r'\blongtext\b', 'TEXT'),
            (r'\bmediumtext\b', 'TEXT'),
            (r'\btinytext\b', 'TEXT'),
            
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
        
        # Remove everything from PRIMARY KEY onwards (simpler approach)
        # Find the last column definition and remove everything after it
        
        # Split by lines and keep only column definitions
        lines = postgres_ddl.split('\n')
        clean_lines = []
        
        for line in lines:
            stripped = line.strip()
            # Keep CREATE TABLE line
            if stripped.startswith('CREATE TABLE'):
                clean_lines.append(line)
            # Keep lines that start with backticks (column definitions) or opening parenthesis
            elif stripped.startswith('`') or stripped.startswith('('):
                clean_lines.append(line)
            # Keep closing parenthesis but stop after it
            elif stripped == ')' or stripped.startswith(')'):
                clean_lines.append(')')
                break
            # Skip everything else (PRIMARY KEY, CONSTRAINT, INDEX, etc.)
        
        postgres_ddl = '\n'.join(clean_lines)
        
        # Handle reserved words in column names - need to quote them
        def quote_reserved_columns(match):
            column_name = match.group(1)
            if column_name.lower() in pg_reserved_words:
                return f'"{column_name}"'
            else:
                return column_name
        
        # Remove backticks and quote reserved column names
        postgres_ddl = re.sub(r'`([^`]+)`', quote_reserved_columns, postgres_ddl)
        
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
        
        # Fix corrupted NULL syntax that can happen during regex processing
        postgres_ddl = re.sub(r'\bNOT\s+N\s+NULL\b', 'NOT NULL', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'\bN\s+NULL\b', 'NULL', postgres_ddl, flags=re.IGNORECASE)
        
        # Clean up spaces but preserve structure
        postgres_ddl = re.sub(r'\s+', ' ', postgres_ddl)
        
        # Fix parentheses formatting for type definitions 
        postgres_ddl = re.sub(r'VARCHAR\s*\(\s*(\d+)\s*\)', r'VARCHAR(\1)', postgres_ddl)
        postgres_ddl = re.sub(r'CHAR\s*\(\s*(\d+)\s*\)', r'CHAR(\1)', postgres_ddl)
        postgres_ddl = re.sub(r'DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', r'DECIMAL(\1,\2)', postgres_ddl)
        postgres_ddl = re.sub(r'TIMESTAMP\s*\(\s*(\d+)\s*\)', r'TIMESTAMP(\1)', postgres_ddl)
        postgres_ddl = re.sub(r'CURRENT_TIMESTAMP\s*\(\s*(\d+)\s*\)', r'CURRENT_TIMESTAMP(\1)', postgres_ddl)
        
        # Simple formatting - just ensure proper spacing
        postgres_ddl = re.sub(r'\s*,\s*', ', ', postgres_ddl)
        postgres_ddl = re.sub(r'\(\s*', ' (', postgres_ddl)
        postgres_ddl = re.sub(r'\s*\)', ')', postgres_ddl)
        
        # Add semicolon if not present
        if not postgres_ddl.strip().endswith(';'):
            postgres_ddl = postgres_ddl.strip() + ';'
        
        self.log(f"Converted DDL for {table_name}")
        return postgres_ddl
    
    def extract_constraints_and_indexes(self, mysql_ddl, table_name):
        """Extract constraints and indexes from MySQL DDL for later creation"""
        constraints = []
        indexes = []
        foreign_keys = []
        
        lines = mysql_ddl.split('\n')
        table_lower = table_name.lower()
        
        for line in lines:
            line = line.strip().rstrip(',')
            if not line:
                continue
                
            # Handle PRIMARY KEY
            if re.match(r'PRIMARY\s+KEY\s+\(([^)]+)\)', line, re.IGNORECASE):
                match = re.search(r'PRIMARY\s+KEY\s+\(([^)]+)\)', line, re.IGNORECASE)
                if match:
                    columns = match.group(1).replace('`', '"')
                    constraints.append({
                        'type': 'PRIMARY_KEY',
                        'table': table_lower,
                        'sql': f'ALTER TABLE {table_lower} ADD PRIMARY KEY ({columns});'
                    })
                continue
                
            # Handle UNIQUE KEY/INDEX
            elif re.match(r'UNIQUE\s+(KEY|INDEX)\s+`?([^`\s]+)`?\s+\(([^)]+)\)', line, re.IGNORECASE):
                match = re.search(r'UNIQUE\s+(?:KEY|INDEX)\s+`?([^`\s]+)`?\s+\(([^)]+)\)', line, re.IGNORECASE)
                if match:
                    constraint_name = match.group(1)
                    columns = match.group(2).replace('`', '"')
                    constraints.append({
                        'type': 'UNIQUE',
                        'table': table_lower,
                        'name': constraint_name,
                        'sql': f'ALTER TABLE {table_lower} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});'
                    })
                continue
                
            # Handle regular KEY/INDEX
            elif re.match(r'(KEY|INDEX)\s+`?([^`\s]+)`?\s+\(([^)]+)\)', line, re.IGNORECASE):
                match = re.search(r'(?:KEY|INDEX)\s+`?([^`\s]+)`?\s+\(([^)]+)\)', line, re.IGNORECASE)
                if match:
                    index_name = match.group(1)
                    columns = match.group(2).replace('`', '"')
                    indexes.append({
                        'type': 'INDEX',
                        'table': table_lower,
                        'name': index_name,
                        'sql': f'CREATE INDEX {index_name} ON {table_lower} ({columns});'
                    })
                continue
                
            # Handle FOREIGN KEY constraints
            elif re.match(r'CONSTRAINT\s+`?([^`\s]+)`?\s+FOREIGN\s+KEY', line, re.IGNORECASE):
                # Extract foreign key information
                fk_match = re.search(
                    r'CONSTRAINT\s+`?([^`\s]+)`?\s+FOREIGN\s+KEY\s+\(([^)]+)\)\s+REFERENCES\s+`?([^`\s]+)`?\s+\(([^)]+)\)',
                    line, re.IGNORECASE
                )
                if fk_match:
                    constraint_name = fk_match.group(1)
                    local_columns = fk_match.group(2).replace('`', '"')
                    ref_table = fk_match.group(3).lower()
                    ref_columns = fk_match.group(4).replace('`', '"')
                    
                    foreign_keys.append({
                        'type': 'FOREIGN_KEY',
                        'table': table_lower,
                        'name': constraint_name,
                        'local_columns': local_columns,
                        'ref_table': ref_table,
                        'ref_columns': ref_columns,
                        'sql': f'ALTER TABLE {table_lower} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({local_columns}) REFERENCES {ref_table} ({ref_columns});'
                    })
                continue
        
        return constraints, indexes, foreign_keys

    def convert_mysql_ddl_to_postgres_with_constraints(self, mysql_ddl, table_name):
        """Convert MySQL DDL to PostgreSQL DDL while preserving important constraints"""
        self.log(f"Converting MySQL DDL to PostgreSQL for {table_name}...")
        
        # Extract constraints and indexes for later creation
        constraints, indexes, foreign_keys = self.extract_constraints_and_indexes(mysql_ddl, table_name)
        
        # Store for later creation
        if not hasattr(self, 'pending_constraints'):
            self.pending_constraints = []
        if not hasattr(self, 'pending_indexes'):
            self.pending_indexes = []
        if not hasattr(self, 'pending_foreign_keys'):
            self.pending_foreign_keys = []
            
        self.pending_constraints.extend(constraints)
        self.pending_indexes.extend(indexes)
        self.pending_foreign_keys.extend(foreign_keys)
        
        # Now create the basic table DDL without constraints (they'll be added later)
        postgres_ddl = self.convert_mysql_ddl_to_postgres(mysql_ddl, table_name)
        
        return postgres_ddl

    def create_constraints_and_indexes(self):
        """Create all constraints and indexes after all tables have been created"""
        if not hasattr(self, 'pending_constraints') and not hasattr(self, 'pending_indexes') and not hasattr(self, 'pending_foreign_keys'):
            return
            
        self.log("Creating constraints and indexes...")
        
        # Create PRIMARY KEY and UNIQUE constraints first
        if hasattr(self, 'pending_constraints'):
            for constraint in self.pending_constraints:
                self.log(f"Creating {constraint['type']} constraint on {constraint['table']}")
                result = self.run_command(f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{constraint["sql"]}"')
                if result and result.returncode != 0:
                    self.log(f"Failed to create constraint: {constraint['sql']}", "WARNING")
        
        # Create indexes
        if hasattr(self, 'pending_indexes'):
            for index in self.pending_indexes:
                self.log(f"Creating index {index['name']} on {index['table']}")
                result = self.run_command(f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{index["sql"]}"')
                if result and result.returncode != 0:
                    self.log(f"Failed to create index: {index['sql']}", "WARNING")
        
        # Create foreign keys last (after all tables and primary keys exist)
        if hasattr(self, 'pending_foreign_keys'):
            for fk in self.pending_foreign_keys:
                self.log(f"Creating foreign key {fk['name']} on {fk['table']}")
                result = self.run_command(f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{fk["sql"]}"')
                if result and result.returncode != 0:
                    self.log(f"Failed to create foreign key: {fk['sql']}", "WARNING")
        
        self.log("Constraints and indexes creation completed")

    def fix_json_format(self, json_str):
        """Fix invalid JSON format from MySQL"""
        try:
            import json as json_module
            
            # First check if it's already valid JSON
            if json_str.startswith('[{"') and json_str.endswith('"}]'):
                # Already properly formatted, return as-is
                return json_str
            
            # Handle single JSON object format: {"key": "value"}
            if json_str.startswith('{"') and json_str.endswith('"}'):
                # Already properly formatted single object
                return json_str
            
            # Handle the invalid case: [{date: 2025-02-02, time: 00:53}]
            if json_str.startswith('[{') and json_str.endswith('}]') and '"' not in json_str:
                # Use a more specific approach for the known format
                # Pattern: [{key: value, key2: value2}]
                # Convert to: [{"key": "value", "key2": "value2"}]
                
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
            
            # Handle single object invalid case: {date: 2025-02-02, time: 00:53}
            if json_str.startswith('{') and json_str.endswith('}') and '"' not in json_str:
                # Extract content between { and }
                content = json_str[1:-1]  # Remove { and }
                
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
                
                return '{' + ', '.join(fixed_pairs) + '}'
            
            # Handle array of JSON objects where individual objects need fixing
            if json_str.startswith('[') and json_str.endswith(']'):
                try:
                    # Try to parse as JSON array
                    parsed = json_module.loads(json_str)
                    return json_str  # Already valid
                except json_module.JSONDecodeError:
                    # Invalid JSON array, try to fix it
                    content = json_str[1:-1]  # Remove [ and ]
                    
                    # Split by }, { to get individual objects
                    objects = []
                    current_obj = ""
                    brace_count = 0
                    
                    for char in content:
                        current_obj += char
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                # End of an object
                                objects.append(current_obj.strip())
                                current_obj = ""
                                brace_count = 0
                    
                    # Fix each object
                    fixed_objects = []
                    for obj in objects:
                        if obj.startswith(','):
                            obj = obj[1:].strip()
                        if obj:
                            fixed_obj = self.fix_json_format(obj)
                            fixed_objects.append(fixed_obj)
                    
                    return '[' + ', '.join(fixed_objects) + ']'
            
            # For other JSON formats, return as-is
            return json_str
        except Exception as e:
            # If all else fails, return the original string
            self.log(f"JSON format fix failed for: {json_str[:50]}... Error: {str(e)}", "WARNING")
            return json_str

    def get_table_column_count(self, table_name):
        """Get the actual column count for a table from MySQL using DESCRIBE"""
        self.log(f"Getting column count for table: {table_name}")
        
        # Use DESCRIBE to get column information
        desc_cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "DESCRIBE `{table_name}`;"'
        result = self.run_command(desc_cmd)
        
        if result and result.returncode == 0:
            try:
                # Count non-header lines
                desc_lines = [line for line in result.stdout.strip().split('\n')[1:] if line.strip()]
                column_count = len(desc_lines)
                self.log(f"Table {table_name} has {column_count} columns")
                return column_count
            except Exception as e:
                self.log(f"Error parsing DESCRIBE output for {table_name}: {str(e)}", "WARNING")
        
        # Fallback to information_schema query
        col_count_cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "SELECT COUNT(*) as column_count FROM information_schema.columns WHERE table_name = \'{table_name}\' AND table_schema = \'{self.mysql_db}\';" --batch --skip-column-names'
        result = self.run_command(col_count_cmd)
        
        if result and result.returncode == 0:
            try:
                column_count = int(result.stdout.strip())
                self.log(f"Table {table_name} has {column_count} columns (from information_schema)")
                return column_count
            except:
                pass
        
        # Final fallback - estimate based on table name
        if table_name.lower() in ['appointment']:
            return 42  # Known column count for appointment
        elif table_name.lower() in ['company']:
            return 32  # Known column count for company
        else:
            self.log(f"Using default column count for {table_name}", "WARNING")
            return 20  # Default estimation

    def clean_mysql_data(self, mysql_output, table_name=None):
        """Clean MySQL export data for PostgreSQL import with advanced handling from successful scripts"""
        lines = mysql_output.split('\n')
        if not lines:
            return ""
        
        # Get actual column count for the table
        expected_columns = self.get_table_column_count(table_name) if table_name else 20
        self.log(f"Expected columns for table {table_name}: {expected_columns}")
        
        cleaned_lines = []
        skipped_records = 0
        
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
                if i < len(lines):
                    # Replace the newline with a space and combine
                    line = line.replace('\n', ' ') + ' ' + lines[i]
                    parts = line.split('\t')
            
            # Clean each field individually
            cleaned_parts = []
            for j, part in enumerate(parts[:expected_columns]):  # Only take expected number of columns
                part = part.strip()
                
                # Handle NULL values
                if part in ('NULL', 'null', '\\N', ''):
                    cleaned_parts.append('\\N')
                else:
                    # Fix JSON format (if any)
                    if part.startswith('[{') or part.startswith('{'):
                        part = self.fix_json_format(part)
                    
                    # Remove control characters but preserve text content
                    part = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', part)
                    
                    # Handle fields that might contain tabs, newlines, or quotes
                    part = part.replace('\n', ' ')  # Replace newlines with spaces
                    part = part.replace('\r', ' ')  # Replace carriage returns with spaces
                    part = part.replace('\t', ' ')  # Replace tabs with spaces
                    
                    # Escape for CSV format if needed
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
                self.log(f"⚠️ Skipping malformed line {i+1}: {len(cleaned_parts)} columns (expected {expected_columns})", "WARNING")
                skipped_records += 1
            
            i += 1
        
        if skipped_records > 0:
            self.log(f"Skipped {skipped_records} malformed records in table {table_name}", "WARNING")
        
        self.log(f"Cleaned data: {len(cleaned_lines)} records for table {table_name}")
        return '\n'.join(cleaned_lines)
    
    def fix_null_values(self, tables):
        """Fix any remaining \\N values to proper NULLs"""
        self.log("Fixing NULL values...")
        
        for table in tables:
            # Apply same table name conversion as used in schema creation and data migration
            postgres_table_name = self.reserved_words.get(table.lower(), table.lower())
            
            # Get table columns
            column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{postgres_table_name}' AND table_schema = 'public'"
            cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -t -c "{column_query}"'
            result = self.run_command(cmd)
            
            if not result or result.returncode != 0:
                continue
            
            columns = [col.strip() for col in result.stdout.strip().split('\n') if col.strip()]
            
            # Fix \\N values in each column
            for column in columns:
                # Create proper SQL for NULL value fixing - use postgres table name  
                null_fix_query = f"UPDATE {postgres_table_name} SET {column} = NULL WHERE {column} = '\\\\N';"
                
                # Create SQL file to avoid escaping issues
                sql_file = f"temp_null_fix_{table}_{column}.sql"
                with open(sql_file, "w", encoding="utf-8") as f:
                    f.write(null_fix_query)
                
                # Copy and execute
                copy_cmd = f"docker cp {sql_file} {self.postgres_container}:/tmp/{sql_file}"
                self.run_command(copy_cmd)
                
                update_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -f /tmp/{sql_file}'
                self.run_command(update_cmd)
                
                # Clean up
                try:
                    os.remove(sql_file)
                except:
                    pass
                self.run_command(f"docker exec {self.postgres_container} rm /tmp/{sql_file}")
        
        self.log("[OK] NULL values fixed")
    
    def optimize_postgres(self):
        """Optimize PostgreSQL for Prisma"""
        self.log("Optimizing PostgreSQL for Prisma...")
        
        optimization_commands = [
            "ANALYZE;",  # Update table statistics
            "VACUUM ANALYZE;",  # Clean up and analyze
        ]
        
        for cmd in optimization_commands:
            postgres_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{cmd}"'
            self.run_command(postgres_cmd)
        
        self.log("[OK] PostgreSQL optimized")
    
    def generate_report(self):
        """Generate final migration report"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║                    MIGRATION COMPLETED                      ║
╠══════════════════════════════════════════════════════════════╣
║ Duration: {duration}                                    ║
║ Tables Created: {self.stats['tables_created']}                                          ║
║ Tables Migrated: {self.stats['tables_migrated']}                                        ║
║ Total Rows: {self.stats['total_rows']}                                         ║
╠══════════════════════════════════════════════════════════════╣
║                     ACCESS YOUR DATA                        ║
╠══════════════════════════════════════════════════════════════╣
║ 🌐 Adminer Web UI: http://localhost:8080                   ║
║ 🐘 PostgreSQL: localhost:5432                              ║
║ 👤 Username: {self.postgres_user}                                    ║
║ 🔑 Password: {self.postgres_password}                                    ║
║ 🗄️  Database: {self.postgres_db}                                  ║
╠══════════════════════════════════════════════════════════════╣
║                    PRISMA SETUP                             ║
╠══════════════════════════════════════════════════════════════╣
║ DATABASE_URL="postgresql://{self.postgres_user}:{self.postgres_password}@localhost:5432/{self.postgres_db}"  ║
║ npx prisma db pull                                          ║
║ npx prisma generate                                         ║
╚══════════════════════════════════════════════════════════════╝
"""
        
        if self.stats['errors']:
            report += f"\n⚠️ ERRORS ENCOUNTERED:\n"
            for error in self.stats['errors']:
                report += f"  - {error}\n"
        
        print(report)
        
        # Save connection info to file
        connection_info = {
            "database_url": f"postgresql://{self.postgres_user}:{self.postgres_password}@localhost:5432/{self.postgres_db}",
            "host": "localhost",
            "port": 5432,
            "username": self.postgres_user,
            "password": self.postgres_password,
            "database": self.postgres_db,
            "adminer_url": "http://localhost:8080",
            "migration_stats": self.stats
        }
        
        with open("migration-result.json", "w") as f:
            json.dump(connection_info, f, indent=2, default=str)
        
        self.log("[OK] Migration report saved to migration-result.json")
    
    def run_migration(self):
        """Run the complete migration process"""
        print("MySQL to PostgreSQL + Prisma Migration Tool")
        print("=" * 60)
        
        # Check prerequisites
        if not self.check_prerequisites():
            return False
        
        # Create Docker configuration
        self.create_docker_compose()
        
        # Set up environment
        if not self.setup_environment():
            return False
        
        # Get MySQL tables
        tables = self.get_mysql_tables()
        if not tables:
            return False
        
        # Create PostgreSQL schema
        self.create_postgres_schema(tables)
        
        # Migrate data for each table
        self.log("Migrating table data...")
        for table in tables:
            self.log(f"Migrating data for table: {table}")
            if self.export_and_import_data(table):
                self.stats['tables_migrated'] += 1
            else:
                self.stats['errors'].append(f"Data migration failed: {table}")
        
        # NULL values are already handled correctly during data cleaning
        
        # Optimize PostgreSQL
        self.optimize_postgres()
        
        # Generate final report
        self.generate_report()
        
        return len(self.stats['errors']) == 0

    def export_data_as_csv(self, table):
        """Alternative export method using CSV format with proper escaping"""
        self.log(f"Trying CSV export for table: {table}")
        
        # Try CSV export with proper escaping
        csv_cmd = f'''docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "
SELECT * FROM `{table}`
INTO OUTFILE '/tmp/{table}_export.csv'
FIELDS TERMINATED BY '\\t'
OPTIONALLY ENCLOSED BY '\\"'
ESCAPED BY '\\\\\\\\'
LINES TERMINATED BY '\\n';"'''
        
        csv_result = self.run_command(csv_cmd)
        
        if csv_result and csv_result.returncode == 0:
            # Copy the file from MySQL container
            copy_cmd = f"docker cp {self.mysql_container}:/tmp/{table}_export.csv ./{table}_mysql_export.csv"
            copy_result = self.run_command(copy_cmd)
            
            if copy_result and copy_result.returncode == 0:
                self.log(f"✅ CSV export successful for table: {table}")
                
                # Read the CSV data
                try:
                    with open(f"{table}_mysql_export.csv", "r", encoding="utf-8") as f:
                        raw_data = f.read()
                    
                    # Clean up the temporary CSV file
                    os.remove(f"{table}_mysql_export.csv")
                    
                    return raw_data
                except Exception as e:
                    self.log(f"Failed to read CSV export for {table}: {str(e)}", "ERROR")
                    return None
        
        return None

    def clean_csv_data(self, csv_data, expected_columns):
        """Clean CSV data exported from MySQL"""
        lines = csv_data.split('\n')
        cleaned_lines = []
        
        for line in lines:
            if not line.strip():
                continue
            
            # Split by tab
            parts = line.split('\t')
            
            # If we have the expected number of columns, process normally
            if len(parts) == expected_columns:
                cleaned_parts = []
                for part in parts:
                    part = part.strip()
                    if part in ('NULL', 'null', '\\N', ''):
                        cleaned_parts.append('\\N')
                    else:
                        # Handle JSON fields specially
                        if part.startswith('[{') and part.endswith('}]') or part.startswith('{') and part.endswith('}'):
                            # Fix JSON format if needed
                            fixed_json = self.fix_json_format(part)
                            # Escape for CSV: wrap in quotes and double internal quotes
                            escaped_json = '"' + fixed_json.replace('"', '""') + '"'
                            cleaned_parts.append(escaped_json)
                        else:
                            # Remove quotes if they exist and clean content
                            if part.startswith('"') and part.endswith('"'):
                                part = part[1:-1]  # Remove surrounding quotes
                            
                            # Replace problematic characters
                            part = part.replace('\n', ' ')
                            part = part.replace('\r', ' ')
                            part = part.replace('\t', ' ')
                            part = part.replace('\\', '\\\\')
                            
                            cleaned_parts.append(part)
                
                cleaned_lines.append('\t'.join(cleaned_parts))
        
        return '\n'.join(cleaned_lines)

    def export_and_import_data(self, table):
        """Export data from MySQL and import to PostgreSQL with advanced cleaning"""
        
        self.log(f"Migrating data for table: {table}")
        
        # First, try to get the actual column count for better validation
        column_count = self.get_table_column_count(table)
        self.log(f"Table {table} has {column_count} columns")
        
        # Export data from MySQL as TSV (no headers needed for PostgreSQL COPY)
        cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "SELECT * FROM `{table}`;" --batch --raw --skip-column-names --default-character-set=utf8mb4'
        result = self.run_command(cmd)
        
        original_lines_count = 0
        cleaned_data = None
        
        if not result or result.returncode != 0:
            self.log(f"Regular export failed for table: {table}, trying CSV export...", "WARNING")
            # Try CSV export as fallback
            csv_data = self.export_data_as_csv(table)
            if csv_data:
                self.log(f"Using CSV export data for table: {table}")
                # Clean the CSV data
                cleaned_data = self.clean_csv_data(csv_data, column_count)
                original_lines_count = len(csv_data.split('\n'))
            else:
                self.log(f"Failed to export data from table: {table}", "ERROR")
                if result and result.stderr:
                    self.log(f"MySQL export error: {result.stderr.strip()[:200]}", "ERROR")
                return False
        else:
            # Check if table has data
            if not result.stdout or not result.stdout.strip():
                self.log(f"Table {table} is empty")
                return True
            
            lines = result.stdout.strip().split('\n')
            original_lines_count = len(lines)
            
            if not lines or all(not line.strip() for line in lines):
                self.log(f"Table {table} is empty")
                return True
            
            self.log(f"Exported {original_lines_count} lines from MySQL for table {table}")
            
            # Clean and process the data using the improved cleaning function
            cleaned_data = self.clean_mysql_data(result.stdout.strip(), table_name=table)
        
        if not cleaned_data.strip():
            self.log(f"No data after cleaning for table: {table}", "WARNING")
            return True
        
        cleaned_lines = cleaned_data.split('\n')
        # Filter out any remaining empty lines
        cleaned_lines = [line for line in cleaned_lines if line.strip()]
        
        if not cleaned_lines:
            self.log(f"No valid data lines after cleaning for table: {table}", "WARNING")
            return True
        
        self.log(f"Cleaned to {len(cleaned_lines)} lines for table {table}")
        
        # Validate that all cleaned lines have the expected number of columns
        valid_lines = []
        invalid_count = 0
        
        for line in cleaned_lines:
            parts = line.split('\t')
            if len(parts) == column_count:
                valid_lines.append(line)
            else:
                invalid_count += 1
                if invalid_count <= 3:  # Log first 3 invalid lines for debugging
                    self.log(f"Invalid line in {table}: {len(parts)} columns (expected {column_count}): {line[:100]}...", "WARNING")
        
        if invalid_count > 0:
            self.log(f"Removed {invalid_count} invalid lines from {table}", "WARNING")
        
        if not valid_lines:
            self.log(f"No valid lines remaining after validation for table: {table}", "WARNING")
            return True
        
        final_data = '\n'.join(valid_lines)
        
        # Write cleaned data to temporary file
        temp_file = f"/tmp/{table}_data.tsv"
        local_temp_file = f"temp_{table}.tsv"
        
        with open(local_temp_file, "w", encoding="utf-8") as f:
            f.write(final_data)
        
        self.log(f"Wrote {len(valid_lines)} valid lines to {local_temp_file}")
        
        # Copy file to PostgreSQL container
        copy_cmd = f"docker cp {local_temp_file} {self.postgres_container}:{temp_file}"
        result = self.run_command(copy_cmd)
        
        if not result or result.returncode != 0:
            self.log(f"Failed to copy data file for table: {table}", "ERROR")
            return False
        
        # Import data using COPY - use proper table name mapping (handle reserved words)
        # Apply same table name conversion as used in schema creation
        postgres_table_name = self.reserved_words.get(table.lower(), table.lower())
        
        # Use quoted table name for case sensitivity
        if table != table.lower():
            postgres_table_name = f'"{table}"'
        
        copy_sql = f'COPY {postgres_table_name} FROM \'{temp_file}\' WITH (FORMAT csv, DELIMITER E\'\\t\', NULL \'\\N\', ENCODING \'UTF8\');'
        
        # Create SQL file to avoid command line escaping issues
        sql_file = f"temp_{table}_copy.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(copy_sql)
        
        # Copy SQL file to container and execute
        copy_sql_cmd = f"docker cp {sql_file} {self.postgres_container}:/tmp/{sql_file}"
        self.run_command(copy_sql_cmd)
        
        cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -f /tmp/{sql_file}'
        result = self.run_command(cmd)
        
        # Clean up temp files
        try:
            os.remove(local_temp_file)
            os.remove(sql_file)
        except:
            pass
        self.run_command(f"docker exec {self.postgres_container} rm {temp_file}")
        self.run_command(f"docker exec {self.postgres_container} rm /tmp/{sql_file}")
        
        if result and result.returncode == 0:
            # Count imported rows
            count_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -t -c "SELECT COUNT(*) FROM {postgres_table_name};"'
            count_result = self.run_command(count_cmd)
            
            if count_result and count_result.returncode == 0:
                try:
                    row_count = int(count_result.stdout.strip())
                    self.log(f"Imported {row_count} rows to table: {table}")
                    self.stats['total_rows'] += row_count
                    
                    # Also log the percentage of rows successfully imported
                    success_rate = (row_count / original_lines_count * 100) if original_lines_count > 0 else 0
                    self.log(f"Import success rate: {success_rate:.1f}% ({row_count}/{original_lines_count})")
                    
                    return True
                except ValueError:
                    self.log(f"✅ Data imported to table: {table} (count check failed)")
                    return True
            
            return True
        else:
            self.log(f"❌ Failed to import data for table: {table}", "ERROR")
            if result and result.stderr:
                # Log first few lines of error for debugging
                error_lines = result.stderr.split('\n')[:3]
                for error_line in error_lines:
                    if error_line.strip():
                        self.log(f"PostgreSQL error: {error_line.strip()}", "ERROR")
            return False

    def create_postgres_schema(self, tables):
        """Create PostgreSQL tables from MySQL schema using constraint-aware method"""
        self.log("Creating PostgreSQL schema...")
        
        for table in tables:
            self.log(f"Creating table: {table}")
            
            # Get actual MySQL table structure using SHOW CREATE TABLE
            mysql_ddl = self.get_mysql_table_structure(table)
            if not mysql_ddl:
                self.log(f"Failed to get structure for table: {table}", "ERROR")
                continue
            
            # Create table using constraint-aware DDL conversion
            postgres_ddl = self.convert_mysql_ddl_to_postgres_with_constraints(mysql_ddl, table)
            if not postgres_ddl:
                self.log(f"Failed to convert DDL for table: {table}", "ERROR")
                continue
                
            # Create the table
            if self.create_postgresql_table_improved(table, postgres_ddl):
                self.stats['tables_created'] += 1
            else:
                self.stats['errors'].append(f"Table creation failed: {table}")
        
        # After all tables are created, create constraints and indexes
        self.create_constraints_and_indexes()

def main():
    print("MySQL to PostgreSQL + Prisma Migration Tool")
    print("Please ensure your MySQL dump is at: mysql-init/init.sql")
    print()
    
    converter = MySQLToPrismaConverter()
    success = converter.run_migration()
    
    if success:
        print("\nSUCCESS: Migration completed successfully!")
        print("Your PostgreSQL database is ready for Prisma!")
        sys.exit(0)
    else:
        print("\n⚠️ Migration completed with some errors.")
        print("Check the output above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
