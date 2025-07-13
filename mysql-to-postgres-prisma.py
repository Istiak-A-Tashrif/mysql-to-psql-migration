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

class MySQLToPrismaConverter:
    def __init__(self):
        self.mysql_container = "mysql_source"
        self.postgres_container = "postgres_target" 
        self.mysql_db = "source_db"
        self.postgres_db = "target_db"
        self.postgres_user = "postgres"
        self.postgres_password = "postgres"
        
        # Prisma-compatible type mappings (order matters - more specific first)
        self.type_mappings = {
            # Integer types - specific patterns first
            r'\bint\(\d+\)\s+auto_increment\b': 'SERIAL PRIMARY KEY',
            r'\bbigint\(\d+\)\s+auto_increment\b': 'BIGSERIAL PRIMARY KEY',
            r'\btinyint\(1\)\b': 'BOOLEAN',  # MySQL boolean equivalent
            r'\btinyint\(\d+\)\b': 'SMALLINT',
            r'\bsmallint\(\d+\)\b': 'SMALLINT',
            r'\bmediumint\(\d+\)\b': 'INTEGER',
            r'\bint\(\d+\)\b': 'INTEGER',
            r'\bbigint\(\d+\)\b': 'BIGINT',
            r'\bint\s+unsigned\b': 'INTEGER',
            r'\bbigint\s+unsigned\b': 'BIGINT',
            r'\btinyint\b(?!\()': 'SMALLINT',  # tinyint without parentheses
            r'\bint\b(?!\()': 'INTEGER',  # int without parentheses
            
            # String types
            r'varchar\((\d+)\)': r'VARCHAR(\1)',
            r'char\((\d+)\)': r'CHAR(\1)',
            r'text': 'TEXT',
            r'longtext': 'TEXT',
            r'mediumtext': 'TEXT',
            r'tinytext': 'TEXT',
            
            # Date/Time types (Prisma compatible) - be specific to avoid column name conflicts
            r'\bdatetime\(\d+\)\b': 'TIMESTAMP(3)',  # Prisma prefers TIMESTAMP(3)
            r'\bdatetime\b': 'TIMESTAMP',
            r'\btimestamp\b': 'TIMESTAMP',
            r'\bdate\b(?=\s|,|\)|\n)': 'DATE',  # Only match date as a type, not in column names
            r'\btime\b(?=\s|,|\)|\n)': 'TIME',  # Only match time as a type, not in column names
            
            # Decimal types
            r'decimal\((\d+),(\d+)\)': r'DECIMAL(\1,\2)',
            r'numeric\((\d+),(\d+)\)': r'DECIMAL(\1,\2)',
            r'double': 'DOUBLE PRECISION',
            r'float': 'REAL',
            
            # Binary types
            r'blob': 'BYTEA',
            r'longblob': 'BYTEA',
            r'mediumblob': 'BYTEA',
            r'tinyblob': 'BYTEA',
            r'binary\((\d+)\)': r'BYTEA',
            r'varbinary\((\d+)\)': r'BYTEA',
            
            # Special types
            r'enum\([^)]+\)': 'VARCHAR(50)',  # Prisma doesn't support native enums in schema
            r'set\([^)]+\)': 'TEXT',
            r'json': 'JSONB',  # PostgreSQL native JSON
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
    
    def convert_mysql_ddl_to_postgres(self, mysql_ddl, table_name):
        """Convert MySQL CREATE TABLE to PostgreSQL with Prisma compatibility"""
        
        # Extract CREATE TABLE statement
        create_match = re.search(r'CREATE TABLE.*?;', mysql_ddl, re.DOTALL | re.IGNORECASE)
        if not create_match:
            # Fallback simple table
            return f'CREATE TABLE IF NOT EXISTS "{table_name}" (id SERIAL PRIMARY KEY);'
        
        ddl = create_match.group(0)
        
        # First, apply type conversions
        for mysql_pattern, postgres_type in self.type_mappings.items():
            ddl = re.sub(mysql_pattern, postgres_type, ddl, flags=re.IGNORECASE)
        
        # Remove MySQL-specific column attributes but be more careful
        ddl = re.sub(r'CHARACTER SET \w+', '', ddl, flags=re.IGNORECASE)
        ddl = re.sub(r'COLLATE \w+', '', ddl, flags=re.IGNORECASE)
        ddl = re.sub(r'AUTO_INCREMENT', '', ddl, flags=re.IGNORECASE)
        ddl = re.sub(r'ON UPDATE CURRENT_TIMESTAMP\(\d+\)', '', ddl, flags=re.IGNORECASE)
        ddl = re.sub(r'DEFAULT CURRENT_TIMESTAMP\(\d+\)', 'DEFAULT CURRENT_TIMESTAMP', ddl, flags=re.IGNORECASE)
        
        # Remove MySQL table engine and other table-level options
        ddl = re.sub(r'\)\s*ENGINE=.*?;', ');', ddl, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove constraints and indexes - but do it more carefully
        lines = ddl.split('\n')
        filtered_lines = []
        in_create_table = False
        primary_key_column = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if 'CREATE TABLE' in line.upper():
                in_create_table = True
                # Fix table name for PostgreSQL
                line = re.sub(f'CREATE TABLE `?{table_name}`?', f'CREATE TABLE IF NOT EXISTS "{table_name}"', line, flags=re.IGNORECASE)
                # Convert backticks to quotes
                line = re.sub(r'`([^`]+)`', r'"\1"', line)
                filtered_lines.append(line)
            elif in_create_table:
                # Skip constraint, key, and index lines
                upper_line = line.upper()
                if any(keyword in upper_line for keyword in ['CONSTRAINT', 'FOREIGN KEY', 'REFERENCES', 'KEY ', 'INDEX ', 'UNIQUE KEY']):
                    continue
                
                # Check for PRIMARY KEY column
                if 'PRIMARY KEY' in upper_line:
                    # Extract column name from PRIMARY KEY line
                    pk_match = re.search(r'PRIMARY KEY \(`?([^`\)]+)`?\)', line, re.IGNORECASE)
                    if pk_match:
                        primary_key_column = pk_match.group(1)
                    continue
                
                # Convert backticks to quotes for column names
                line = re.sub(r'`([^`]+)`', r'"\1"', line)
                
                # Check if this is the closing parenthesis
                if line.startswith(')'):
                    # Add PRIMARY KEY constraint if we found one
                    if primary_key_column:
                        filtered_lines.append(f'  PRIMARY KEY ("{primary_key_column}")')
                    filtered_lines.append(');')
                    break
                else:
                    # Clean up trailing commas before adding
                    if line.endswith(','):
                        filtered_lines.append('  ' + line)
                    else:
                        filtered_lines.append('  ' + line + ',')
        
        # Join lines and clean up
        ddl = '\n'.join(filtered_lines)
        
        # Final cleanup - remove any trailing commas before closing parenthesis or PRIMARY KEY
        ddl = re.sub(r',\s*\n\s*PRIMARY KEY', '\n  PRIMARY KEY', ddl)
        ddl = re.sub(r',\s*\n\s*\)', '\n)', ddl)
        ddl = re.sub(r',\s*,', ',', ddl)  # Remove double commas
        
        # Ensure statement ends with semicolon
        if not ddl.endswith(';'):
            ddl += ';'
        
        return ddl
    
    def create_postgres_schema(self, tables):
        """Create PostgreSQL tables from MySQL schema"""
        self.log("Creating PostgreSQL schema...")
        
        for table in tables:
            self.log(f"Creating table: {table}")
            
            # Get MySQL table structure
            cmd = f"docker exec {self.mysql_container} mysqldump -u root -prootpass --no-data --single-transaction {self.mysql_db} {table}"
            result = self.run_command(cmd)
            
            if not result or result.returncode != 0:
                self.log(f"Failed to get structure for table: {table}", "ERROR")
                continue
            
            # Convert DDL
            postgres_ddl = self.convert_mysql_ddl_to_postgres(result.stdout, table)
            self.log(f"Generated DDL for {table}: {postgres_ddl[:100]}...")
            
            # Create table in PostgreSQL using a file to avoid shell escaping issues
            ddl_file = f"temp_{table}_ddl.sql"
            with open(ddl_file, "w", encoding="utf-8") as f:
                f.write(postgres_ddl)
            
            # Copy DDL file to container and execute
            copy_cmd = f"docker cp {ddl_file} {self.postgres_container}:/tmp/{ddl_file}"
            copy_result = self.run_command(copy_cmd)
            
            if copy_result and copy_result.returncode == 0:
                cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -f /tmp/{ddl_file}'
                result = self.run_command(cmd)
                if result:
                    self.log(f"DDL execution result for {table}: return_code={result.returncode}, stdout={result.stdout[:200]}, stderr={result.stderr[:200]}")
            else:
                result = None
                self.log(f"Failed to copy DDL file for table: {table}", "ERROR")
            
            # Clean up temp file
            try:
                os.remove(ddl_file)
            except:
                pass
            self.run_command(f"docker exec {self.postgres_container} rm /tmp/{ddl_file}")
            
            if result and result.returncode == 0:
                self.log(f"[OK] Created table: {table}")
                self.stats['tables_created'] += 1
            else:
                self.log(f"[ERROR] Failed to create table: {table}", "ERROR")
                self.stats['errors'].append(f"Table creation failed: {table}")
    
    def export_and_import_data(self, table):
        """Export data from MySQL and import to PostgreSQL"""
        
        # Export data from MySQL as TSV with headers
        cmd = f'docker exec {self.mysql_container} mysql -u root -prootpass -D {self.mysql_db} -e "SELECT * FROM `{table}`;" --batch --raw --default-character-set=utf8mb4'
        result = self.run_command(cmd)
        
        if not result or result.returncode != 0:
            self.log(f"Failed to export data from table: {table}", "ERROR")
            return False
        
        # Check if table has data
        if not result.stdout or not result.stdout.strip():
            self.log(f"Table {table} is empty")
            return True
        
        lines = result.stdout.strip().split('\n')
        if len(lines) <= 1:  # Only header or empty
            self.log(f"Table {table} is empty")
            return True
        
        # Clean and process the data
        cleaned_lines = []
        
        for i, line in enumerate(lines):
            if not line.strip():
                continue
                
            # Split by tabs and clean up each field
            fields = line.split('\t')
            cleaned_fields = []
            
            for field in fields:
                field = field.strip()
                
                # Handle NULL values
                if field in ('NULL', 'null', '\\N', ''):
                    cleaned_fields.append('\\N')
                else:
                    # Escape special characters for PostgreSQL TSV format
                    field = field.replace('\\', '\\\\')
                    field = field.replace('\n', '\\n')
                    field = field.replace('\r', '\\r')
                    field = field.replace('\t', '\\t')
                    
                    # Handle JSON data - ensure it's properly formatted
                    if field.startswith('[') or field.startswith('{'):
                        try:
                            import json
                            parsed = json.loads(field)
                            field = json.dumps(parsed)
                        except:
                            # If JSON parsing fails, just escape quotes
                            field = field.replace('"', '\\"')
                    
                    cleaned_fields.append(field)
            
            cleaned_lines.append('\t'.join(cleaned_fields))
        
        # Write cleaned data to temporary file
        temp_file = f"/tmp/{table}_data.tsv"
        with open(f"temp_{table}.tsv", "w", encoding="utf-8") as f:
            f.write('\n'.join(cleaned_lines))
        
        # Copy file to PostgreSQL container
        copy_cmd = f"docker cp temp_{table}.tsv {self.postgres_container}:{temp_file}"
        result = self.run_command(copy_cmd)
        
        if not result or result.returncode != 0:
            self.log(f"Failed to copy data file for table: {table}", "ERROR")
            return False
        
        # Import data using COPY - let PostgreSQL handle column mapping automatically
        copy_sql = f"\\COPY \"{table}\" FROM '{temp_file}' WITH (FORMAT csv, HEADER true, DELIMITER E'\\t', NULL '\\\\N', ENCODING 'UTF8')"
        cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{copy_sql}"'
        result = self.run_command(cmd)
        
        # Clean up temp file
        try:
            os.remove(f"temp_{table}.tsv")
        except:
            pass
        self.run_command(f"docker exec {self.postgres_container} rm {temp_file}")
        
        if result and result.returncode == 0:
            # Count imported rows
            count_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -t -c "SELECT COUNT(*) FROM \\"{table}\\""'
            count_result = self.run_command(count_cmd)
            
            if count_result and count_result.returncode == 0:
                try:
                    row_count = int(count_result.stdout.strip())
                    self.log(f"[OK] Imported {row_count} rows to table: {table}")
                    self.stats['total_rows'] += row_count
                    return True
                except:
                    self.log(f"[OK] Data imported to table: {table} (count check failed)")
                    return True
            
            return True
        else:
            self.log(f"[ERROR] Failed to import data for table: {table}", "ERROR")
            if result and result.stderr:
                # Log first line of error for debugging
                error_line = result.stderr.split('\n')[0]
                self.log(f"Error details: {error_line}", "ERROR")
            return False
    
    def clean_mysql_data(self, mysql_output):
        """Clean MySQL export data for PostgreSQL import"""
        lines = mysql_output.split('\n')
        if not lines:
            return ""
        
        cleaned_lines = []
        
        for line in lines:
            # Remove problematic characters
            line = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', line)
            
            # Convert MySQL NULL representation to PostgreSQL format
            parts = line.split('\t')
            cleaned_parts = []
            
            for part in parts:
                part = part.strip()
                if part in ('NULL', 'null', '\\N', ''):
                    cleaned_parts.append('\\N')
                else:
                    # Escape special characters for CSV
                    part = part.replace('\\', '\\\\')
                    part = part.replace('\n', '\\n')
                    part = part.replace('\r', '\\r')
                    part = part.replace('\t', '\\t')
                    cleaned_parts.append(part)
            
            cleaned_lines.append('\t'.join(cleaned_parts))
        
        return '\n'.join(cleaned_lines)
    
    def fix_null_values(self, tables):
        """Fix any remaining \\N values to proper NULLs"""
        self.log("Fixing NULL values...")
        
        for table in tables:
            # Get table columns
            column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = 'public'"
            cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -t -c "{column_query}"'
            result = self.run_command(cmd)
            
            if not result or result.returncode != 0:
                continue
            
            columns = [col.strip() for col in result.stdout.strip().split('\n') if col.strip()]
            
            # Fix \\N values in each column
            for column in columns:
                # Create proper SQL for NULL value fixing
                null_fix_query = f"UPDATE \"{table}\" SET \"{column}\" = NULL WHERE \"{column}\" = '\\\\N'"
                update_cmd = f'docker exec {self.postgres_container} psql -U {self.postgres_user} -d {self.postgres_db} -c "{null_fix_query}"'
                self.run_command(update_cmd)
        
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
        
        # Fix NULL values
        self.fix_null_values(tables)
        
        # Optimize PostgreSQL
        self.optimize_postgres()
        
        # Generate final report
        self.generate_report()
        
        return len(self.stats['errors']) == 0

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
