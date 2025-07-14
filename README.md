# MySQL to PostgreSQL Migration Scripts

This project provides dedicated migration scripts for migrating MySQL tables to PostgreSQL with full preservation of MySQL case-sensitive naming and original ID values.

##  Features

- **Case Preservation**: Maintains MySQL case-sensitive table and column names in PostgreSQL
- **ID Preservation**: Preserves original MySQL ID values with proper auto-increment setup
- **3-Phase Migration**: Structure → Indexes → Foreign Keys for optimal performance
- **Referential Integrity**: Handles foreign key dependencies correctly
- **Robust Error Handling**: Comprehensive error checking and verification

## 📁 Project Structure

```
├── docker-compose.yml              # MySQL and PostgreSQL containers
├── mysql-init/
│   └── init.sql                   # Sample database initialization
├── table_utils.py                 # Shared migration utilities
├── company_migration.py           # Company table migration
├── user_migration.py              # User table migration  
├── appointment_migration.py       # Appointment table migration
└── verify_table_structure.py      # Structure verification tool
```

## 🛠️ Core Components

### Migration Scripts
- **`company_migration.py`** - Migrates Company table (independent, no dependencies)
- **`user_migration.py`** - Migrates User table (depends on Company)
- **`appointment_migration.py`** - Migrates Appointment table (depends on Company, User)

### Utilities
- **`table_utils.py`** - Shared functions for DDL conversion, data transfer, and verification
- **`verify_table_structure.py`** - Compare table structures between MySQL and PostgreSQL

##  Usage

### 1. Start Docker Containers
```bash
docker-compose up -d
```

### 2. Run 3-Phase Migration

Each table supports 3 phases:
- **Phase 1**: Create table structure and import data
- **Phase 2**: Create indexes (for performance)
- **Phase 3**: Create foreign keys (after dependencies exist)

#### Company Table (no dependencies)
```bash
python company_migration.py --phase=1   # Structure + Data
python company_migration.py --phase=2   # Indexes
python company_migration.py --phase=3   # Foreign Keys
```

#### User Table (depends on Company)
```bash
python user_migration.py --phase=1      # Structure + Data
python user_migration.py --phase=2      # Indexes
python user_migration.py --phase=3      # Foreign Keys (requires Company)
```

#### Appointment Table (depends on Company, User)
```bash
python appointment_migration.py --phase=1  # Structure + Data
python appointment_migration.py --phase=2  # Indexes
python appointment_migration.py --phase=3  # Foreign Keys (requires Company, User)
```

### 3. Full Migration (All Phases)
```bash
python company_migration.py --full
python user_migration.py --full
python appointment_migration.py --full
```

### 4. Verification
```bash
python company_migration.py --verify
python user_migration.py --verify
python appointment_migration.py --verify
```

## ✨ Key Features

### ID Preservation with Auto-Increment
- Preserves original MySQL IDs exactly (e.g., 1,3,4,5,7,8...30)
- Sets up PostgreSQL sequences to start from MAX(id) + 1
- New records get proper sequential IDs (31, 32, 33...)

### Case-Sensitive Naming
- Table: `Company` → `"Company"` (quoted for case preservation)
- Columns: `firstName` → `"firstName"` (maintains camelCase)

### Foreign Key Support
- Handles complex dependency chains
- Validates referenced tables exist before creating constraints
- Preserves MySQL referential actions (CASCADE, RESTRICT, etc.)

##  Migration Process

1. **DDL Conversion**: Converts MySQL CREATE TABLE to PostgreSQL syntax
2. **Data Export**: Extracts data from MySQL with proper encoding
3. **Structure Creation**: Creates PostgreSQL table with case-sensitive names
4. **Data Import**: Imports data preserving original IDs
5. **Sequence Setup**: Creates auto-increment sequence from MAX(id) + 1
6. **Index Creation**: Creates indexes matching MySQL structure
7. **Foreign Key Creation**: Adds referential integrity constraints

##  Supported Data Types

| MySQL | PostgreSQL | Notes |
|-------|------------|-------|
| `int` | `INTEGER` | With preserved IDs |
| `varchar(n)` | `VARCHAR` | Size constraints removed |
| `decimal(m,n)` | `DECIMAL` | Precision preserved |
| `datetime(n)` | `TIMESTAMP` | Without timezone |
| `enum(...)` | `VARCHAR(50)` | Enum converted to varchar |
| `tinyint(1)` | `BOOLEAN` | MySQL boolean equivalent |

##  Migration Status

| Table | Structure | Data | Indexes | Foreign Keys | Status |
|-------|-----------|------|---------|--------------|---------|
| Company |  |  (27 records) |  (4 indexes) |  | Complete |
| User |  |  (151 records) |  (3 indexes) |  | Complete |
| Appointment |  |  (329 records) |  |  | Complete |

## 🐳 Docker Environment

- **MySQL**: `mysql_source` container with source database
- **PostgreSQL**: `postgres_target` container with target database
- **Networking**: Containers communicate via Docker network

## 🚨 Important Notes

- Run migrations in dependency order: Company → User → Appointment
- Foreign keys are created in Phase 3 after all referenced tables exist
- Original MySQL IDs are preserved exactly
- Case-sensitive naming requires quoted identifiers in PostgreSQL
- Auto-increment sequences start from MAX(existing_id) + 1

##  Verification

Use the verification tools to ensure migration success:
```bash
python verify_table_structure.py Company
python verify_table_structure.py User
python verify_table_structure.py Appointment
```

Verification checks:
-  Table existence
-  Column count and names
-  Data type compatibility
-  Record count matching
-  Sample data comparison
