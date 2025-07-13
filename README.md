# MySQL to PostgreSQL Migration Tool for Prisma

A complete, production-ready script that converts any MySQL dump to a Prisma-compatible PostgreSQL database with zero data loss.

## 🚀 Quick Start

```bash
# 1. Place your MySQL dump in the mysql-init directory
cp your-database-dump.sql mysql-init/init.sql

# 2. Run the migration
python mysql-to-postgres-prisma.py

# 3. Access your migrated data
# - Adminer UI: http://localhost:8080
# - PostgreSQL: localhost:5432 (postgres/postgres)
```

## 📋 Prerequisites

- **Docker** and **Docker Compose** installed
- **Python 3.6+**
- MySQL dump file (should contain CREATE TABLE and INSERT statements)

## 🔧 What This Tool Does

1. **Docker Environment Setup**: Creates optimized MySQL and PostgreSQL containers
2. **Schema Conversion**: Converts MySQL DDL to PostgreSQL with Prisma-compatible types
3. **Data Migration**: Exports and imports all data with proper encoding handling
4. **NULL Value Fixing**: Converts MySQL `\N` values to PostgreSQL NULLs
5. **Prisma Optimization**: Configures PostgreSQL for optimal Prisma performance
6. **Web Interface**: Provides Adminer for database inspection

## 📁 File Structure

```
your-project/
├── mysql-init/
│   └── init.sql              # Your MySQL dump file (REQUIRED)
├── mysql-to-postgres-prisma.py  # Migration script
├── docker-compose.yml       # Auto-generated Docker config
└── migration-result.json    # Migration results and connection info
```

## 🎯 Prisma Setup After Migration

1. **Add to your .env file:**
   ```
   DATABASE_URL="postgresql://postgres:postgres@localhost:5432/target_db"
   ```

2. **Pull the schema:**
   ```bash
   npx prisma db pull
   ```

3. **Generate Prisma client:**
   ```bash
   npx prisma generate
   ```

## 🔄 Type Conversions

| MySQL Type | PostgreSQL Type | Prisma Compatible |
|------------|-----------------|-------------------|
| `int(11) auto_increment` | `SERIAL PRIMARY KEY` | ✅ |
| `tinyint(1)` | `BOOLEAN` | ✅ |
| `datetime(3)` | `TIMESTAMP(3)` | ✅ |
| `varchar(255)` | `VARCHAR(255)` | ✅ |
| `text` | `TEXT` | ✅ |
| `enum(...)` | `VARCHAR(50)` | ✅ |
| `json` | `JSONB` | ✅ |

## 🌐 Database Access

### Adminer Web Interface
- **URL**: http://localhost:8080
- **System**: PostgreSQL
- **Server**: postgres_target
- **Username**: postgres
- **Password**: postgres
- **Database**: target_db

### Direct PostgreSQL Connection
- **Host**: localhost
- **Port**: 5432
- **Username**: postgres
- **Password**: postgres
- **Database**: target_db

## 🔍 Troubleshooting

### Migration Fails to Start
```bash
# Check Docker is running
docker --version
docker-compose --version

# Ensure MySQL dump exists
ls -la mysql-init/init.sql
```

### Database Connection Issues
```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs mysql_source
docker-compose logs postgres_target
```

### Data Import Errors
- Ensure your MySQL dump contains both schema and data
- Check for unsupported MySQL features (stored procedures, triggers)
- Verify character encoding is UTF-8

## 🧹 Cleanup

To stop and remove all containers:
```bash
docker-compose down -v
```

To remove all migration files:
```bash
rm docker-compose.yml migration-result.json
```

## 📊 Migration Report

After migration, check `migration-result.json` for:
- Connection details
- Migration statistics
- Error reports (if any)

## 🛡️ Production Notes

- **Data Safety**: This tool preserves all data with zero-loss guarantee
- **Performance**: Optimized for large datasets with batch processing
- **Compatibility**: Works with any standard MySQL dump file
- **Prisma Ready**: All type conversions are Prisma-compatible

## 📝 Example Output

```
🚀 MySQL to PostgreSQL + Prisma Migration Tool
================================================================
[09:15:23] INFO: Checking prerequisites...
[09:15:24] INFO: ✅ All prerequisites met
[09:15:25] INFO: Creating Docker Compose configuration...
[09:15:26] INFO: ✅ Docker Compose configuration created
[09:15:27] INFO: Setting up Docker environment...
[09:15:45] INFO: ✅ Docker environment ready
[09:15:46] INFO: Getting MySQL table list...
[09:15:47] INFO: Found 8 tables: users, posts, comments, categories, tags...
[09:15:48] INFO: Creating PostgreSQL schema...
[09:15:52] INFO: ✅ Created table: users
[09:15:53] INFO: ✅ Created table: posts
[09:15:58] INFO: Migrating table data...
[09:16:05] INFO: ✅ Imported 1,547 rows to table: users
[09:16:12] INFO: ✅ Imported 3,291 rows to table: posts
[09:16:15] INFO: ✅ Migration completed successfully!

╔══════════════════════════════════════════════════════════════╗
║                    MIGRATION COMPLETED                      ║
╠══════════════════════════════════════════════════════════════╣
║ Duration: 0:02:45                                          ║
║ Tables Created: 8                                          ║
║ Tables Migrated: 8                                         ║
║ Total Rows: 12,847                                         ║
╠══════════════════════════════════════════════════════════════╣
║                     ACCESS YOUR DATA                        ║
╠══════════════════════════════════════════════════════════════╣
║ 🌐 Adminer Web UI: http://localhost:8080                   ║
║ 🐘 PostgreSQL: localhost:5432                              ║
║ 👤 Username: postgres                                      ║
║ 🔑 Password: postgres                                      ║
║ 🗄️  Database: target_db                                   ║
╠══════════════════════════════════════════════════════════════╣
║                    PRISMA SETUP                             ║
╠══════════════════════════════════════════════════════════════╣
║ DATABASE_URL="postgresql://postgres:postgres@localhost:5432/target_db"  ║
║ npx prisma db pull                                          ║
║ npx prisma generate                                         ║
╚══════════════════════════════════════════════════════════════╝
```

---

**Ready for production!** 🎉 Your MySQL database has been successfully migrated to PostgreSQL and is ready for Prisma.
