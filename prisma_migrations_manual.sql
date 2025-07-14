-- INSERT statements for _prisma_migrations
INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) 
VALUES ('02796cc7-a3c3-4704-bcd8-4bdb412741fe', 'e69c9f21be2b53770b13ea52bf6c4f304a9fc86b41f1e932729ec2de45574341', TIMESTAMP '2025-02-01 16:26:08.942', '0_init', NULL, NULL, TIMESTAMP '2025-02-01 16:26:07.355', 1);

INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) 
VALUES ('4fb47a21-ebc9-45f5-89c4-317b97ebbce0', 'c3c13771ecc008137c104a4fb3ba5685788723f86efa9c23f6189d2a3641e00d', TIMESTAMP '2025-02-01 16:26:54.533', '20250201162635_add_test_field', NULL, NULL, TIMESTAMP '2025-02-01 16:26:36.370', 1);

INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) 
VALUES ('bf337f8f-e9da-4688-8576-52eeb1602741', '379f857fed830a45b0349650c3edb79183c9fe076c9fdb465de120d5d104ee06', TIMESTAMP '2025-02-01 16:34:16.672', '20250201163413_add_another_test_field', NULL, NULL, TIMESTAMP '2025-02-01 16:34:14.831', 1);

INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) 
VALUES ('e2fd6b31-833a-4648-8c0c-aad9c4628c10', '8091bb4eb561069a968cfaa50016b6f053abefc64d12079eaced1eb16dc70ade', TIMESTAMP '2025-02-01 16:36:46.630', '20250201163644_remove_test_fields', NULL, NULL, TIMESTAMP '2025-02-01 16:36:45.109', 1);

INSERT INTO "_prisma_migrations" (id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count) 
VALUES ('5918aaae-aaa7-472e-9f67-45bc1e9baf89', 'bf4862df6685faba2b587ea7f200fcd181a86c6d5d5b560efb7b1f167a95475a', NULL, '20250202094550_add_first_contact_time_to_client_table', 'A migration failed to apply. New migrations cannot be applied before the error is recovered from. Read more about how to resolve migration issues in a production database: https://pris.ly/d/migrate-resolve

Migration name: 20250202094550_add_first_contact_time_to_client_table

Database error code: 1146

Database error:
Table ''autoworx-production-1.client'' doesn''t exist

Please check the query number 1 from the migration file.

   0: sql_schema_connector::apply_migration::apply_script
           with migration_name="20250202094550_add_first_contact_time_to_client_table"
             at schema-engine\connectors\sql-schema-connector\src\apply_migration.rs:106
   1: schema_core::commands::apply_migrations::Applying migration
           with migration_name="20250202094550_add_first_contact_time_to_client_table"
             at schema-engine\core\src\commands\apply_migrations.rs:91
   2: schema_core::state::ApplyMigrations
             at schema-engine\core\src\state.rs:226', NULL, TIMESTAMP '2025-02-02 10:40:22.775', 0);
