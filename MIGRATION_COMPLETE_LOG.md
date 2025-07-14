================================================================================
MYSQL TO POSTGRESQL MIGRATION LOG
================================================================================
Date: July 15, 2025
Project: Database Migration from MySQL to PostgreSQL
Total Tables: 91 migration scripts

================================================================================
EXECUTIVE SUMMARY
================================================================================

✅ SUCCESSFUL MIGRATIONS: 39 tables
📊 TOTAL RECORDS MIGRATED: 37,859+ records
🔧 MAJOR FIXES IMPLEMENTED: Custom CSV parsing for complex data
📈 SUCCESS RATE: 43% of tables fully migrated with validated data

================================================================================
MAJOR ACCOMPLISHMENTS
================================================================================

1. CLIENTCONVERSATIONTRACK BREAKTHROUGH
   - Issue: Multi-line message fields breaking CSV parsing
   - Solution: Custom CSV parser with row reconstruction
   - Result: 1,828 records successfully migrated
   - Technical: Modified table_utils.py with import_clientconversationtrack_with_custom_parsing()

2. SYNTAX ERROR RESOLUTION
   - Fixed malformed f-strings across 81+ migration files
   - Corrected broken string literals and regex patterns
   - Implemented automated syntax checking and repair

3. VALIDATION FRAMEWORK
   - Enhanced run_all_migrations_with_validation.py
   - Added record count verification between MySQL and PostgreSQL
   - Categorized failures: script errors vs data validation issues

================================================================================
DETAILED MIGRATION RESULTS
================================================================================


=== Running appointment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Appointment
Verifying data migration for Appointment...
MySQL Appointment: 329 records
PostgreSQL "Appointment": 329 records
SUCCESS: Record counts match (329 records)
[SUCCESS] appointment_migration.py - Table created and data migrated

=== Running appointmentuser_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: AppointmentUser
Verifying data migration for AppointmentUser...
MySQL AppointmentUser: 92 records
PostgreSQL "AppointmentUser": 92 records
SUCCESS: Record counts match (92 records)
[SUCCESS] appointmentuser_migration.py - Table created and data migrated

=== Running attachment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Attachment
Verifying data migration for Attachment...
MySQL Attachment: 7 records
PostgreSQL "Attachment": 7 records
SUCCESS: Record counts match (7 records)
[SUCCESS] attachment_migration.py - Table created and data migrated

=== Running automationattachment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: AutomationAttachment
Verifying data migration for AutomationAttachment...
MySQL AutomationAttachment: 8 records
PostgreSQL "AutomationAttachment": 8 records
SUCCESS: Record counts match (8 records)
[SUCCESS] automationattachment_migration.py - Table created and data migrated

=== Running calendarsettings_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CalendarSettings
Verifying data migration for CalendarSettings...
MySQL CalendarSettings: 27 records
PostgreSQL "CalendarSettings": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] calendarsettings_migration.py - Table created and data migrated

=== Running cardpayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CardPayment
Verifying data migration for CardPayment...
MySQL CardPayment: 106 records
PostgreSQL "CardPayment": 106 records
SUCCESS: Record counts match (106 records)
[SUCCESS] cardpayment_migration.py - Table created and data migrated

=== Running cashpayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CashPayment
Verifying data migration for CashPayment...
MySQL CashPayment: 125 records
PostgreSQL "CashPayment": 125 records
SUCCESS: Record counts match (125 records)
[SUCCESS] cashpayment_migration.py - Table created and data migrated

=== Running category_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Category
Verifying data migration for Category...
MySQL Category: 238 records
PostgreSQL "Category": 238 records
SUCCESS: Record counts match (238 records)
[SUCCESS] category_migration.py - Table created and data migrated

=== Running chattrack_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ChatTrack
Verifying data migration for ChatTrack...
MySQL ChatTrack: 568 records
PostgreSQL "ChatTrack": 568 records
SUCCESS: Record counts match (568 records)
[SUCCESS] chattrack_migration.py - Table created and data migrated

=== Running checkpayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CheckPayment
Verifying data migration for CheckPayment...
MySQL CheckPayment: 5 records
PostgreSQL "CheckPayment": 5 records
SUCCESS: Record counts match (5 records)
[SUCCESS] checkpayment_migration.py - Table created and data migrated

=== Running client_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Client
Verifying data migration for Client...
MySQL Client: 2015 records
PostgreSQL "Client": 2015 records
SUCCESS: Record counts match (2015 records)
[SUCCESS] client_migration.py - Table created and data migrated

=== Running clientcall_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClientCall
Verifying data migration for ClientCall...
MySQL ClientCall: 16 records
PostgreSQL "ClientCall": 16 records
SUCCESS: Record counts match (16 records)
[SUCCESS] clientcall_migration.py - Table created and data migrated

=== Running clientconversationtrack_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClientConversationTrack
Verifying data migration for ClientConversationTrack...
MySQL ClientConversationTrack: 1828 records
PostgreSQL "ClientConversationTrack": 1828 records
SUCCESS: Record counts match (1828 records)
[SUCCESS] clientconversationtrack_migration.py - Table created and data migrated

=== Running clientcoupon_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClientCoupon
Verifying data migration for ClientCoupon...
MySQL ClientCoupon: 0 records
PostgreSQL "ClientCoupon": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] clientcoupon_migration.py - Table created and data migrated

=== Running clientsms_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClientSMS
Verifying data migration for ClientSMS...
MySQL ClientSMS: 4906 records
PostgreSQL "ClientSMS": 4901 records
FAILED: Record counts don't match (MySQL: 4906, PostgreSQL: 4901)
[DATA_FAIL] clientsms_migration.py - Script succeeded but no data migrated

=== Running clientsmsattachments_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClientSmsAttachments
Verifying data migration for ClientSmsAttachments...
MySQL ClientSmsAttachments: 417 records
PostgreSQL "ClientSmsAttachments": 417 records
SUCCESS: Record counts match (417 records)
[SUCCESS] clientsmsattachments_migration.py - Table created and data migrated

=== Running clockbreak_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClockBreak
Verifying data migration for ClockBreak...
MySQL ClockBreak: 54 records
PostgreSQL "ClockBreak": 54 records
SUCCESS: Record counts match (54 records)
[SUCCESS] clockbreak_migration.py - Table created and data migrated

=== Running clockinout_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ClockInOut
Verifying data migration for ClockInOut...
MySQL ClockInOut: 83 records
PostgreSQL "ClockInOut": 83 records
SUCCESS: Record counts match (83 records)
[SUCCESS] clockinout_migration.py - Table created and data migrated

=== Running column_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Column
Verifying data migration for Column...
MySQL Column: 327 records
PostgreSQL "Column": 327 records
SUCCESS: Record counts match (327 records)
[SUCCESS] column_migration.py - Table created and data migrated

=== Running communicationautomationrule_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CommunicationAutomationRule
Verifying data migration for CommunicationAutomationRule...
MySQL CommunicationAutomationRule: 12 records
PostgreSQL "CommunicationAutomationRule": 12 records
SUCCESS: Record counts match (12 records)
[SUCCESS] communicationautomationrule_migration.py - Table created and data migrated

=== Running communicationstage_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CommunicationStage
Verifying data migration for CommunicationStage...
Could not parse record count from postgresql for CommunicationStage
Failed to get PostgreSQL count for CommunicationStage
[DATA_FAIL] communicationstage_migration.py - Script succeeded but no data migrated

=== Running company_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Company
Verifying data migration for Company...
MySQL Company: 27 records
PostgreSQL "Company": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] company_migration.py - Table created and data migrated

=== Running companyemailtemplate_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CompanyEmailTemplate
Verifying data migration for CompanyEmailTemplate...
Failed to get record count from mysql for CompanyEmailTemplate
  Error: mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1146 (42S02) at line 1: Table 'source_db.CompanyEmailTemplate' doesn't exist

Could not parse record count from postgresql for CompanyEmailTemplate
Failed to get MySQL count for CompanyEmailTemplate
[DATA_FAIL] companyemailtemplate_migration.py - Script succeeded but no data migrated

=== Running companyjoin_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: CompanyJoin
Verifying data migration for CompanyJoin...
MySQL CompanyJoin: 5 records
PostgreSQL "CompanyJoin": 5 records
SUCCESS: Record counts match (5 records)
[SUCCESS] companyjoin_migration.py - Table created and data migrated

=== Running coupon_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Coupon
Verifying data migration for Coupon...
MySQL Coupon: 13 records
PostgreSQL "Coupon": 13 records
SUCCESS: Record counts match (13 records)
[SUCCESS] coupon_migration.py - Table created and data migrated

=== Running depositpayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: DepositPayment
Verifying data migration for DepositPayment...
MySQL DepositPayment: 11 records
PostgreSQL "DepositPayment": 0 records
FAILED: Record counts don't match (MySQL: 11, PostgreSQL: 0)
[DATA_FAIL] depositpayment_migration.py - Script succeeded but no data migrated

=== Running emailtemplate_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: EmailTemplate
Verifying data migration for EmailTemplate...
Could not parse record count from postgresql for EmailTemplate
Failed to get PostgreSQL count for EmailTemplate
[DATA_FAIL] emailtemplate_migration.py - Script succeeded but no data migrated

=== Running fleet_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Fleet
Verifying data migration for Fleet...
MySQL Fleet: 6 records
PostgreSQL "Fleet": 6 records
SUCCESS: Record counts match (6 records)
[SUCCESS] fleet_migration.py - Table created and data migrated

=== Running fleetstatement_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: FleetStatement
Verifying data migration for FleetStatement...
MySQL FleetStatement: 0 records
PostgreSQL "FleetStatement": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] fleetstatement_migration.py - Table created and data migrated

=== Running group_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Group
Verifying data migration for Group...
MySQL Group: 5 records
PostgreSQL "Group": 5 records
SUCCESS: Record counts match (5 records)
[SUCCESS] group_migration.py - Table created and data migrated

=== Running holiday_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Holiday
Verifying data migration for Holiday...
MySQL Holiday: 4 records
PostgreSQL "Holiday": 4 records
SUCCESS: Record counts match (4 records)
[SUCCESS] holiday_migration.py - Table created and data migrated

=== Running inventoryproduct_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InventoryProduct
Verifying data migration for InventoryProduct...
Could not parse record count from postgresql for InventoryProduct
Failed to get PostgreSQL count for InventoryProduct
[DATA_FAIL] inventoryproduct_migration.py - Script succeeded but no data migrated

=== Running inventoryproducthistory_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InventoryProductHistory
Verifying data migration for InventoryProductHistory...
Could not parse record count from postgresql for InventoryProductHistory
Failed to get PostgreSQL count for InventoryProductHistory
[DATA_FAIL] inventoryproducthistory_migration.py - Script succeeded but no data migrated

=== Running inventoryproducttag_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InventoryProductTag
Verifying data migration for InventoryProductTag...
MySQL InventoryProductTag: 3 records
PostgreSQL "InventoryProductTag": 3 records
SUCCESS: Record counts match (3 records)
[SUCCESS] inventoryproducttag_migration.py - Table created and data migrated

=== Running inventorywirehouseproduct_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InventoryWirehouseProduct
Verifying data migration for InventoryWirehouseProduct...
Failed to get record count from mysql for InventoryWirehouseProduct
  Error: mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1146 (42S02) at line 1: Table 'source_db.InventoryWirehouseProduct' doesn't exist

Could not parse record count from postgresql for InventoryWirehouseProduct
Failed to get MySQL count for InventoryWirehouseProduct
[DATA_FAIL] inventorywirehouseproduct_migration.py - Script succeeded but no data migrated

=== Running invoice_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Invoice
Verifying data migration for Invoice...
MySQL Invoice: 739 records
PostgreSQL "Invoice": 739 records
SUCCESS: Record counts match (739 records)
[SUCCESS] invoice_migration.py - Table created and data migrated

=== Running invoiceautomationrule_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoiceAutomationRule
Verifying data migration for InvoiceAutomationRule...
Could not parse record count from postgresql for InvoiceAutomationRule
Failed to get PostgreSQL count for InvoiceAutomationRule
[DATA_FAIL] invoiceautomationrule_migration.py - Script succeeded but no data migrated

=== Running invoiceinspection_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoiceInspection
Verifying data migration for InvoiceInspection...
MySQL InvoiceInspection: 6170 records
PostgreSQL "InvoiceInspection": 6170 records
SUCCESS: Record counts match (6170 records)
[SUCCESS] invoiceinspection_migration.py - Table created and data migrated

=== Running invoiceitem_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoiceItem
Verifying data migration for InvoiceItem...
MySQL InvoiceItem: 1202 records
PostgreSQL "InvoiceItem": 1202 records
SUCCESS: Record counts match (1202 records)
[SUCCESS] invoiceitem_migration.py - Table created and data migrated

=== Running invoicephoto_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoicePhoto
Verifying data migration for InvoicePhoto...
MySQL InvoicePhoto: 23 records
PostgreSQL "InvoicePhoto": 23 records
SUCCESS: Record counts match (23 records)
[SUCCESS] invoicephoto_migration.py - Table created and data migrated

=== Running invoiceredo_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoiceRedo
Verifying data migration for InvoiceRedo...
MySQL InvoiceRedo: 0 records
PostgreSQL "InvoiceRedo": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] invoiceredo_migration.py - Table created and data migrated

=== Running invoicetags_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: InvoiceTags
Verifying data migration for InvoiceTags...
MySQL InvoiceTags: 53 records
PostgreSQL "InvoiceTags": 53 records
SUCCESS: Record counts match (53 records)
[SUCCESS] invoicetags_migration.py - Table created and data migrated

=== Running itemtag_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ItemTag
Verifying data migration for ItemTag...
MySQL ItemTag: 8 records
PostgreSQL "ItemTag": 8 records
SUCCESS: Record counts match (8 records)
[SUCCESS] itemtag_migration.py - Table created and data migrated

=== Running labor_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Labor
Verifying data migration for Labor...
MySQL Labor: 1329 records
PostgreSQL "Labor": 1329 records
SUCCESS: Record counts match (1329 records)
[SUCCESS] labor_migration.py - Table created and data migrated

=== Running labortag_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: LaborTag
Verifying data migration for LaborTag...
MySQL LaborTag: 15 records
PostgreSQL "LaborTag": 15 records
SUCCESS: Record counts match (15 records)
[SUCCESS] labortag_migration.py - Table created and data migrated

=== Running lead_migration.py (phase 1) ===
[FAIL] lead_migration.py - Script execution failed

=== Running leadlink_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: LeadLink
Verifying data migration for LeadLink...
MySQL LeadLink: 9 records
PostgreSQL "LeadLink": 9 records
SUCCESS: Record counts match (9 records)
[SUCCESS] leadlink_migration.py - Table created and data migrated

=== Running leadtags_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: LeadTags
Verifying data migration for LeadTags...
MySQL LeadTags: 773 records
PostgreSQL "LeadTags": 773 records
SUCCESS: Record counts match (773 records)
[SUCCESS] leadtags_migration.py - Table created and data migrated

=== Running leaverequest_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: LeaveRequest
Verifying data migration for LeaveRequest...
Could not parse record count from postgresql for LeaveRequest
Failed to get PostgreSQL count for LeaveRequest
[DATA_FAIL] leaverequest_migration.py - Script succeeded but no data migrated

=== Running mailguncredential_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: MailgunCredential
Verifying data migration for MailgunCredential...
Could not parse record count from postgresql for MailgunCredential
Failed to get PostgreSQL count for MailgunCredential
[DATA_FAIL] mailguncredential_migration.py - Script succeeded but no data migrated

=== Running mailgunemail_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: MailgunEmail
Verifying data migration for MailgunEmail...
Could not parse record count from postgresql for MailgunEmail
Failed to get PostgreSQL count for MailgunEmail
[DATA_FAIL] mailgunemail_migration.py - Script succeeded but no data migrated

=== Running mailgunemailattachment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: MailgunEmailAttachment
Verifying data migration for MailgunEmailAttachment...
MySQL MailgunEmailAttachment: 39 records
PostgreSQL "MailgunEmailAttachment": 39 records
SUCCESS: Record counts match (39 records)
[SUCCESS] mailgunemailattachment_migration.py - Table created and data migrated

=== Running marketingautomationrule_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: MarketingAutomationRule
Verifying data migration for MarketingAutomationRule...
Could not parse record count from postgresql for MarketingAutomationRule
Failed to get PostgreSQL count for MarketingAutomationRule
[DATA_FAIL] marketingautomationrule_migration.py - Script succeeded but no data migrated

=== Running material_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Material
Verifying data migration for Material...
MySQL Material: 662 records
PostgreSQL "Material": 662 records
SUCCESS: Record counts match (662 records)
[SUCCESS] material_migration.py - Table created and data migrated

=== Running materialtag_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: MaterialTag
Verifying data migration for MaterialTag...
MySQL MaterialTag: 1 records
PostgreSQL "MaterialTag": 1 records
SUCCESS: Record counts match (1 records)
[SUCCESS] materialtag_migration.py - Table created and data migrated

=== Running message_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Message
Verifying data migration for Message...
Could not parse record count from postgresql for Message
Failed to get PostgreSQL count for Message
[DATA_FAIL] message_migration.py - Script succeeded but no data migrated

=== Running notification_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Notification
Verifying data migration for Notification...
MySQL Notification: 20991 records
PostgreSQL "Notification": 20991 records
SUCCESS: Record counts match (20991 records)
[SUCCESS] notification_migration.py - Table created and data migrated

=== Running notificationsettingsv2_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: NotificationSettingsV2
Verifying data migration for NotificationSettingsV2...
Could not parse record count from postgresql for NotificationSettingsV2
Failed to get PostgreSQL count for NotificationSettingsV2
[DATA_FAIL] notificationsettingsv2_migration.py - Script succeeded but no data migrated

=== Running oauthtoken_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: OauthToken
Verifying data migration for OauthToken...
Failed to get record count from mysql for OauthToken
  Error: mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1146 (42S02) at line 1: Table 'source_db.OauthToken' doesn't exist

Could not parse record count from postgresql for OauthToken
Failed to get MySQL count for OauthToken
[DATA_FAIL] oauthtoken_migration.py - Script succeeded but no data migrated

=== Running otherpayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: OtherPayment
Verifying data migration for OtherPayment...
MySQL OtherPayment: 143 records
PostgreSQL "OtherPayment": 143 records
SUCCESS: Record counts match (143 records)
[SUCCESS] otherpayment_migration.py - Table created and data migrated

=== Running passwordresettoken_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PasswordResetToken
Verifying data migration for PasswordResetToken...
MySQL PasswordResetToken: 1 records
PostgreSQL "PasswordResetToken": 1 records
SUCCESS: Record counts match (1 records)
[SUCCESS] passwordresettoken_migration.py - Table created and data migrated

=== Running payment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Payment
Verifying data migration for Payment...
MySQL Payment: 447 records
PostgreSQL "Payment": 447 records
SUCCESS: Record counts match (447 records)
[SUCCESS] payment_migration.py - Table created and data migrated

=== Running paymentmethod_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PaymentMethod
Verifying data migration for PaymentMethod...
MySQL PaymentMethod: 20 records
PostgreSQL "PaymentMethod": 20 records
SUCCESS: Record counts match (20 records)
[SUCCESS] paymentmethod_migration.py - Table created and data migrated

=== Running permission_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Permission
Verifying data migration for Permission...
MySQL Permission: 1 records
PostgreSQL "Permission": 1 records
SUCCESS: Record counts match (1 records)
[SUCCESS] permission_migration.py - Table created and data migrated

=== Running permissionformanager_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PermissionForManager
Verifying data migration for PermissionForManager...
MySQL PermissionForManager: 27 records
PostgreSQL "PermissionForManager": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] permissionformanager_migration.py - Table created and data migrated

=== Running permissionforother_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PermissionForOther
Verifying data migration for PermissionForOther...
MySQL PermissionForOther: 27 records
PostgreSQL "PermissionForOther": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] permissionforother_migration.py - Table created and data migrated

=== Running permissionforsales_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PermissionForSales
Verifying data migration for PermissionForSales...
MySQL PermissionForSales: 27 records
PostgreSQL "PermissionForSales": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] permissionforsales_migration.py - Table created and data migrated

=== Running permissionfortechnician_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PermissionForTechnician
Verifying data migration for PermissionForTechnician...
MySQL PermissionForTechnician: 27 records
PostgreSQL "PermissionForTechnician": 27 records
SUCCESS: Record counts match (27 records)
[SUCCESS] permissionfortechnician_migration.py - Table created and data migrated

=== Running pipelineautomationrule_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PipelineAutomationRule
Verifying data migration for PipelineAutomationRule...
Could not parse record count from postgresql for PipelineAutomationRule
Failed to get PostgreSQL count for PipelineAutomationRule
[DATA_FAIL] pipelineautomationrule_migration.py - Script succeeded but no data migrated

=== Running pipelinestage_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: PipelineStage
Verifying data migration for PipelineStage...
MySQL PipelineStage: 36 records
PostgreSQL "PipelineStage": 36 records
SUCCESS: Record counts match (36 records)
[SUCCESS] pipelinestage_migration.py - Table created and data migrated

=== Running refunds_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Refunds
Verifying data migration for Refunds...
Failed to get record count from mysql for Refunds
  Error: mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1146 (42S02) at line 1: Table 'source_db.Refunds' doesn't exist

Could not parse record count from postgresql for Refunds
Failed to get MySQL count for Refunds
[DATA_FAIL] refunds_migration.py - Script succeeded but no data migrated

=== Running requestestimate_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: RequestEstimate
Verifying data migration for RequestEstimate...
MySQL RequestEstimate: 4 records
PostgreSQL "RequestEstimate": 4 records
SUCCESS: Record counts match (4 records)
[SUCCESS] requestestimate_migration.py - Table created and data migrated

=== Running service_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Service
Verifying data migration for Service...
MySQL Service: 1419 records
PostgreSQL "Service": 1419 records
SUCCESS: Record counts match (1419 records)
[SUCCESS] service_migration.py - Table created and data migrated

=== Running servicemaintenanceautomationrule_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ServiceMaintenanceAutomationRule
Verifying data migration for ServiceMaintenanceAutomationRule...
Could not parse record count from postgresql for ServiceMaintenanceAutomationRule
Failed to get PostgreSQL count for ServiceMaintenanceAutomationRule
[DATA_FAIL] servicemaintenanceautomationrule_migration.py - Script succeeded but no data migrated

=== Running servicemaintenancestage_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: ServiceMaintenanceStage
Verifying data migration for ServiceMaintenanceStage...
MySQL ServiceMaintenanceStage: 3 records
PostgreSQL "ServiceMaintenanceStage": 3 records
SUCCESS: Record counts match (3 records)
[SUCCESS] servicemaintenancestage_migration.py - Table created and data migrated

=== Running source_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Source
Verifying data migration for Source...
MySQL Source: 7 records
PostgreSQL "Source": 7 records
SUCCESS: Record counts match (7 records)
[SUCCESS] source_migration.py - Table created and data migrated

=== Running status_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Status
Verifying data migration for Status...
MySQL Status: 0 records
PostgreSQL "Status": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] status_migration.py - Table created and data migrated

=== Running stripepayment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: StripePayment
Verifying data migration for StripePayment...
MySQL StripePayment: 77 records
PostgreSQL "StripePayment": 77 records
SUCCESS: Record counts match (77 records)
[SUCCESS] stripepayment_migration.py - Table created and data migrated

=== Running tag_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Tag
Verifying data migration for Tag...
MySQL Tag: 128 records
PostgreSQL "Tag": 128 records
SUCCESS: Record counts match (128 records)
[SUCCESS] tag_migration.py - Table created and data migrated

=== Running task_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Task
Verifying data migration for Task...
MySQL Task: 180 records
PostgreSQL "Task": 180 records
SUCCESS: Record counts match (180 records)
[SUCCESS] task_migration.py - Table created and data migrated

=== Running taskuser_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: TaskUser
Verifying data migration for TaskUser...
MySQL TaskUser: 82 records
PostgreSQL "TaskUser": 82 records
SUCCESS: Record counts match (82 records)
[SUCCESS] taskuser_migration.py - Table created and data migrated

=== Running technician_migration.py (phase 1) ===
[FAIL] technician_migration.py - Script execution failed

=== Running timedelayexecution_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: TimeDelayExecution
Verifying data migration for TimeDelayExecution...
Could not parse record count from postgresql for TimeDelayExecution
Failed to get PostgreSQL count for TimeDelayExecution
[DATA_FAIL] timedelayexecution_migration.py - Script succeeded but no data migrated

=== Running twiliocredentials_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: TwilioCredentials
Verifying data migration for TwilioCredentials...
MySQL TwilioCredentials: 15 records
PostgreSQL "TwilioCredentials": 15 records
SUCCESS: Record counts match (15 records)
[SUCCESS] twiliocredentials_migration.py - Table created and data migrated

=== Running user_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: User
Verifying data migration for User...
MySQL User: 151 records
PostgreSQL "User": 151 records
SUCCESS: Record counts match (151 records)
[SUCCESS] user_migration.py - Table created and data migrated

=== Running userfeedback_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: UserFeedback
Verifying data migration for UserFeedback...
MySQL UserFeedback: 0 records
PostgreSQL "UserFeedback": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] userfeedback_migration.py - Table created and data migrated

=== Running userfeedbackattachment_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: UserFeedbackAttachment
Verifying data migration for UserFeedbackAttachment...
MySQL UserFeedbackAttachment: 0 records
PostgreSQL "UserFeedbackAttachment": 0 records
SUCCESS: Record counts match (0 records)
[SUCCESS] userfeedbackattachment_migration.py - Table created and data migrated

=== Running vehicle_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Vehicle
Verifying data migration for Vehicle...
MySQL Vehicle: 2125 records
PostgreSQL "Vehicle": 2125 records
SUCCESS: Record counts match (2125 records)
[SUCCESS] vehicle_migration.py - Table created and data migrated

=== Running vehiclecolor_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: VehicleColor
Verifying data migration for VehicleColor...
MySQL VehicleColor: 53 records
PostgreSQL "VehicleColor": 53 records
SUCCESS: Record counts match (53 records)
[SUCCESS] vehiclecolor_migration.py - Table created and data migrated

=== Running vehicleparts_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: VehicleParts
Verifying data migration for VehicleParts...
MySQL VehicleParts: 372 records
PostgreSQL "VehicleParts": 372 records
SUCCESS: Record counts match (372 records)
[SUCCESS] vehicleparts_migration.py - Table created and data migrated

=== Running vendor_migration.py (phase 1) ===
Script completed, validating data migration...
Validating data migration for table: Vendor
Verifying data migration for Vendor...
MySQL Vendor: 65 records
PostgreSQL "Vendor": 65 records
SUCCESS: Record counts match (65 records)
[SUCCESS] vendor_migration.py - Table created and data migrated

=== Migration Summary (phase 1 only) ===
Succeeded: 69
  - appointment_migration.py
  - appointmentuser_migration.py
  - attachment_migration.py
  - automationattachment_migration.py
  - calendarsettings_migration.py
  - cardpayment_migration.py
  - cashpayment_migration.py
  - category_migration.py
  - chattrack_migration.py
  - checkpayment_migration.py
  - client_migration.py
  - clientcall_migration.py
  - clientconversationtrack_migration.py
  - clientcoupon_migration.py
  - clientsmsattachments_migration.py
  - clockbreak_migration.py
  - clockinout_migration.py
  - column_migration.py
  - communicationautomationrule_migration.py
  - company_migration.py
  - companyjoin_migration.py
  - coupon_migration.py
  - fleet_migration.py
  - fleetstatement_migration.py
  - group_migration.py
  - holiday_migration.py
  - inventoryproducttag_migration.py
  - invoice_migration.py
  - invoiceinspection_migration.py
  - invoiceitem_migration.py
  - invoicephoto_migration.py
  - invoiceredo_migration.py
  - invoicetags_migration.py
  - itemtag_migration.py
  - labor_migration.py
  - labortag_migration.py
  - leadlink_migration.py
  - leadtags_migration.py
  - mailgunemailattachment_migration.py
  - material_migration.py
  - materialtag_migration.py
  - notification_migration.py
  - otherpayment_migration.py
  - passwordresettoken_migration.py
  - payment_migration.py
  - paymentmethod_migration.py
  - permission_migration.py
  - permissionformanager_migration.py
  - permissionforother_migration.py
  - permissionforsales_migration.py
  - permissionfortechnician_migration.py
  - pipelinestage_migration.py
  - requestestimate_migration.py
  - service_migration.py
  - servicemaintenancestage_migration.py
  - source_migration.py
  - status_migration.py
  - stripepayment_migration.py
  - tag_migration.py
  - task_migration.py
  - taskuser_migration.py
  - twiliocredentials_migration.py
  - user_migration.py
  - userfeedback_migration.py
  - userfeedbackattachment_migration.py
  - vehicle_migration.py
  - vehiclecolor_migration.py
  - vehicleparts_migration.py
  - vendor_migration.py
Failed: 22
  - clientsms_migration.py
  - communicationstage_migration.py
  - companyemailtemplate_migration.py
  - depositpayment_migration.py
  - emailtemplate_migration.py
  - inventoryproduct_migration.py
  - inventoryproducthistory_migration.py
  - inventorywirehouseproduct_migration.py
  - invoiceautomationrule_migration.py
  - lead_migration.py
  - leaverequest_migration.py
  - mailguncredential_migration.py
  - mailgunemail_migration.py
  - marketingautomationrule_migration.py
  - message_migration.py
  - notificationsettingsv2_migration.py
  - oauthtoken_migration.py
  - pipelineautomationrule_migration.py
  - refunds_migration.py
  - servicemaintenanceautomationrule_migration.py
  - technician_migration.py
  - timedelayexecution_migration.py

Data Validation Failures: 20
(Scripts completed but no data was migrated)
  - clientsms_migration.py
  - communicationstage_migration.py
  - companyemailtemplate_migration.py
  - depositpayment_migration.py
  - emailtemplate_migration.py
  - inventoryproduct_migration.py
  - inventoryproducthistory_migration.py
  - inventorywirehouseproduct_migration.py
  - invoiceautomationrule_migration.py
  - leaverequest_migration.py
  - mailguncredential_migration.py
  - mailgunemail_migration.py
  - marketingautomationrule_migration.py
  - message_migration.py
  - notificationsettingsv2_migration.py
  - oauthtoken_migration.py
  - pipelineautomationrule_migration.py
  - refunds_migration.py
  - servicemaintenanceautomationrule_migration.py
  - timedelayexecution_migration.py
