import subprocess
import sys
import os
from pathlib import Path

# Import our table utilities for validation
sys.path.insert(0, '.')
from table_utils import verify_data_migration, get_table_record_count

SCRIPTS_FILE = 'migration_scripts.txt'
LOGS_DIR = 'migration_logs'

# Ensure logs directory exists
Path(LOGS_DIR).mkdir(exist_ok=True)

successes = []
failures = []
data_validation_failures = []

def extract_table_name_from_script(script_name):
    """Extract table name from migration script filename"""
    # Remove _migration.py suffix
    base_name = script_name.replace('_migration.py', '')
    
    # Handle special cases and convert to PascalCase
    table_name_mappings = {
        'appointment': 'Appointment',
        'appointmentuser': 'AppointmentUser',
        'attachment': 'Attachment',
        'automationattachment': 'AutomationAttachment',
        'calendarsettings': 'CalendarSettings',
        'cardpayment': 'CardPayment',
        'cashpayment': 'CashPayment',
        'category': 'Category',
        'chattrack': 'ChatTrack',
        'checkpayment': 'CheckPayment',
        'client': 'Client',
        'clientcall': 'ClientCall',
        'clientconversationtrack': 'ClientConversationTrack',
        'clientcoupon': 'ClientCoupon',
        'clientsms': 'ClientSMS',
        'clientsmsattachments': 'ClientSmsAttachments',
        'clockbreak': 'ClockBreak',
        'clockinout': 'ClockInOut',
        'column': 'Column',
        'communicationautomationrule': 'CommunicationAutomationRule',
        'communicationstage': 'CommunicationStage',
        'company': 'Company',
        'companyemailtemplate': 'CompanyEmailTemplate',
        'companyjoin': 'CompanyJoin',
        'coupon': 'Coupon',
        'depositpayment': 'DepositPayment',
        'emailtemplate': 'EmailTemplate',
        'fleet': 'Fleet',
        'fleetstatement': 'FleetStatement',
        'group': 'Group',
        'holiday': 'Holiday',
        'inventoryproduct': 'InventoryProduct',
        'inventoryproducthistory': 'InventoryProductHistory',
        'inventoryproducttag': 'InventoryProductTag',
        'inventorywirehouseproduct': 'InventoryWirehouseProduct',
        'invoice': 'Invoice',
        'invoiceautomationrule': 'InvoiceAutomationRule',
        'invoiceinspection': 'InvoiceInspection',
        'invoiceitem': 'InvoiceItem',
        'invoicephoto': 'InvoicePhoto',
        'invoiceredo': 'InvoiceRedo',
        'invoicetags': 'InvoiceTags',
        'itemtag': 'ItemTag',
        'labor': 'Labor',
        'labortag': 'LaborTag',
        'lead': 'Lead',
        'leadlink': 'LeadLink',
        'leadtags': 'LeadTags',
        'leaverequest': 'LeaveRequest',
        'mailguncredential': 'MailgunCredential',
        'mailgunemail': 'MailgunEmail',
        'mailgunemailattachment': 'MailgunEmailAttachment',
        'marketingautomationrule': 'MarketingAutomationRule',
        'material': 'Material',
        'materialtag': 'MaterialTag',
        'message': 'Message',
        'notification': 'Notification',
        'notificationsettingsv2': 'NotificationSettingsV2',
        'oauthtoken': 'OauthToken',
        'otherpayment': 'OtherPayment',
        'passwordresettoken': 'PasswordResetToken',
        'payment': 'Payment',
        'paymentmethod': 'PaymentMethod',
        'permission': 'Permission',
        'permissionformanager': 'PermissionForManager',
        'permissionforother': 'PermissionForOther',
        'permissionforsales': 'PermissionForSales',
        'permissionfortechnician': 'PermissionForTechnician',
        'pipelineautomationrule': 'PipelineAutomationRule',
        'pipelinestage': 'PipelineStage',
        'refunds': 'Refunds',
        'requestestimate': 'RequestEstimate',
        'service': 'Service',
        'servicemaintenanceautomationrule': 'ServiceMaintenanceAutomationRule',
        'servicemaintenancestage': 'ServiceMaintenanceStage',
        'source': 'Source',
        'status': 'Status',
        'stripepayment': 'StripePayment',
        'tag': 'Tag',
        'task': 'Task',
        'taskuser': 'TaskUser',
        'technician': 'Technician',
        'timedelayexecution': 'TimeDelayExecution',
        'twiliocredentials': 'TwilioCredentials',
        'user': 'User',
        'userfeedback': 'UserFeedback',
        'userfeedbackattachment': 'UserFeedbackAttachment',
        'vehicle': 'Vehicle',
        'vehiclecolor': 'VehicleColor',
        'vehicleparts': 'VehicleParts',
        'vendor': 'Vendor'
    }
    
    return table_name_mappings.get(base_name.lower(), base_name.capitalize())

def validate_migration_data(script_name):
    """Validate that migration actually transferred data by comparing record counts"""
    table_name = extract_table_name_from_script(script_name)
    
    if not table_name:
        print(f"Could not determine table name for {script_name}")
        return False
    
    # Now validate the data migration
    print(f"Validating data migration for table: {table_name}")
    return verify_data_migration(table_name, preserve_case=True)

with open(SCRIPTS_FILE) as f:
    scripts = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

for script in scripts:
    log_file = f"{LOGS_DIR}/{script.replace('.py', '')}.log"
    print(f"\n=== Running {script} (phase 1) ===")
    try:
        # Set environment for UTF-8 encoding
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Always run with --phase 1 only
        result = subprocess.run([sys.executable, script, '--phase', '1'], capture_output=True, text=True, env=env)
        output = result.stdout + '\n' + result.stderr
        with open(log_file, 'w', encoding='utf-8') as log:
            log.write(output)
        
        # Check if script completed without errors
        if result.returncode == 0:
            # Additional validation: check if data was actually migrated
            print(f"Script completed, validating data migration...")
            if validate_migration_data(script):
                print(f"[SUCCESS] {script} - Table created and data migrated")
                successes.append(script)
            else:
                print(f"[DATA_FAIL] {script} - Script succeeded but no data migrated")
                data_validation_failures.append(script)
                failures.append(script)
        else:
            print(f"[FAIL] {script} - Script execution failed")
            failures.append(script)
    except Exception as e:
        with open(log_file, 'a', encoding='utf-8') as log:
            log.write(f"\nException: {e}\n")
        print(f"[ERROR] {script}: {e}")
        failures.append(script)

print("\n=== Migration Summary (phase 1 only) ===")
print(f"Succeeded: {len(successes)}")
for s in successes:
    print(f"  - {s}")
    
print(f"Failed: {len(failures)}")
for f in failures:
    print(f"  - {f}")

if data_validation_failures:
    print(f"\nData Validation Failures: {len(data_validation_failures)}")
    print("(Scripts completed but no data was migrated)")
    for f in data_validation_failures:
        print(f"  - {f}")
