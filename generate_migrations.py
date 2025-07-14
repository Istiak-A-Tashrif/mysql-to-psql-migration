#!/usr/bin/env python3
"""
Migration Script Generator
Creates clean migration scripts using a working template
"""

import os
import re

# List of tables that need new migration scripts
BROKEN_TABLES = [
    "CompanyJoin", "DepositPayment", "EmailTemplate", "FleetStatement", "Group",
    "InventoryProduct", "InventoryProductHistory", "InventoryProductTag", 
    "InvoiceAutomationRule", "InvoicePhoto", "InvoiceRedo", "InvoiceTags",
    "ItemTag", "LaborTag", "LeadLink", "LeadTags", "LeaveRequest",
    "MailgunCredential", "MailgunEmail", "MailgunEmailAttachment", 
    "MarketingAutomationRule", "MaterialTag", "Message", "NotificationSettingsV2",
    "OauthToken", "OtherPayment", "PasswordResetToken", "PaymentMethod",
    "Permission", "PermissionForManager", "PermissionForOther", "PermissionForSales",
    "PermissionForTechnician", "PipelineAutomationRule", "PipelineStage",
    "refunds", "RequestEstimate", "ServiceMaintenanceAutomationRule", 
    "ServiceMaintenanceStage", "StripePayment", "TaskUser", "TimeDelayExecution",
    "TwilioCredentials", "UserFeedback", "UserFeedbackAttachment", "VehicleParts"
]

def create_migration_script(table_name):
    """Create a clean migration script for the given table"""
    
    script_name = f"{table_name.lower()}_migration.py"
    
    # Read the template (client_migration.py)
    with open('client_migration.py', 'r', encoding='utf-8') as f:
        template = f.read()
    
    # Replace Client with the new table name
    new_script = template.replace('Client', table_name)
    new_script = new_script.replace('client', table_name.lower())
    new_script = new_script.replace('CLIENT', table_name.upper())
    
    # Update the docstring description
    description_map = {
        "CompanyJoin": "Company join/relationship data",
        "DepositPayment": "Deposit payment records", 
        "EmailTemplate": "Email template configurations",
        "FleetStatement": "Fleet statement records",
        "Group": "User group definitions",
        "InventoryProduct": "Product inventory data",
        "InventoryProductHistory": "Product inventory history",
        "InventoryProductTag": "Product inventory tags",
        "InvoiceAutomationRule": "Invoice automation rules",
        "InvoicePhoto": "Invoice photo attachments",
        "InvoiceRedo": "Invoice redo records",
        "InvoiceTags": "Invoice tag assignments",
        "ItemTag": "Item tag definitions",
        "LaborTag": "Labor tag assignments", 
        "LeadLink": "Lead relationship links",
        "LeadTags": "Lead tag assignments",
        "LeaveRequest": "Employee leave requests",
        "MailgunCredential": "Mailgun API credentials",
        "MailgunEmail": "Mailgun email records",
        "MailgunEmailAttachment": "Mailgun email attachments",
        "MarketingAutomationRule": "Marketing automation rules",
        "MaterialTag": "Material tag assignments",
        "Message": "System message records",
        "NotificationSettingsV2": "User notification settings",
        "OauthToken": "OAuth token storage",
        "OtherPayment": "Miscellaneous payment records",
        "PasswordResetToken": "Password reset tokens",
        "PaymentMethod": "Payment method definitions",
        "Permission": "User permission definitions",
        "PermissionForManager": "Manager permission assignments",
        "PermissionForOther": "Other user permission assignments", 
        "PermissionForSales": "Sales permission assignments",
        "PermissionForTechnician": "Technician permission assignments",
        "PipelineAutomationRule": "Pipeline automation rules",
        "PipelineStage": "Pipeline stage definitions",
        "refunds": "Refund transaction records",
        "RequestEstimate": "Estimate request records", 
        "ServiceMaintenanceAutomationRule": "Service maintenance automation",
        "ServiceMaintenanceStage": "Service maintenance stages",
        "StripePayment": "Stripe payment records",
        "TaskUser": "Task user assignments",
        "TimeDelayExecution": "Time delay execution records",
        "TwilioCredentials": "Twilio API credentials",
        "UserFeedback": "User feedback records",
        "UserFeedbackAttachment": "User feedback attachments",
        "VehicleParts": "Vehicle parts inventory"
    }
    
    description = description_map.get(table_name, f"{table_name} table data")
    
    # Update the docstring
    new_script = re.sub(
        r'This script provides a complete 3-phase migration approach specifically for the.*?\n.*?customer/client data',
        f'This script provides a complete 3-phase migration approach specifically for the {table_name} table:\n1. Phase 1: Table + Data (without constraints)\n2. Phase 2: Indexes (after data import for performance)\n3. Phase 3: Foreign Keys (after all tables exist)\n\nFeatures:\n- Preserves MySQL case sensitivity for table and column names\n- Handles {table_name}-specific data types and constraints\n- Manages foreign key dependencies\n- Creates appropriate indexes for {table_name} table\n- {description}',
        new_script,
        flags=re.DOTALL
    )
    
    # Update function names to be table-specific
    new_script = new_script.replace('get_client_table_info', f'get_{table_name.lower()}_table_info')
    new_script = new_script.replace('extract_client_indexes_from_ddl', f'extract_{table_name.lower()}_indexes_from_ddl')
    new_script = new_script.replace('extract_client_foreign_keys_from_ddl', f'extract_{table_name.lower()}_foreign_keys_from_ddl')
    new_script = new_script.replace('convert_client_mysql_to_postgresql_ddl', f'convert_{table_name.lower()}_mysql_to_postgresql_ddl')
    new_script = new_script.replace('phase1_create_client_table', f'phase1_create_{table_name.lower()}_table')
    new_script = new_script.replace('phase2_add_client_indexes', f'phase2_add_{table_name.lower()}_indexes')
    new_script = new_script.replace('phase3_add_client_foreign_keys', f'phase3_add_{table_name.lower()}_foreign_keys')
    
    # Write the new script
    with open(script_name, 'w', encoding='utf-8') as f:
        f.write(new_script)
    
    print(f"✅ Created {script_name}")
    
    # Test syntax
    try:
        with open(script_name, 'r', encoding='utf-8') as f:
            compile(f.read(), script_name, 'exec')
        print(f"   ✅ Syntax is valid")
    except SyntaxError as e:
        print(f"   ❌ Syntax error: {e}")

def main():
    """Generate all migration scripts"""
    print("Generating clean migration scripts...")
    print(f"Creating {len(BROKEN_TABLES)} migration scripts...")
    
    for table_name in BROKEN_TABLES:
        create_migration_script(table_name)
    
    print(f"\nGenerated {len(BROKEN_TABLES)} migration scripts")
    print("All scripts use the working client_migration.py as a template")

if __name__ == "__main__":
    main()
