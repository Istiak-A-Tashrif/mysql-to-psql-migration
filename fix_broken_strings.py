#!/usr/bin/env python3
"""
Fix specific broken string patterns in migration files
"""
import os
import glob
import re

def fix_broken_strings():
    """Fix broken string literals in migration files"""
    
    # Files that were reported with syntax errors
    files_to_fix = [
        'depositpayment_migration.py', 'emailtemplate_migration.py', 'fleetstatement_migration.py',
        'group_migration.py', 'inventoryproducthistory_migration.py', 'inventoryproducttag_migration.py',
        'invoiceautomationrule_migration.py', 'invoicephoto_migration.py', 'invoiceredo_migration.py',
        'invoicetags_migration.py', 'itemtag_migration.py', 'labortag_migration.py',
        'leadlink_migration.py', 'leadtags_migration.py', 'leaverequest_migration.py',
        'mailguncredential_migration.py', 'mailgunemailattachment_migration.py', 'mailgunemail_migration.py',
        'marketingautomationrule_migration.py', 'materialtag_migration.py', 'message_migration.py',
        'notificationsettingsv2_migration.py', 'oauthtoken_migration.py', 'otherpayment_migration.py',
        'passwordresettoken_migration.py', 'paymentmethod_migration.py', 'permissionformanager_migration.py',
        'permissionforother_migration.py', 'permissionforsales_migration.py', 'permissionfortechnician_migration.py',
        'permission_migration.py', 'pipelineautomationrule_migration.py', 'pipelinestage_migration.py',
        'refunds_migration.py', 'requestestimate_migration.py', 'servicemaintenanceautomationrule_migration.py',
        'servicemaintenancestage_migration.py', 'stripepayment_migration.py', 'taskuser_migration.py',
        'timedelayexecution_migration.py', 'twiliocredentials_migration.py', 'userfeedbackattachment_migration.py',
        'userfeedback_migration.py', 'vehicleparts_migration.py'
    ]
    
    fixed_count = 0
    
    for file_path in files_to_fix:
        if not os.path.exists(file_path):
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Fix broken newline string literal
            content = re.sub(r"create_table_statement = '\n'\\.join", 
                           "create_table_statement = '\\\\n'.join", content)
            
            # Fix unterminated f-strings in SHOW INDEX commands
            content = re.sub(r'f"SHOW INDEX FROM `([^`]+)`;\\n\\s*\\]\\)', 
                           r'f"SHOW INDEX FROM `\1`;"\n        ])', content)
            
            if content != original_content:
                try:
                    compile(content, file_path, 'exec')
                    print(f"✅ Fixed {file_path}")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    fixed_count += 1
                except SyntaxError as e:
                    # Try manual line-by-line approach
                    lines = content.split('\\n')
                    fixed_lines = []
                    
                    for i, line in enumerate(lines):
                        if "create_table_statement = '" in line and i + 1 < len(lines) and lines[i + 1].startswith("'.join"):
                            # Fix broken string
                            fixed_lines.append("        create_table_statement = '\\\\n'.join(create_table_lines[1:])  # Skip header")
                            # Skip the next line
                            if i + 1 < len(lines):
                                i += 1
                                continue
                        elif 'f"SHOW INDEX FROM' in line and not line.rstrip().endswith(';"'):
                            # Fix unterminated f-string
                            table_match = re.search(r'`([^`]+)`', line)
                            if table_match:
                                table_name = table_match.group(1)
                                fixed_lines.append(f'            \\'-e\\', f"SHOW INDEX FROM `{table_name}`;"')
                            else:
                                fixed_lines.append(line)
                        else:
                            fixed_lines.append(line)
                    
                    new_content = '\\n'.join(fixed_lines)
                    try:
                        compile(new_content, file_path, 'exec')
                        print(f"✅ Fixed {file_path} (manual)")
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(new_content)
                        fixed_count += 1
                    except SyntaxError as e2:
                        print(f"⚠️ {file_path} still has errors: {e2}")
                        
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
    
    print(f"\\nFixed {fixed_count} files")

if __name__ == "__main__":
    fix_broken_strings()
