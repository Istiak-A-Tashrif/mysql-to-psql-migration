import subprocess
import sys
import os
import argparse
from pathlib import Path

# Set UTF-8 encoding for Windows to handle emoji characters
os.environ['PYTHONIOENCODING'] = 'utf-8'

SCRIPTS_FILE = 'migration_scripts.txt'
LOGS_DIR = 'migration_logs'

def run_migrations(phase='1'):
    """Run all migration scripts for the specified phase"""
    print(f"\n=== Running all migrations for phase {phase} ===")
    
    # Ensure logs directory exists
    Path(LOGS_DIR).mkdir(exist_ok=True)

    successes = []
    failures = []

    with open(SCRIPTS_FILE) as f:
        scripts = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

    for script in scripts:
        log_file = f"{LOGS_DIR}/{script.replace('.py', '')}_phase{phase}.log"
        print(f"\n=== Running {script} (phase {phase}) ===")
        try:
            # Run with specified phase
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            result = subprocess.run([sys.executable, script, '--phase', phase], 
                                   capture_output=True, text=True, encoding='utf-8', env=env)
            output = result.stdout + '\n' + result.stderr
            with open(log_file, 'w', encoding='utf-8') as log:
                log.write(output)
            
            # Check for various success indicators based on phase
            if phase == '1':
                success_indicators = [
                    'Operation completed successfully',
                    'Phase 1 complete',
                    'Successfully imported',
                    'imported data to',
                    'Table creation output: CREATE TABLE'
                ]
                # Additional pattern checks for phase 1
                pattern_checks = [
                    ('Created "' in output and 'table successfully' in output)
                ]
            elif phase == '2':
                success_indicators = [
                    'Operation completed successfully',
                    'Phase 2 complete',
                    'created index',
                    'Created indexes',
                    'Index creation',
                    'Skipping existing index'
                ]
                # Additional pattern checks for phase 2
                pattern_checks = [
                    ('Creating' in output and 'indexes' in output),
                    ('Found' in output and 'indexes' in output),
                    ('Created' in output and 'index' in output),
                    ('skip' in output and 'index' in output),
                    ('relation' in output and 'already exists' in output),  # Indexes already exist = success
                    ('Creating' in output and 'index:' in output),  # Creating index: [name] = success attempt
                    ('Found' in output and 'indexes and' in output and 'foreign keys' in output)  # Found X indexes and Y foreign keys
                ]
            elif phase == '3':
                success_indicators = [
                    'Operation completed successfully',
                    'Phase 3 complete',
                    'created foreign key',
                    'Created foreign keys',
                    'Foreign key creation'
                ]
                # Additional pattern checks for phase 3
                pattern_checks = [
                    ('Creating' in output and 'foreign keys' in output),
                    ('Found' in output and 'foreign keys' in output)
                ]
            else:
                success_indicators = ['Operation completed successfully']
                pattern_checks = []
            
            # Check both string indicators and pattern matches
            string_match = any(indicator in output for indicator in success_indicators)
            pattern_match = any(pattern_checks) if pattern_checks else False
            
            # For phase 2, if indexes already exist, consider it success regardless of return code
            indexes_already_exist = phase == '2' and ('relation' in output and 'already exists' in output)
            
            if (result.returncode == 0 and (string_match or pattern_match)) or indexes_already_exist:
                print(f"[SUCCESS] {script}")
                successes.append(script)
            else:
                print(f"[FAIL] {script}")
                failures.append(script)
        except Exception as e:
            with open(log_file, 'a', encoding='utf-8') as log:
                log.write(f"\nException: {e}\n")
            print(f"[ERROR] {script}: {e}")
            failures.append(script)

    print(f"\n=== Migration Summary (phase {phase}) ===")
    print(f"Succeeded: {len(successes)}")
    for s in successes:
        print(f"  - {s}")
    print(f"Failed: {len(failures)}")
    for f in failures:
        print(f"  - {f}")
    
    return len(failures) == 0

def main():
    parser = argparse.ArgumentParser(description='Run all migration scripts for a specific phase')
    parser.add_argument('--phase', choices=['1', '2', '3'], default='1', 
                       help='Migration phase to run (1=table+data, 2=indexes, 3=foreign keys)')
    parser.add_argument('--all-phases', action='store_true', 
                       help='Run all phases in sequence (1, 2, 3)')
    
    args = parser.parse_args()
    
    if args.all_phases:
        print("Running all phases in sequence...")
        success = True
        for phase in ['1', '2', '3']:
            if not run_migrations(phase):
                print(f"Phase {phase} had failures. Stopping.")
                success = False
                break
        if success:
            print("\n=== ALL PHASES COMPLETED SUCCESSFULLY ===")
        else:
            print("\n=== SOME PHASES FAILED ===")
    else:
        run_migrations(args.phase)

if __name__ == "__main__":
    main()