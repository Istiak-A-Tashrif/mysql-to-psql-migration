import subprocess
import sys
import os
from pathlib import Path

# Set UTF-8 encoding for Windows to handle emoji characters
os.environ['PYTHONIOENCODING'] = 'utf-8'

SCRIPTS_FILE = 'migration_scripts.txt'
LOGS_DIR = 'migration_logs'

# Ensure logs directory exists
Path(LOGS_DIR).mkdir(exist_ok=True)

successes = []
failures = []

with open(SCRIPTS_FILE) as f:
    scripts = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

for script in scripts:
    log_file = f"{LOGS_DIR}/{script.replace('.py', '')}.log"
    print(f"\n=== Running {script} (phase 1) ===")
    try:
        # Always run with --phase 1 only
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        result = subprocess.run([sys.executable, script, '--phase', '1'], 
                               capture_output=True, text=True, encoding='utf-8', env=env)
        output = result.stdout + '\n' + result.stderr
        with open(log_file, 'w', encoding='utf-8') as log:
            log.write(output)
        # Check for various success indicators
        success_indicators = [
            'Operation completed successfully',
            'Phase 1 complete',
            'Successfully imported',
            'imported data to',
            'Table creation output: CREATE TABLE',
            'Created "' in output and 'table successfully' in output
        ]
        
        if result.returncode == 0 and any(indicator in output for indicator in success_indicators):
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

print("\n=== Migration Summary (phase 1 only) ===")
print(f"Succeeded: {len(successes)}")
for s in successes:
    print(f"  - {s}")
print(f"Failed: {len(failures)}")
for f in failures:
    print(f"  - {f}") 