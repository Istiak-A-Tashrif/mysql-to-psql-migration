import subprocess
import sys
from pathlib import Path

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
        result = subprocess.run([sys.executable, script, '--phase', '1'], capture_output=True, text=True)
        output = result.stdout + '\n' + result.stderr
        with open(log_file, 'w', encoding='utf-8') as log:
            log.write(output)
        if result.returncode == 0 and 'Operation completed successfully' in output:
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