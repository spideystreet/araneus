import subprocess
import sys
import os
from dotenv import load_dotenv

def run_dbt(args):
    # Load .env file
    load_dotenv()
    
    # Ensure we are in the dbt directory or point to it
    project_dir = "dbt"
    profiles_dir = "dbt"
    
    cmd = ["dbt"] + args + ["--project-dir", project_dir, "--profiles-dir", profiles_dir]
    
    print(f"Running command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"dbt failed with exit code {e.returncode}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/tool_run_dbt.py [dbt_command] [options]")
        sys.exit(1)
    
    run_dbt(sys.argv[1:])
