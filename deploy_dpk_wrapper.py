#!/usr/bin/env python3
"""
Wrapper to deploy DPK with logging workaround
"""
import os
import sys

# Set log directory to a writable location before importing dtlpy
os.environ['DTLPY_LOG_DIR'] = '/tmp/dataloop_logs'
os.makedirs('/tmp/dataloop_logs', exist_ok=True)

# Now import and run
from deploy_dpk import main

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        print("\nIf you see permission errors, try running manually:")
        print("  python -c \"import dtlpy as dl; dl.setenv('env2'); dl.login()\"")
        print("  Then run: python deploy_dpk.py")
        sys.exit(1)
