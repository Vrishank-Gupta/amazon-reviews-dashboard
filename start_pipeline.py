import subprocess
import time

print("Starting pipeline...")
subprocess.Popen(["python", "run_pipeline.py"])

print("Pipeline process spawned. Keeping console open...")
time.sleep(15)