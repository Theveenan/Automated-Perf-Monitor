import subprocess
import os
command = 'logman start "New Data Collector Set"'
print("starting logging")
#subprocess.run(command, shell=True)

# Specify the file name you want to delete
file_to_delete = "delete.txt"

# Get the current script's directory
current_dir = os.path.dirname(__file__)

# Navigate two levels up to reach the parent directory
parent_dir = os.path.dirname(current_dir)

# Construct the full path to the file
file_path = os.path.join(parent_dir, file_to_delete)
os.remove(file_path)
#subprocess.run(command, shell=False)