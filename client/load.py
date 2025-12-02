import subprocess
import time

PHOTO_ID = "358cf156a0dfa77c9e9854d46e10ed06de944982664d265bbff6ce80e89fd16b"   # Replace with your actual photo ID
DOWNLOAD_COUNT = 50
DOWNLOAD_INTERVAL = 1       # seconds between downloads
STATUS_INTERVAL = 10        # seconds between status checks
STATUS_CHECKS = 6           # number of status checks

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip()

def download_photo():
    print(f"Downloading photo {PHOTO_ID}...")
    output, error = run_cmd(f"python3 client_service.py download {PHOTO_ID} download.jpg")
    if output:
        print(output)
    if error:
        print("Error:", error)

def check_status():
    print(f"Checking status of photo {PHOTO_ID}...")
    output, error = run_cmd(f"python3 client_service.py status {PHOTO_ID}")
    if output:
        print(output)
    if error:
        print("Error:", error)


if __name__ == "__main__":
    print("Starting hot photo replication test...")
    
    # Step 1: Repeated downloads to generate high access rate
    for i in range(DOWNLOAD_COUNT):
        download_photo()
        time.sleep(DOWNLOAD_INTERVAL)
    
    # Step 2: Periodic status checks to observe replication progress
    for i in range(STATUS_CHECKS):
        check_status()
        time.sleep(STATUS_INTERVAL)
    
    print("Test complete. Monitor replication logs and status outputs for replica count updates.")
