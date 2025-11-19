import subprocess
import os

LOCAL_DIR = "satellite_images"
HDFS_DIR = "/data/raw/satellite"

# 1) create HDFS folder if not exists
subprocess.run([
    "docker", "exec", "namenode",
    "hdfs", "dfs", "-mkdir", "-p", HDFS_DIR
])

# 2) upload all .tif images
for filename in os.listdir(LOCAL_DIR):
    if filename.endswith(".tif"):
        local_path = f"{LOCAL_DIR}/{filename}"
        print("Uploading:", filename)

        # copy to namenode
        subprocess.run(["docker", "cp", local_path, f"namenode:/{filename}"])

        # move to HDFS
        subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-put", "-f", f"/{filename}", f"{HDFS_DIR}/{filename}"
        ])

print("✅ DONE — All satellite images uploaded to HDFS.")
