import os
import subprocess

def send_file_to_hdfs(local_path, hdfs_path):
    print(f"\n📤 Sending {local_path} → HDFS:{hdfs_path}")

    # 1) Copy file from local Windows → namenode:/tmp/
    subprocess.run(["docker", "cp", local_path, "namenode:/tmp/"], check=True)

    # 2) Execute HDFS command inside the namenode container
    hdfs_cmd = f"hdfs dfs -mkdir -p {hdfs_path}"
    subprocess.run(["docker", "exec", "namenode", "bash", "-c", hdfs_cmd], check=True)

    hdfs_put = f"hdfs dfs -put -f /tmp/{os.path.basename(local_path)} {hdfs_path}"
    subprocess.run(["docker", "exec", "namenode", "bash", "-c", hdfs_put], check=True)

    print(f"✔ DONE: {local_path} uploaded → {hdfs_path}")


# WEATHER
send_file_to_hdfs("data/raw/weather/weather_casablanca_2023.json",
                  "/data/casablanca/weather/")

send_file_to_hdfs("data/raw/weather/weather_casablanca_2024.json",
                  "/data/casablanca/weather/")

# NDVI
send_file_to_hdfs("data/raw/satellite/ndvi_casablanca_2023.json",
                  "/data/casablanca/ndvi/")

send_file_to_hdfs("data/raw/satellite/ndvi_casablanca_2024.json",
                  "/data/casablanca/ndvi/")
