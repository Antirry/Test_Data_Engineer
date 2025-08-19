import csv, random, time
from datetime import datetime, timedelta

ips = [f"192.168.0.{i}" for i in range(1,6)]
uas = ["Mozilla", "Chrome", "", "Safari"]

def gen_csv(filename, rows):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["req_id","user_id","campaign_id","creative_id","ip","ua","ts"])
        for i in range(rows):
            writer.writerow([
                f"req_{i}",
                f"user_{random.randint(1,10)}",
                f"camp_{random.randint(1,3)}",
                f"creat_{random.randint(1,5)}",
                random.choice(ips),
                random.choice(uas),
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            ])

gen_csv("/data/impressions.csv", 20)
gen_csv("/data/clicks.csv", 10)
