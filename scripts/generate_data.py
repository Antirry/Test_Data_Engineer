import csv, random, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path('/data')
IMPR = DATA_DIR / 'impressions.csv'
CLKS = DATA_DIR / 'clicks.csv'

def _row(uid_ok=True):
    return [
        str(uuid.uuid4()) if uid_ok else '',
        f'user{random.randint(1,50)}',
        f'camp{random.randint(1,10)}',
        f'cre{random.randint(1,20)}',
        f'192.168.0.{random.randint(1,255)}',
        random.choice(['Mozilla/5.0','Chrome/91.0','','Safari/14.0']),
        (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 1800))).isoformat()
    ]

def generate_impressions(n=200):
    IMPR.parent.mkdir(parents=True, exist_ok=True)
    with IMPR.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['req_id','user_id','campaign_id','creative_id','ip','ua','ts'])
        for _ in range(n):
            w.writerow(_row(uid_ok=True))

def generate_clicks(n=300):
    CLKS.parent.mkdir(parents=True, exist_ok=True)
    with CLKS.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['req_id','user_id','campaign_id','creative_id','ip','ua','ts'])
        for _ in range(n):
            # 10% грязи: пустой req_id
            w.writerow(_row(uid_ok=(random.random() > 0.1)))
        # Синтетический всплеск для гарантированного срабатывания анти-фрода
        burst_ip = '10.10.10.10'
        base_ts = datetime.now(timezone.utc)
        for i in range(8):  # 8 кликов за ~5 сек
            w.writerow([
                str(uuid.uuid4()),
                f'user{random.randint(1,50)}',
                f'camp{random.randint(1,10)}',
                f'cre{random.randint(1,20)}',
                burst_ip,
                'Mozilla/5.0',
                (base_ts - timedelta(seconds=random.randint(0, 5))).isoformat()
            ])

if __name__ == '__main__':
    generate_impressions()
    generate_clicks()
    print('Data generated.')
    
