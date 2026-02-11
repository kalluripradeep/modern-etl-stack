"""
Prepare test data for Spark processing
Copies Parquet file to Spark data directory
"""

import pandas as pd
from datetime import datetime
import shutil
import os


print("📦 Creating test data for Spark...")


data = {
    'order_id': range(1, 101),
    'customer_id': [i % 20 + 1 for i in range(100)],
    'order_date': [datetime.now().date() for _ in range(100)],
    'total_amount': [100.50 + i for i in range(100)],
    'status': ['pending', 'shipped', 'delivered'] * 33 + ['pending'],
    'created_at': [datetime.now() for _ in range(100)],
    'updated_at': [datetime.now() for _ in range(100)]
}

df = pd.DataFrame(data)


os.makedirs('spark/data', exist_ok=True)


today = datetime.now().strftime('%Y%m%d')
output_file = f'spark/data/orders_{today}.parquet'
df.to_parquet(output_file, index=False)

print(f"✅ Created test data: {output_file}")
print(f"📊 Rows: {len(df)}")
print(f"📅 Date: {today}")