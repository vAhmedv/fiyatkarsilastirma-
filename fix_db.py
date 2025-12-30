import sqlite3

conn = sqlite3.connect('prices.db')
cursor = conn.cursor()

# Add missing columns
try:
    cursor.execute("ALTER TABLE product ADD COLUMN status VARCHAR DEFAULT 'active'")
    print('Added status column')
except Exception as e:
    print(f'status column error: {e}')

try:
    cursor.execute("ALTER TABLE product ADD COLUMN error_message VARCHAR")
    print('Added error_message column')
except Exception as e:
    print(f'error_message column error: {e}')

conn.commit()
conn.close()
print('Database updated!')
