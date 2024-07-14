import mysql.connector
import random
import string
from tqdm import tqdm

# Database setup
conn = mysql.connector.connect(
    host='localhost',  # Replace with your MySQL server host
    user='sa',       # Replace with your MySQL username
    password='password', # Replace with your MySQL password
    database='test'  # Replace with your MySQL database name
)
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS agd (
        groupID INT PRIMARY KEY,
        status VARCHAR(1),
        programname VARCHAR(6),
        carrier VARCHAR(255)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS agdh (
        id int auto_increment primary key,
        groupID INT(255),
        status VARCHAR(1),
        programname VARCHAR(6),
        carrier VARCHAR(255)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS gel (
        groupID INT PRIMARY KEY,
        status VARCHAR(1),
        programname VARCHAR(6),
        carrier VARCHAR(255)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS gelh (
        id int auto_increment primary key,
        groupID INT(255),
        status VARCHAR(1),
        programname VARCHAR(6),
        carrier VARCHAR(255)
    )
''')

# Helper functions
def random_string(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_status():
    return random.choice(['F', 'T', 'I'])

def random_carrier():
    return random.choice(['carrier1', 'carrier2', 'carrier3'])

def insert_mock_data(table_name, num_records):
    print(f"Inserting data into {table_name}...")
    existing_ids = set()

    # Fetch existing groupIDs to avoid duplicates
    cursor.execute(f"SELECT groupID FROM {table_name}")
    for row in cursor.fetchall():
        existing_ids.add(row[0])

    for _ in tqdm(range(num_records), desc=f"{table_name}"):
        while True:
            groupID = random.randint(1, 100000)
            if groupID not in existing_ids:
                existing_ids.add(groupID)
                break
        status = random_status()
        programname = random_string()
        carrier = random_carrier()
        cursor.execute(f'''
            INSERT INTO {table_name} (groupID, status, programname, carrier)
            VALUES (%s, %s, %s, %s)
        ''', (groupID, status, programname, carrier))

# Insert mock data
insert_mock_data('agd', 100000)
# insert_mock_data('agdh', 100)
insert_mock_data('gel', 100000)
# insert_mock_data('gelh', 100)

# Commit and close
conn.commit()
conn.close()

print("Mock data created successfully.")
