
# Batch Processing Application

## Overview

This project is a batch processing application that updates

 and inserts records into various tables in a MySQL database. The process is divided into several steps and runs in chunks to handle large amounts of data efficiently.

## Architecture

- **CarrierStorage**: Holds the carrier value and the states of different tasks.
- **ChunkProcessingService**: Manages the batch processing, including updating and inserting records in chunks.
- **BatchJobController**: REST controller to trigger the batch job.
- **MySQL Database**: Stores the data for the batch processing.

## Project Setup

### Prerequisites

- Java 11 or higher
- Maven
- MySQL
- Python 3
- Required Python packages: `mysql-connector-python`, `tqdm`

### Database Setup

1. **Install MySQL**:
   - Follow the instructions on the [MySQL website](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/) to install MySQL.

2. **Create Database and Tables**:
   - Run the following commands in the MySQL command line or a MySQL client to create the database and tables:
     ```sql
     CREATE DATABASE testdb;

     USE testdb;

     CREATE TABLE IF NOT EXISTS table1 (
         groupID INT PRIMARY KEY,
         status VARCHAR(1),
         programname VARCHAR(6),
         carrier VARCHAR(6)
     );

     CREATE TABLE IF NOT EXISTS table1_history (
         groupID INT(255),
         status VARCHAR(1),
         programname VARCHAR(6),
         carrier VARCHAR(6)
     );

     CREATE TABLE IF NOT EXISTS table2 (
         groupID INT PRIMARY KEY,
         status VARCHAR(1),
         programname VARCHAR(6),
         carrier VARCHAR(6)
     );

     CREATE TABLE IF NOT EXISTS table2_history (
         groupID INT(255),
         status VARCHAR(1),
         programname VARCHAR(6),
         carrier VARCHAR(6)
     );
     ```

3. **Generate Mock Data**:
   - Use the following Python script to generate mock data for testing:
     ```python
     import mysql.connector
     import random
     import string
     from tqdm import tqdm

     # Database setup
     conn = mysql.connector.connect(
         host='localhost',  # Replace with your MySQL server host
         user='root',       # Replace with your MySQL username
         password='password', # Replace with your MySQL password
         database='testdb'  # Replace with your MySQL database name
     )
     cursor = conn.cursor()

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
     insert_mock_data('table1', 1000)
     insert_mock_data('table2', 1000)

     # Commit and close
     conn.commit()
     conn.close()

     print("Mock data created successfully.")
     ```

### Java Project Setup

1. **Clone the Repository**:
   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Build the Project**:
   ```sh
   mvn clean install
   ```

3. **Run the Application**:
   ```sh
   mvn spring-boot:run
   ```

### REST API

The batch job can be triggered using the following REST endpoint:

- **Start Job**:
  ```sh
  GET /batch/start?carrier=<carrier>
  ```

### Explanation of the Main Components

#### CarrierStorage

Holds the carrier value and the states of different tasks:
- `carrier`: The carrier value.
- `isRunning`: Indicates if the process is currently running.
- `updateCompleted`: Indicates if the update step is completed.
- `insertCompleted`: Indicates if the insert step is completed.
- `table2UpdateCompleted`: Indicates if the table2 update step is completed.

#### ChunkProcessingService

Manages the batch processing, including updating and inserting records in chunks:
- `startProcessing()`: Starts the scheduled processing.
- `stopProcessing()`: Stops the scheduled processing.
- `processChunk()`: Processes each chunk and moves through the steps.

#### BatchJobController

REST controller to trigger the batch job:
- `startJob(@RequestParam String carrier)`: Endpoint to start the batch job with the given carrier.

## License

This project is licensed under the MIT License.

