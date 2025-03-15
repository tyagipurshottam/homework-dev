import time
import mysql.connector
import logging
import random
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

query_time_gauge = Gauge('mysql_write_query_time_ms', 'Time spent on MySQL write queries in milliseconds')

def get_db_connection():
    return mysql.connector.connect(
        host="mysql-primary-headless.foo.svc.cluster.local",
        user="root",
        password="password",
        database="test_db"
    )

def main():
    start_http_server(8000)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            id INT PRIMARY KEY AUTO_INCREMENT,
            value INT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    try:
        while True:
            value = random.randint(1, 100)
            start_time = time.time()
            cursor.execute("INSERT INTO test_data (value) VALUES (%s)", (value,))
            conn.commit()
            query_time = (time.time() - start_time) * 1000
            query_time_gauge.set(query_time)
            logger.info(f"Inserted value: {value}, query time: {query_time:.2f}ms")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Writer stopped")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
