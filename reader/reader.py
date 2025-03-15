import time
import mysql.connector
import logging
import os
from flask import Flask, jsonify
from prometheus_client import Gauge, make_wsgi_app

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

query_time_gauge = Gauge('mysql_read_query_time_ms', 'Time spent on MySQL read queries in milliseconds')

app = Flask(__name__)
POD_NAME = os.getenv("POD_NAME", "unknown")

def get_db_connection():
    return mysql.connector.connect(
        host="mysql-secondary-headless.foo.svc.cluster.local",
        user="root",
        password="password",
        database="test_db"
    )

def count_rows():
    conn = get_db_connection()
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute("SELECT COUNT(*) FROM test_data")
    count = cursor.fetchone()[0]
    query_time = (time.time() - start_time) * 1000
    cursor.close()
    conn.close()
    return count, query_time

def reader_loop():
    try:
        while True:
            count, query_time = count_rows()
            query_time_gauge.set(query_time)
            logger.info(f"Row count: {count}, query time: {query_time:.2f}ms")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Reader stopped")

@app.route('/rows')
def get_row_count():
    count, query_time = count_rows()
    query_time_gauge.set(query_time)
    return jsonify({"rows": count, "pod": POD_NAME})

if __name__ == "__main__":
    from threading import Thread
    Thread(target=reader_loop, daemon=True).start()
    app.wsgi_app = make_wsgi_app(app.wsgi_app)
    app.run(host='0.0.0.0', port=5000)
