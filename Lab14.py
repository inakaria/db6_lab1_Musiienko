import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor

username = 'musiienko_milena'
password = 'undrugcat10'
database = 'DB6_Lab1'

sqldrop = '''
DROP TABLE IF EXISTS user_counter
'''
sqlcreate = '''
CREATE TABLE user_counter (
    user_id INT PRIMARY KEY,
    counter INT,
    version INT
)
'''
sqlinsert = '''
INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)
with conn:                      
    cursor = conn.cursor()
    cursor.execute(sqldrop)
    cursor.execute(sqlcreate)
    cursor.execute(sqlinsert)


def optimistic_concurrency_control_update(user_thread_id):
    conn = psycopg2.connect(user=username, password=password, dbname=database)
    
    with conn:
        cursor = conn.cursor()
        for i in range(10_000):
            while True:
                cursor.execute(f"SELECT counter, version FROM user_counter WHERE user_id = {user_thread_id[0]}")
                current_values = cursor.fetchone()
                counter, version = current_values[0], current_values[1]
                counter += 1
                cursor.execute(f"UPDATE user_counter SET counter = {counter}, version = {version + 1} WHERE user_id = {user_thread_id[0]} AND version = {version}")
                conn.commit()
                count = cursor.rowcount
                if count > 0:
                    break

start_time = time.time()

with ThreadPoolExecutor(max_workers = 10) as exec:
        exec.map(optimistic_concurrency_control_update, [(1, i) for i in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Musiienko Milena KM-12. Lab-1. Task 4.\nExecution time: {total_time} sec.") #445.305
