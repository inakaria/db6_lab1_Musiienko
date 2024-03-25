import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor
import database


conn = psycopg2.connect(user=database.username, password=database.password, dbname=database.db)
with conn:                      
    cursor = conn.cursor()
    cursor.execute(database.sqldrop)
    cursor.execute(database.sqlcreate)
    cursor.execute(database.sqlinsert)


def optimistic_concurrency_control_update(user_thread_id):
    conn = psycopg2.connect(user=database.username, password=database.password, dbname=database.db)
    
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
