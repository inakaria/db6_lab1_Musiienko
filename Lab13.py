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


def row_level_locking_update(user_thread_id):
    conn = psycopg2.connect(user=database.username, password=database.password, dbname=database.db)

    with conn:                   
        cursor = conn.cursor()
        for i in range(10_000):
            cursor.execute(f"SELECT counter FROM user_counter WHERE user_id = {user_thread_id[0]} FOR UPDATE")
            counter = cursor.fetchone()[0]
            counter += 1
            cursor.execute(f"UPDATE user_counter SET counter = {counter} WHERE user_id = {user_thread_id[0]}")

        conn.commit()

start_time = time.time()

with ThreadPoolExecutor(max_workers = 10) as exec:
        exec.map(row_level_locking_update, [(1, i) for i in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Musiienko Milena KM-12. Lab-1. Task 3.\nExecution time: {total_time} sec.") #262.892
