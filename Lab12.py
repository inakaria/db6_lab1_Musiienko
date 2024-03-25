import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor
import database


conn = psycopg2.connect(user=database.username, password=database.password, dbname=database.db)              
cursor = conn.cursor()
cursor.execute(database.sqldrop)
cursor.execute(database.sqlcreate)
cursor.execute(database.sqlinsert)


def in_place_update(user_id):
    for i in range(10_000):
        cursor.execute(f"UPDATE user_counter SET counter = counter + 1 WHERE user_id = {user_id}")
    conn.commit()

start_time = time.time()

with ThreadPoolExecutor(max_workers=10) as exec:
        exec.map(in_place_update, [1 for i in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Musiienko Milena KM-12. Lab-1. Task 2.\nExecution time: {total_time} sec.") #146.411

cursor.close()
conn.close()
