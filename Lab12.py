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
cursor = conn.cursor()
cursor.execute(sqldrop)
cursor.execute(sqlcreate)
cursor.execute(sqlinsert)


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
