username = 'musiienko_milena'
password = 'undrugcat10'
db = 'DB6_Lab1'

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