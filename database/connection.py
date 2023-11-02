from psycopg2 import connect

def init_db():
    conn = connect(
        dbname='t2',
        user='postgres',
        password='postgres',
        host='db',
    )
    return conn