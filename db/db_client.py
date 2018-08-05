import psycopg2


def connect(*args):
    return psycopg2.connect(dbname="test", user="postgres", password="secret")
