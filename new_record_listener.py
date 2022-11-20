import psycopg2
import psycopg2.extensions
from select import select
from database_model import DatabaseModel
from time import sleep
from tabulate import tabulate


def main():
    model = DatabaseModel('db_connection_info.json')
    last_record = model.get_record(-1)
    if last_record is None:
        last_record_id = 0
    else:
        last_record_id = last_record['record_id']
    model.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    model.cursor.execute("LISTEN new_record;")
    while True:
        if not select([model.connection], [], [], 5) == ([], [], []):
            model.connection.poll()
            while model.connection.notifies:
                model.connection.notifies.pop()
                model.cursor.execute("SELECT record_id from cd2.record WHERE record_id > " + str(last_record_id)
                                     + " ORDER BY record_id")
                record_ids = list(map(lambda f: str(f[0]), model.cursor.fetchall()))
                print('New records:', ', '.join(record_ids))
                if len(record_ids) != 0:
                    last_record_id = int(record_ids[-1])


if __name__ == '__main__':
    main()
