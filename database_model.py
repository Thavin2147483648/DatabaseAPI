import json
import psycopg2


class DatabaseModel:
    def __init__(self, connection_info_filename):
        with open(connection_info_filename, 'r') as f:
            connection_info = json.load(f)
        self.connection = psycopg2.connect(**connection_info)
        self.sensor_headers = ('record_id', 'temp_bmp', 'press_bmp', 'hum_htu', 'temp_ds18b20', 'charge', 'sta0')
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def get_record(self, record_id):
        record_id = int(record_id)
        base_query = "SELECT " + ', '.join(self.sensor_headers) + " FROM cd2.record"
        if record_id == -1:
            self.cursor.execute(base_query + " ORDER BY record_id DESC LIMIT 1")
        else:
            self.cursor.execute(base_query + " WHERE record_id = " + str(record_id) + " LIMIT 1")
        result = self.cursor.fetchone()
        if result is None:
            return None
        return dict(zip(self.sensor_headers, result))

    def get_records_last_reply(self):
        query = 'SELECT probe.probe_id, MAX(EXTRACT(epoch from probe_timestamp)) ' \
                'FROM cd2.probe LEFT JOIN cd2.record ' \
                'ON probe.probe_id = record.probe_id GROUP BY probe.probe_id ORDER BY probe_id;'
        self.cursor.execute(query)
        return tuple(map(lambda x: dict(zip(('id', 'timestamp'), (x[0], int(x[1]) if x[1] is not None else 0))),
                         self.cursor.fetchall()))
