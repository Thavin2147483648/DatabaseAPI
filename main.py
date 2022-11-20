from flask import Flask, request, abort, jsonify
from database_model import DatabaseModel
from tabulate import tabulate

app = Flask(__name__)
model = DatabaseModel('db_connection_info.json')


def get_integer_arg(key):
    value = 0
    try:
        value = int(request.args.get(key))
    except (TypeError, ValueError):
        abort(422)
    return value


@app.route('/')
def index():
    return 'Get record: /record.get?id={record_id}<br>Get probes last records timestamp: /probe.check'


@app.route('/record.get')
def record_get():
    record_id = get_integer_arg('id')
    record = model.get_record(record_id)
    if record is None:
        abort(404)
    return jsonify(record)


@app.route('/probe.check')
def probe_check():
    probes = model.get_records_last_reply()
    print(probes)
    return jsonify(probes)


def main():
    app.run(debug=True)


if __name__ == '__main__':
    main()
