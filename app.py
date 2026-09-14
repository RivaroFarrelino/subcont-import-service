import os
import tempfile
from flask import Flask, request, jsonify
import mysql.connector
import pandas as pd

app = Flask(__name__)

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DATABASE', 'subcont')
}

BATCH_SIZE = 2000

TABLE_CONFIGS = [
    {
        'table': 'report',
        'sources': [
            {'sheet': 'Report', 'header_row': 2, 'usecols': list(range(38))}
        ],
        'columns': [
            'row_id', 'commission_number', 'position_number', 'piece_position', 'upload_date',
            'check_1', 'check_2', 'source_document', 'commission_number_dup', 'fppp_number',
            'customer_name', 'opening', 'position_name', 'color', 'unit_name', 'glass_type',
            'frame_qty', 'sash_qty', 'total_scan', 'code', 'barcode', 'cut_qty', 'cutting_qty',
            'cutting_date_1', 'cutting_date_2', 'assembly_user', 'assembly_scan_qty', 'assembly_date',
            'sealant_user', 'sealant_scan_qty', 'sealant_date', 'packing_user', 'packing_standard_pack',
            'packing_scan_qty', 'project_code', 'billing_period', 'check_3', 'commission_number_ref'
        ],
        'key_columns': ['commission_number', 'position_number']
    },
    {
        'table': 'data_awal',
        'sources': [
            {'sheet': 'Data Awal', 'header_row': 3, 'usecols': list(range(13))}
        ],
        'columns': [
            'project_code', 'fppp_ref', 'customer_name', 'commission_number', 'position_number',
            'unit_code', 'upload_date', 'order_qty', 'frame_qty', 'sash_qty', 'check_value',
            'check_2', 'check_3'
        ],
        'key_columns': ['commission_number', 'position_number']
    },
    {
        'table': 'detail',
        'sources': [
            {'sheet': 'Detail', 'header_row': 0, 'usecols': list(range(9))},
            {'sheet': 'Detail 2025', 'header_row': 0, 'usecols': list(range(9))}
        ],
        'columns': [
            'order_number', 'commission_number', 'position_number', 'position_name',
            'mark_code', 'mark_content', 'entry_date', 'check_value', 'brand'
        ],
        'key_columns': ['commission_number', 'position_number', 'mark_code']
    },
    {
        'table': 'qty_pot',
        'sources': [
            {'sheet': 'Qty Pot', 'header_row': 0, 'usecols': list(range(6))}
        ],
        'columns': [
            'commission_number', 'position_number', 'piece_number', 'cut_qty',
            'entry_date', 'check_value'
        ],
        'key_columns': ['commission_number', 'position_number', 'piece_number']
    },
    {
        'table': 'laporan_cutting',
        'sources': [
            {'sheet': 'Laporan Cutting', 'header_row': 1, 'usecols': list(range(14))},
            {'sheet': 'Cutting 2024', 'header_row': 1, 'usecols': list(range(14))}
        ],
        'columns': [
            'commission_number', 'position_number', 'cost_center', 'piece_position',
            'part_code', 'status', 'user_name', 'event_time', 'event_date', 'check_value',
            'shift', 'unit', 'opening', 'brand'
        ],
        'key_columns': ['commission_number', 'position_number', 'part_code']
    },
    {
        'table': 'laporan_sealant',
        'sources': [
            {'sheet': 'Laporan Sealant', 'header_row': 1, 'usecols': list(range(12))}
        ],
        'columns': [
            'commission_number', 'position_number', 'cost_center', 'piece_position',
            'part_code', 'status', 'user_name', 'event_time', 'event_date', 'check_value',
            'unit', 'opening'
        ],
        'key_columns': ['commission_number', 'position_number', 'part_code']
    },
    {
        'table': 'laporan_assembling',
        'sources': [
            {'sheet': 'Laporan Assembling', 'header_row': 1, 'usecols': list(range(12))}
        ],
        'columns': [
            'commission_number', 'position_number', 'cost_center', 'piece_position',
            'part_code', 'status', 'user_name', 'event_time', 'event_date', 'check_value',
            'unit', 'opening'
        ],
        'key_columns': ['commission_number', 'position_number', 'part_code']
    },
    {
        'table': 'laporan_packing',
        'sources': [
            {'sheet': 'Laporan Packing', 'header_row': 1, 'usecols': list(range(17))}
        ],
        'columns': [
            'production_order', 'unit_position', 'station', 'unit_sequence', 'part_code',
            'status', 'user_name', 'event_time', 'event_date', 'check_value', 'unit', 'opening',
            'start_time', 'start_date', 'order_ref', 'division', 'event_time_2'
        ],
        'key_columns': ['production_order', 'unit_position', 'part_code']
    },
    {
        'table': 'laporan_qc',
        'sources': [
            {'sheet': 'Laporan QC', 'header_row': 1, 'usecols': list(range(10))}
        ],
        'columns': [
            'qc_user', 'station', 'event_date', 'event_time', 'production_order',
            'fppp_ref', 'unit_number', 'unit_code', 'code', 'note'
        ],
        'key_columns': ['code']
    }
]


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def read_sheet(file_path, source, columns):
    return pd.read_excel(
        file_path,
        sheet_name=source['sheet'],
        header=source['header_row'],
        usecols=source['usecols'],
        names=columns
    )


def build_upsert_sql(table, columns):
    column_list = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_clause = ', '.join([col + ' = VALUES(' + col + ')' for col in columns])
    return (
        'INSERT INTO ' + table + ' (' + column_list + ') VALUES (' + placeholders + ') '
        'ON DUPLICATE KEY UPDATE ' + update_clause
    )


def upsert_dataframe(cursor, table, columns, key_columns, dataframe):
    dataframe = dataframe.dropna(how='all', subset=key_columns)
    sql = build_upsert_sql(table, columns)
    rows = [tuple(clean_value(value) for value in row) for row in dataframe.itertuples(index=False, name=None)]

    processed = 0
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        cursor.executemany(sql, batch)
        processed += len(batch)

    return processed


def run_import(file_path):
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()
    summary = {}

    for config in TABLE_CONFIGS:
        table = config['table']
        total_rows = 0

        for source in config['sources']:
            dataframe = read_sheet(file_path, source, config['columns'])
            total_rows += upsert_dataframe(cursor, table, config['columns'], config['key_columns'], dataframe)

        connection.commit()
        summary[table] = total_rows

    cursor.close()
    connection.close()
    return summary


@app.route('/import', methods=['POST'])
def handle_import():
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'File tidak ditemukan di request'}), 400

    uploaded_file = request.files['file']
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, uploaded_file.filename)
    uploaded_file.save(temp_path)

    try:
        summary = run_import(temp_path)
        return jsonify({'success': True, 'summary': summary})
    except Exception as error:
        return jsonify({'success': False, 'message': str(error)}), 500
    finally:
        os.remove(temp_path)
        os.rmdir(temp_dir)


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
