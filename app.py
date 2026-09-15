import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import traceback
import uuid
from datetime import datetime

import mysql.connector
import pandas as pd
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger('subcont-import')

app = Flask(__name__)

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DATABASE', 'subcont'),
    'connection_timeout': int(os.environ.get('MYSQL_CONNECT_TIMEOUT', 30))
}

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 2000))
IMPORT_LOCK_NAME = 'subcont_import'

JOB_TABLE_DDL = (
    'CREATE TABLE IF NOT EXISTS import_jobs ('
    'job_id VARCHAR(36) NOT NULL, '
    'status VARCHAR(20) NOT NULL, '
    'file_name VARCHAR(255) NULL, '
    'current_table VARCHAR(64) NULL, '
    'processed_rows INT NOT NULL DEFAULT 0, '
    'summary TEXT NULL, '
    'error_message TEXT NULL, '
    'started_at DATETIME NULL, '
    'finished_at DATETIME NULL, '
    'PRIMARY KEY (job_id), '
    'KEY idx_status (status)'
    ') ENGINE=InnoDB'
)

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


def connect():
    return mysql.connector.connect(**MYSQL_CONFIG)


def ensure_job_table():
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(JOB_TABLE_DDL)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info('tabel import_jobs siap')
    except Exception as error:
        logger.error('gagal menyiapkan tabel import_jobs: %s', error)


def create_job(job_id, file_name):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(
        'INSERT INTO import_jobs (job_id, status, file_name, started_at) VALUES (%s, %s, %s, %s)',
        (job_id, 'queued', file_name, datetime.now())
    )
    connection.commit()
    cursor.close()
    connection.close()


def update_job(job_id, **fields):
    if not fields:
        return
    assignments = ', '.join([key + ' = %s' for key in fields])
    values = list(fields.values()) + [job_id]
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute('UPDATE import_jobs SET ' + assignments + ' WHERE job_id = %s', values)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('job %s gagal update status: %s', job_id, error)


def is_import_running():
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute('SELECT IS_USED_LOCK(%s)', (IMPORT_LOCK_NAME,))
        result = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        return result is not None
    except Exception as error:
        logger.error('gagal cek lock import: %s', error)
        return False


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def read_sheet(excel_file, source, columns):
    if source['sheet'] not in excel_file.sheet_names:
        logger.warning('sheet tidak ada, dilewati: %s', source['sheet'])
        return None
    return pd.read_excel(
        excel_file,
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
    batch = []
    processed = 0

    for row in dataframe.itertuples(index=False, name=None):
        batch.append(tuple(clean_value(value) for value in row))
        if len(batch) >= BATCH_SIZE:
            cursor.executemany(sql, batch)
            processed += len(batch)
            batch = []
            logger.info('tabel %s progres %s baris', table, processed)

    if batch:
        cursor.executemany(sql, batch)
        processed += len(batch)

    return processed


def run_import(job_id, file_path):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute('SELECT GET_LOCK(%s, 0)', (IMPORT_LOCK_NAME,))
    acquired = cursor.fetchone()[0]

    if acquired != 1:
        cursor.close()
        connection.close()
        logger.warning('job %s dilewati, ada import lain yang sedang berjalan', job_id)
        update_job(
            job_id,
            status='skipped',
            error_message='Import lain sedang berjalan',
            finished_at=datetime.now()
        )
        return

    summary = {}
    total_processed = 0

    try:
        logger.info('job %s mulai, file %s', job_id, file_path)
        update_job(job_id, status='running')

        excel_file = pd.ExcelFile(file_path)
        logger.info('job %s sheet terdeteksi: %s', job_id, excel_file.sheet_names)

        for config in TABLE_CONFIGS:
            table = config['table']
            update_job(job_id, current_table=table)
            table_rows = 0

            for source in config['sources']:
                dataframe = read_sheet(excel_file, source, config['columns'])
                if dataframe is None:
                    continue
                rows = upsert_dataframe(cursor, table, config['columns'], config['key_columns'], dataframe)
                del dataframe
                table_rows += rows
                logger.info('job %s tabel %s sheet %s selesai %s baris', job_id, table, source['sheet'], rows)

            connection.commit()
            summary[table] = table_rows
            total_processed += table_rows
            update_job(job_id, processed_rows=total_processed)
            logger.info('job %s tabel %s commit, akumulasi %s baris', job_id, table, total_processed)

        excel_file.close()
        update_job(
            job_id,
            status='success',
            summary=json.dumps(summary),
            processed_rows=total_processed,
            current_table=None,
            finished_at=datetime.now()
        )
        logger.info('job %s selesai, total %s baris', job_id, total_processed)
    except Exception as error:
        connection.rollback()
        logger.error('job %s gagal: %s', job_id, traceback.format_exc())
        update_job(
            job_id,
            status='failed',
            error_message=str(error),
            summary=json.dumps(summary),
            finished_at=datetime.now()
        )
    finally:
        cursor.execute('SELECT RELEASE_LOCK(%s)', (IMPORT_LOCK_NAME,))
        cursor.fetchall()
        cursor.close()
        connection.close()
        logger.info('job %s lock dilepas', job_id)


def import_worker(job_id, file_path, temp_dir):
    try:
        run_import(job_id, file_path)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info('job %s file sementara dihapus', job_id)


@app.route('/import', methods=['POST'])
def handle_import():
    if is_import_running():
        logger.warning('permintaan import ditolak, ada import yang sedang berjalan')
        return jsonify({'success': False, 'message': 'Import lain sedang berjalan'}), 409

    job_id = str(uuid.uuid4())
    temp_dir = None

    if 'file' in request.files:
        uploaded_file = request.files['file']
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, uploaded_file.filename)
        uploaded_file.save(file_path)
        file_name = uploaded_file.filename
        logger.info('job %s menerima upload %s', job_id, file_name)
    else:
        payload = request.get_json(silent=True) or {}
        file_path = payload.get('file_path')
        if not file_path:
            return jsonify({
                'success': False,
                'message': 'Kirim file lewat multipart field "file" atau kirim JSON {"file_path": "..."}'
            }), 400
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': 'File tidak ditemukan: ' + file_path}), 400
        file_name = os.path.basename(file_path)
        logger.info('job %s memakai file yang sudah ada %s', job_id, file_path)

    try:
        create_job(job_id, file_name)
    except Exception as error:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
        logger.error('job %s gagal dibuat: %s', job_id, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    thread = threading.Thread(target=import_worker, args=(job_id, file_path, temp_dir), daemon=True)
    thread.start()

    return jsonify({'success': True, 'job_id': job_id, 'status': 'queued'}), 202


@app.route('/status/<job_id>', methods=['GET'])
def job_status(job_id):
    try:
        connection = connect()
        cursor = connection.cursor(dictionary=True)
        cursor.execute('SELECT * FROM import_jobs WHERE job_id = %s', (job_id,))
        job = cursor.fetchone()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('gagal membaca status job %s: %s', job_id, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    if job is None:
        return jsonify({'success': False, 'message': 'Job tidak ditemukan'}), 404

    if job.get('summary'):
        job['summary'] = json.loads(job['summary'])

    return jsonify({'success': True, 'job': job})


@app.route('/health', methods=['GET'])
def health_check():
    try:
        connection = connect()
        connection.close()
        return jsonify({'status': 'ok', 'database': 'connected'})
    except Exception as error:
        logger.error('health check gagal: %s', error)
        return jsonify({'status': 'degraded', 'database': str(error)}), 503


ensure_job_table()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
