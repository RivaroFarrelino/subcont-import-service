import json
import logging
import os
import secrets
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps

import mysql.connector
import pandas as pd
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger('subcont-import')

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.environ.get('MAX_UPLOAD_MB', 1024)) * 1024 * 1024
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SECURE_COOKIE', 'false').lower() == 'true'
app.permanent_session_lifetime = timedelta(hours=int(os.environ.get('SESSION_HOURS', 12)))

SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
app.secret_key = SECRET_KEY

APP_USER = os.environ.get('APP_USER', 'admin')
APP_PASSWORD = os.environ.get('APP_PASSWORD', '')
API_TOKEN = os.environ.get('API_TOKEN', '')

LOGIN_MAX_ATTEMPTS = int(os.environ.get('LOGIN_MAX_ATTEMPTS', 5))
LOGIN_LOCK_SECONDS = int(os.environ.get('LOGIN_LOCK_SECONDS', 300))
login_attempts = {}
login_lock = threading.Lock()

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DATABASE', 'subcont'),
    'connection_timeout': int(os.environ.get('MYSQL_CONNECT_TIMEOUT', 30))
}

HEALTH_MYSQL_CONFIG = dict(MYSQL_CONFIG)
HEALTH_MYSQL_CONFIG['connection_timeout'] = int(os.environ.get('HEALTH_CONNECT_TIMEOUT', 3))
HEALTH_CACHE_SECONDS = int(os.environ.get('HEALTH_CACHE_SECONDS', 15))
health_state = {'waktu': 0.0, 'sehat': True}
health_lock = threading.Lock()

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 2000))
NUMERIC_TYPES = {'int', 'bigint', 'smallint', 'mediumint', 'tinyint', 'decimal', 'float', 'double', 'numeric'}
DATE_TYPES = {'date', 'datetime', 'timestamp'}
UPLOAD_DIR = os.environ.get('UPLOAD_DIR', '/files')
CHUNK_MB = int(os.environ.get('CHUNK_MB', 4))
CHUNK_MAX_AGE_HOURS = int(os.environ.get('CHUNK_MAX_AGE_HOURS', 6))
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
    'KEY idx_started (started_at)'
    ') ENGINE=InnoDB'
)

JOB_TABLE_PATCH = 'ALTER TABLE import_jobs ADD COLUMN notes TEXT NULL'

REPORT_INDEX_DDL = 'CREATE INDEX idx_report_fppp ON report (fppp_number)'
REPORT_INDEX_KACA = 'idx_report_assembly_packing'
MIGRASI_LOCK_NAME = 'subcont_migrasi_kolom'

KOLOM_GANTI_NAMA = [
    ('report', 'sealant_date', 'packing_date'),
    ('report', 'assembly_date', 'sealant_date'),
    ('report', 'cutting_date_2', 'assembly_date'),
    ('report', 'cutting_date_1', 'cutting_date'),
    ('data_awal', 'sash_qty', 'daun_qty')
]

TABLE_CONFIGS = [
    {
        'table': 'report',
        'sources': [
            {'sheet': 'Report', 'header_row': 2, 'usecols': list(range(38))},
            {'sheet': 'Monitoring', 'header_row': 2, 'usecols': list(range(38))}
        ],
        'columns': [
            'row_id', 'commission_number', 'position_number', 'piece_position', 'upload_date',
            'check_1', 'check_2', 'source_document', 'commission_number_dup', 'fppp_number',
            'customer_name', 'opening', 'position_name', 'color', 'unit_name', 'glass_type',
            'frame_qty', 'daun_qty', 'total_scan', 'code', 'barcode', 'cut_qty', 'cutting_qty',
            'cutting_date', 'assembly_date', 'assembly_user', 'assembly_scan_qty', 'sealant_date',
            'sealant_user', 'sealant_scan_qty', 'packing_date', 'packing_user', 'packing_standard_pack',
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
            'unit_code', 'upload_date', 'order_qty', 'frame_qty', 'daun_qty', 'check_value',
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

TOTAL_TABLES = len(TABLE_CONFIGS)


def alamat_pemanggil():
    diteruskan = request.headers.get('X-Forwarded-For', '')
    if diteruskan:
        return diteruskan.split(',')[0].strip()
    return request.remote_addr or 'tidak diketahui'


def sisa_kunci(alamat):
    with login_lock:
        catatan = login_attempts.get(alamat)
        if not catatan:
            return 0
        jumlah, sampai = catatan
        if jumlah < LOGIN_MAX_ATTEMPTS:
            return 0
        sisa = int(sampai - time.time())
        if sisa <= 0:
            login_attempts.pop(alamat, None)
            return 0
        return sisa


def catat_gagal(alamat):
    with login_lock:
        jumlah = login_attempts.get(alamat, (0, 0))[0] + 1
        login_attempts[alamat] = (jumlah, time.time() + LOGIN_LOCK_SECONDS)


def bersihkan_gagal(alamat):
    with login_lock:
        login_attempts.pop(alamat, None)


def kredensial_cocok(pengguna, sandi):
    if not APP_PASSWORD:
        return False
    return secrets.compare_digest(pengguna, APP_USER) and secrets.compare_digest(sandi, APP_PASSWORD)


def butuh_login(view):
    @wraps(view)
    def pembungkus(*args, **kwargs):
        if API_TOKEN and secrets.compare_digest(request.headers.get('X-API-Token', ''), API_TOKEN):
            return view(*args, **kwargs)
        if session.get('masuk'):
            return view(*args, **kwargs)
        if request.path == '/':
            return redirect(url_for('login_page'))
        return jsonify({'success': False, 'message': 'Belum login'}), 401
    return pembungkus


WIB = timezone(timedelta(hours=7))


def sekarang_wib():
    return datetime.now(WIB).replace(tzinfo=None)


def connect():
    return mysql.connector.connect(**MYSQL_CONFIG)


def ensure_job_table():
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(JOB_TABLE_DDL)
        connection.commit()
        try:
            cursor.execute(JOB_TABLE_PATCH)
            connection.commit()
            logger.info('kolom notes ditambahkan ke import_jobs')
        except Exception:
            pass
        cursor.close()
        connection.close()
        logger.info('tabel import_jobs siap')
    except Exception as error:
        logger.error('gagal menyiapkan tabel import_jobs: %s', error)


def info_kolom(cursor, table):
    cursor.execute(
        'SELECT column_name, column_type, is_nullable, data_type, character_maximum_length '
        'FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = %s',
        (table,)
    )
    return {str(baris[0]).lower(): baris for baris in cursor.fetchall()}


def ganti_nama_kolom(cursor, table, lama, baru, kolom):
    try:
        cursor.execute('ALTER TABLE `' + table + '` RENAME COLUMN `' + lama + '` TO `' + baru + '`')
    except mysql.connector.Error as error:
        logger.info('RENAME COLUMN %s.%s tidak didukung (%s), memakai CHANGE', table, lama, error)
        tipe = kolom[lama][1]
        nullable = 'NULL' if kolom[lama][2] == 'YES' else 'NOT NULL'
        cursor.execute('ALTER TABLE `' + table + '` CHANGE `' + lama + '` `' + baru + '` ' + tipe + ' ' + nullable)


def migrasi_kolom():
    connection = connect()
    cursor = connection.cursor()
    cursor.execute('SELECT GET_LOCK(%s, 60)', (MIGRASI_LOCK_NAME,))

    if cursor.fetchone()[0] != 1:
        cursor.close()
        connection.close()
        logger.warning('migrasi nama kolom dilewati, proses lain sedang memegang kunci')
        return 0

    diganti = 0

    try:
        for table, lama, baru in KOLOM_GANTI_NAMA:
            kolom = info_kolom(cursor, table)

            if lama in kolom and baru not in kolom:
                ganti_nama_kolom(cursor, table, lama, baru, kolom)
                connection.commit()
                diganti += 1
                logger.info('kolom %s.%s diganti jadi %s', table, lama, baru)

        logger.info('migrasi nama kolom selesai, %s kolom diganti', diganti)
        return diganti
    finally:
        cursor.execute('SELECT RELEASE_LOCK(%s)', (MIGRASI_LOCK_NAME,))
        cursor.fetchone()
        cursor.close()
        connection.close()


def ensure_migrasi_kolom():
    try:
        migrasi_kolom()
    except Exception as error:
        logger.error('migrasi nama kolom gagal: %s', error)


def ddl_index_kaca(cursor):
    kolom = info_kolom(cursor, 'report')
    user = kolom.get('assembly_user')

    if not user or 'packing_date' not in kolom:
        return None

    cursor.execute(
        'SELECT COUNT(*) FROM information_schema.statistics '
        'WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s',
        ('report', REPORT_INDEX_KACA)
    )

    if cursor.fetchone()[0] > 0:
        return None

    panjang = user[4] or 0
    prefix = '(50)' if user[3] in ('text', 'tinytext', 'mediumtext', 'longtext') or panjang > 255 else ''
    return 'CREATE INDEX ' + REPORT_INDEX_KACA + ' ON report (assembly_user' + prefix + ', packing_date)'


def bangun_report_index():
    try:
        connection = connect()
        cursor = connection.cursor()
        try:
            cursor.execute(REPORT_INDEX_DDL)
            connection.commit()
            logger.info('index idx_report_fppp selesai dibuat')
        except Exception as error:
            logger.info('index idx_report_fppp dilewati: %s', error)
        try:
            ddl = ddl_index_kaca(cursor)
            if ddl:
                cursor.execute(ddl)
                connection.commit()
                logger.info('index %s selesai dibuat', REPORT_INDEX_KACA)
            else:
                logger.info('index %s sudah ada atau kolom belum siap', REPORT_INDEX_KACA)
        except Exception as error:
            logger.info('index %s dilewati: %s', REPORT_INDEX_KACA, error)
        cursor.close()
        connection.close()
    except Exception as error:
        logger.info('pembuatan index report dilewati: %s', error)


def ensure_report_index():
    thread = threading.Thread(target=bangun_report_index, daemon=True)
    thread.start()
    logger.info('pembuatan index report dijalankan di latar belakang')


def resolve_upload_dir():
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        probe = os.path.join(UPLOAD_DIR, '.tulis_tes')
        with open(probe, 'w') as handle:
            handle.write('ok')
        os.remove(probe)
        return UPLOAD_DIR
    except Exception as error:
        fallback = tempfile.gettempdir()
        logger.warning('folder upload %s tidak bisa ditulis (%s), memakai %s', UPLOAD_DIR, error, fallback)
        return fallback


def chunk_root():
    return os.path.join(resolve_upload_dir(), '.bagian')


def chunk_path(upload_id):
    return os.path.join(chunk_root(), upload_id)


def id_sah(nilai):
    try:
        uuid.UUID(str(nilai))
        return True
    except Exception:
        return False


def nama_excel_sah(nama):
    return bool(nama) and nama.lower().endswith(('.xlsx', '.xlsm'))


def bersihkan_bagian_lama():
    akar = chunk_root()
    if not os.path.isdir(akar):
        return
    batas = time.time() - CHUNK_MAX_AGE_HOURS * 3600
    for nama in os.listdir(akar):
        jalur = os.path.join(akar, nama)
        try:
            if os.path.isdir(jalur) and os.path.getmtime(jalur) < batas:
                shutil.rmtree(jalur, ignore_errors=True)
                logger.info('sisa unggah lama dihapus: %s', nama)
        except Exception as error:
            logger.warning('gagal membersihkan sisa unggah %s: %s', nama, error)


def create_job(job_id, file_name):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(
        'INSERT INTO import_jobs (job_id, status, file_name, started_at) VALUES (%s, %s, %s, %s)',
        (job_id, 'queued', file_name, sekarang_wib())
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


def fetch_job(job_id):
    connection = connect()
    cursor = connection.cursor(dictionary=True)
    cursor.execute('SELECT * FROM import_jobs WHERE job_id = %s', (job_id,))
    job = cursor.fetchone()
    cursor.close()
    connection.close()
    return job


def fetch_recent_jobs(limit):
    connection = connect()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        'SELECT job_id, status, file_name, processed_rows, started_at, finished_at '
        'FROM import_jobs ORDER BY started_at DESC LIMIT %s',
        (limit,)
    )
    jobs = cursor.fetchall()
    cursor.close()
    connection.close()
    return jobs


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


def fetch_column_types(table):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s',
        (MYSQL_CONFIG['database'], table)
    )
    types = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}
    cursor.close()
    connection.close()
    return types


def coerce_types(dataframe, columns, column_types, table, notes):
    for column in columns:
        kind = column_types.get(column.lower())
        if kind in NUMERIC_TYPES:
            converted = pd.to_numeric(dataframe[column], errors='coerce')
        elif kind in DATE_TYPES:
            converted = pd.to_datetime(dataframe[column], errors='coerce')
        else:
            continue
        rusak = int((converted.isna() & dataframe[column].notna()).sum())
        if rusak:
            pesan = table + '.' + column + ': ' + str(rusak) + ' nilai tidak valid, dikosongkan'
            notes.append(pesan)
            logger.warning(pesan)
        dataframe[column] = converted
    return dataframe


def drop_invalid_keys(dataframe, key_columns, table, notes):
    before = len(dataframe)
    dataframe = dataframe.dropna(subset=key_columns)
    dibuang = before - len(dataframe)
    if dibuang:
        pesan = table + ': ' + str(dibuang) + ' baris dilewati karena kolom kunci kosong'
        notes.append(pesan)
        logger.warning(pesan)
    return dataframe


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def cari_sheet(excel_file, nama):
    if nama in excel_file.sheet_names:
        return nama
    target = str(nama).strip().lower()
    for tersedia in excel_file.sheet_names:
        if str(tersedia).strip().lower() == target:
            return tersedia
    return None


def read_sheet(excel_file, source, columns):
    nama = cari_sheet(excel_file, source['sheet'])
    if nama is None:
        logger.warning('sheet tidak ada, dilewati: %s', source['sheet'])
        return None
    if nama != source['sheet']:
        logger.info('sheet %s dicocokkan ke %s', source['sheet'], nama)
    return pd.read_excel(
        excel_file,
        sheet_name=nama,
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


def upsert_dataframe(cursor, table, columns, key_columns, dataframe, column_types, notes):
    dataframe = coerce_types(dataframe, columns, column_types, table, notes)
    dataframe = drop_invalid_keys(dataframe, key_columns, table, notes)
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
            finished_at=sekarang_wib()
        )
        return

    summary = {}
    notes = []
    total_processed = 0

    try:
        diganti = migrasi_kolom()
        if diganti:
            notes.append('migrasi nama kolom: ' + str(diganti) + ' kolom diganti')
        logger.info('job %s mulai, file %s', job_id, file_path)
        update_job(job_id, status='running')

        excel_file = pd.ExcelFile(file_path)
        logger.info('job %s sheet terdeteksi: %s', job_id, excel_file.sheet_names)

        for index, config in enumerate(TABLE_CONFIGS, start=1):
            table = config['table']
            update_job(job_id, current_table=table + ' (' + str(index) + '/' + str(TOTAL_TABLES) + ')')
            column_types = fetch_column_types(table)
            table_rows = 0

            for source in config['sources']:
                dataframe = read_sheet(excel_file, source, config['columns'])
                if dataframe is None:
                    continue
                rows = upsert_dataframe(
                    cursor, table, config['columns'], config['key_columns'],
                    dataframe, column_types, notes
                )
                del dataframe
                table_rows += rows
                logger.info('job %s tabel %s sheet %s selesai %s baris', job_id, table, source['sheet'], rows)

            connection.commit()
            summary[table] = table_rows
            total_processed += table_rows
            update_job(job_id, processed_rows=total_processed)
            logger.info('job %s tabel %s commit, akumulasi %s baris', job_id, table, total_processed)

            if table_rows == 0:
                dicari = ', '.join([s['sheet'] for s in config['sources']])
                pesan = table + ': tidak ada baris yang masuk, sheet yang dicari: ' + dicari
                notes.append(pesan)
                logger.warning('job %s %s', job_id, pesan)

        excel_file.close()
        update_job(
            job_id,
            status='success',
            summary=json.dumps(summary),
            notes=json.dumps(notes),
            processed_rows=total_processed,
            current_table=None,
            finished_at=sekarang_wib()
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
            notes=json.dumps(notes),
            current_table=None,
            finished_at=sekarang_wib()
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


def start_job(file_path, file_name, temp_dir):
    job_id = str(uuid.uuid4())
    create_job(job_id, file_name)
    thread = threading.Thread(target=import_worker, args=(job_id, file_path, temp_dir), daemon=True)
    thread.start()
    return job_id


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    alamat = alamat_pemanggil()

    if not APP_PASSWORD:
        return render_template('login.html', pesan='APP_PASSWORD belum diatur di server. Hubungi tim IT.'), 503

    if request.method == 'POST':
        terkunci = sisa_kunci(alamat)
        if terkunci:
            logger.warning('login diblokir sementara untuk %s, sisa %s detik', alamat, terkunci)
            return render_template(
                'login.html',
                pesan='Terlalu banyak percobaan gagal. Coba lagi dalam ' + str(terkunci) + ' detik.'
            ), 429

        pengguna = request.form.get('pengguna', '')
        sandi = request.form.get('sandi', '')

        if kredensial_cocok(pengguna, sandi):
            bersihkan_gagal(alamat)
            session.permanent = True
            session['masuk'] = pengguna
            logger.info('login berhasil: %s dari %s', pengguna, alamat)
            return redirect(url_for('upload_page'))

        catat_gagal(alamat)
        logger.warning('login gagal untuk pengguna %s dari %s', pengguna, alamat)
        return render_template('login.html', pesan='Nama pengguna atau kata sandi salah.'), 401

    if session.get('masuk'):
        return redirect(url_for('upload_page'))

    return render_template('login.html', pesan=None)


@app.route('/logout', methods=['GET'])
def logout_page():
    pengguna = session.get('masuk')
    session.clear()
    if pengguna:
        logger.info('logout: %s', pengguna)
    return redirect(url_for('login_page'))


@app.route('/', methods=['GET'])
@butuh_login
def upload_page():
    return render_template('index.html', pengguna=session.get('masuk'))


@app.route('/import', methods=['POST'])
@butuh_login
def handle_import():
    if is_import_running():
        logger.warning('permintaan import ditolak, ada import yang sedang berjalan')
        return jsonify({'success': False, 'message': 'Import lain sedang berjalan, tunggu sampai selesai'}), 409

    temp_dir = None

    if 'file' in request.files:
        uploaded_file = request.files['file']
        if not uploaded_file.filename:
            return jsonify({'success': False, 'message': 'File belum dipilih'}), 400

        file_name = secure_filename(uploaded_file.filename)
        if not file_name.lower().endswith(('.xlsx', '.xlsm')):
            return jsonify({'success': False, 'message': 'File harus berformat .xlsx atau .xlsm'}), 400

        target_dir = resolve_upload_dir()
        file_path = os.path.join(target_dir, file_name)
        uploaded_file.save(file_path)
        logger.info('menerima upload %s ke %s', file_name, file_path)
    else:
        payload = request.get_json(silent=True) or {}
        file_path = payload.get('file_path')
        if not file_path:
            return jsonify({
                'success': False,
                'message': 'Kirim file lewat multipart field "file" atau JSON {"file_path": "..."}'
            }), 400
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': 'File tidak ditemukan: ' + file_path}), 400
        file_name = os.path.basename(file_path)
        logger.info('memakai file yang sudah ada %s', file_path)

    try:
        job_id = start_job(file_path, file_name, temp_dir)
    except Exception as error:
        logger.error('gagal memulai job: %s', error)
        return jsonify({'success': False, 'message': str(error)}), 500

    return jsonify({'success': True, 'job_id': job_id, 'status': 'queued'}), 202


@app.route('/upload/init', methods=['POST'])
@butuh_login
def upload_init():
    if is_import_running():
        return jsonify({'success': False, 'message': 'Import lain sedang berjalan, tunggu sampai selesai'}), 409

    payload = request.get_json(silent=True) or {}
    nama = secure_filename(payload.get('file_name', ''))

    if not nama_excel_sah(nama):
        return jsonify({'success': False, 'message': 'File harus berformat .xlsx atau .xlsm'}), 400

    bersihkan_bagian_lama()
    upload_id = str(uuid.uuid4())

    try:
        os.makedirs(chunk_path(upload_id), exist_ok=True)
    except Exception as error:
        logger.error('gagal menyiapkan folder unggah: %s', error)
        return jsonify({'success': False, 'message': str(error)}), 500

    logger.info('unggah %s dimulai untuk %s', upload_id, nama)
    return jsonify({'success': True, 'upload_id': upload_id, 'chunk_size': CHUNK_MB * 1024 * 1024})


@app.route('/upload/chunk', methods=['POST'])
@butuh_login
def upload_chunk():
    upload_id = request.form.get('upload_id', '')
    index = request.form.get('index', '')

    if not id_sah(upload_id) or not index.isdigit():
        return jsonify({'success': False, 'message': 'Permintaan tidak sah'}), 400

    folder = chunk_path(upload_id)
    if not os.path.isdir(folder):
        return jsonify({'success': False, 'message': 'Sesi unggah tidak ditemukan atau sudah kedaluwarsa'}), 404

    bagian = request.files.get('chunk')
    if bagian is None:
        return jsonify({'success': False, 'message': 'Potongan file tidak ditemukan'}), 400

    bagian.save(os.path.join(folder, index.zfill(6)))
    return jsonify({'success': True, 'index': int(index)})


@app.route('/upload/finish', methods=['POST'])
@butuh_login
def upload_finish():
    if is_import_running():
        return jsonify({'success': False, 'message': 'Import lain sedang berjalan, tunggu sampai selesai'}), 409

    payload = request.get_json(silent=True) or {}
    upload_id = payload.get('upload_id', '')
    nama = secure_filename(payload.get('file_name', ''))
    total = payload.get('total', 0)

    if not id_sah(upload_id) or not nama_excel_sah(nama) or not isinstance(total, int) or total < 1:
        return jsonify({'success': False, 'message': 'Permintaan tidak sah'}), 400

    folder = chunk_path(upload_id)
    if not os.path.isdir(folder):
        return jsonify({'success': False, 'message': 'Sesi unggah tidak ditemukan atau sudah kedaluwarsa'}), 404

    hilang = [i for i in range(total) if not os.path.exists(os.path.join(folder, str(i).zfill(6)))]
    if hilang:
        logger.error('unggah %s tidak lengkap, potongan hilang: %s', upload_id, hilang[:10])
        return jsonify({
            'success': False,
            'message': 'Unggahan tidak lengkap, ' + str(len(hilang)) + ' potongan hilang. Coba ulangi.'
        }), 400

    target = os.path.join(resolve_upload_dir(), nama)

    try:
        with open(target, 'wb') as keluaran:
            for i in range(total):
                with open(os.path.join(folder, str(i).zfill(6)), 'rb') as masukan:
                    shutil.copyfileobj(masukan, keluaran, 1024 * 1024)
        logger.info('unggah %s digabung jadi %s (%s potongan)', upload_id, target, total)
    except Exception as error:
        logger.error('gagal menggabung unggah %s: %s', upload_id, error)
        return jsonify({'success': False, 'message': str(error)}), 500
    finally:
        shutil.rmtree(folder, ignore_errors=True)

    try:
        job_id = start_job(target, nama, None)
    except Exception as error:
        logger.error('gagal memulai job setelah unggah %s: %s', upload_id, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    return jsonify({'success': True, 'job_id': job_id, 'status': 'queued'}), 202


FPPP_KOLOM_ANGKA = [
    'frame_qty', 'daun_qty', 'total_scan', 'cut_qty', 'cutting_qty',
    'assembly_scan_qty', 'sealant_scan_qty', 'packing_scan_qty'
]
FPPP_BAGIAN_DASAR = 5
FPPP_BARIS_MAKS = 500


def fppp_base(nomor):
    bagian = str(nomor).split('/')
    if len(bagian) <= FPPP_BAGIAN_DASAR:
        return '/'.join(bagian)
    return '/'.join(bagian[:FPPP_BAGIAN_DASAR])


def fppp_lengkap(nomor):
    return len(str(nomor).split('/')) > FPPP_BAGIAN_DASAR


def escape_like(teks):
    return teks.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


@app.route('/fppp', methods=['POST'])
@butuh_login
def fppp_lookup():
    payload = request.get_json(silent=True) or {}
    nomor = str(payload.get('fppp_number', '')).strip()
    opening = str(payload.get('opening', '')).strip()
    sertakan_baris = bool(payload.get('include_rows'))

    if not nomor:
        return jsonify({'success': False, 'message': 'fppp_number wajib diisi'}), 400

    dasar = fppp_base(nomor)
    eksak = fppp_lengkap(nomor)

    agregat = ', '.join(['SUM(COALESCE(' + k + ', 0)) AS ' + k for k in FPPP_KOLOM_ANGKA])
    identitas = (
        "LEFT(GROUP_CONCAT(DISTINCT project_code ORDER BY project_code SEPARATOR ', '), 200) AS project_code, "
        "LEFT(GROUP_CONCAT(DISTINCT customer_name ORDER BY customer_name SEPARATOR ', '), 200) AS customer_name"
    )
    sql = 'SELECT fppp_number, COUNT(*) AS found, ' + identitas + ', ' + agregat + ' FROM report WHERE '

    if eksak:
        sql += 'fppp_number = %s'
        params = [nomor]
    else:
        sql += '(fppp_number = %s OR fppp_number LIKE %s)'
        params = [dasar, escape_like(dasar) + '/%']

    if opening:
        sql += ' AND LOWER(TRIM(position_name)) = LOWER(%s)'
        params.append(opening)

    sql += ' GROUP BY fppp_number ORDER BY fppp_number'

    try:
        connection = connect()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql, params)
        hasil = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('lookup fppp %s gagal: %s', nomor, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    variants = []
    for row in hasil:
        totals = {}
        for k in FPPP_KOLOM_ANGKA:
            nilai = row.get(k)
            totals[k] = float(nilai) if nilai is not None else 0.0
        variants.append({
            'fppp_number': row['fppp_number'],
            'found': int(row['found']),
            'project_code': row.get('project_code') or '',
            'customer_name': row.get('customer_name') or '',
            'totals': totals
        })

    logger.info(
        'lookup fppp input=%s dasar=%s eksak=%s position_name=%s varian=%s',
        nomor, dasar, eksak, opening or '-', len(variants)
    )

    balasan = {
        'success': True,
        'input': nomor,
        'base': dasar,
        'opening': opening,
        'exact_input': eksak,
        'variant_count': len(variants),
        'variants': variants,
        'resolved': len(variants) == 1,
        'ambiguous': len(variants) > 1,
        'fppp_number': variants[0]['fppp_number'] if len(variants) == 1 else None,
        'found': variants[0]['found'] if len(variants) == 1 else 0,
        'totals': variants[0]['totals'] if len(variants) == 1 else None
    }

    if len(variants) > 1:
        balasan['message'] = (
            'Nomor FPPP ' + dasar + ' ada di ' + str(len(variants)) + ' proyek: '
            + ' | '.join([
                v['fppp_number'] + ' [' + v['project_code'] + ' - ' + v['customer_name'] + ']'
                for v in variants
            ])
        )
        logger.warning('lookup fppp ambigu: %s', balasan['message'])

    if sertakan_baris and len(variants) == 1:
        try:
            connection = connect()
            cursor = connection.cursor(dictionary=True)
            sql_baris = (
                'SELECT position_number, position_name, opening, '
                + ', '.join(FPPP_KOLOM_ANGKA)
                + ' FROM report WHERE fppp_number = %s'
            )
            params_baris = [variants[0]['fppp_number']]
            if opening:
                sql_baris += ' AND LOWER(TRIM(position_name)) = LOWER(%s)'
                params_baris.append(opening)
            sql_baris += ' LIMIT ' + str(FPPP_BARIS_MAKS)
            cursor.execute(sql_baris, params_baris)
            balasan['rows'] = cursor.fetchall()
            cursor.close()
            connection.close()
        except Exception as error:
            logger.error('lookup fppp baris %s gagal: %s', nomor, error)

    return jsonify(balasan)


UNIT_MAKS_BARIS = 5000


@app.route('/fppp-units', methods=['POST'])
@butuh_login
def fppp_units():
    payload = request.get_json(silent=True) or {}
    nomor = str(payload.get('fppp_number', '')).strip()

    if not nomor:
        return jsonify({'success': False, 'message': 'fppp_number wajib diisi'}), 400

    dasar = fppp_base(nomor)
    eksak = fppp_lengkap(nomor)
    sql = (
        'SELECT fppp_number, opening, position_name, COUNT(*) AS baris FROM report WHERE '
        + ('fppp_number = %s' if eksak else '(fppp_number = %s OR fppp_number LIKE %s)')
        + ' GROUP BY fppp_number, opening, position_name ORDER BY opening, position_name LIMIT ' + str(UNIT_MAKS_BARIS)
    )
    params = [nomor] if eksak else [dasar, escape_like(dasar) + '/%']
    mulai = time.time()

    try:
        connection = connect()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql, params)
        hasil = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('unit fppp %s gagal: %s', nomor, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    varian = sorted(set(str(r['fppp_number'] or '') for r in hasil))
    unik = {}
    for r in hasil:
        posisi = str(r['position_name'] or '').strip()
        if not posisi:
            continue
        opening = str(r['opening'] or '').strip() or posisi.split('.')[0]
        unik[(opening.upper(), posisi.upper())] = {'opening': opening, 'position_name': posisi}

    units = sorted(unik.values(), key=lambda u: (u['opening'].upper(), u['position_name'].upper()))
    logger.info('unit fppp input=%s dasar=%s varian=%s unit=%s %.0fms', nomor, dasar, len(varian), len(units), (time.time() - mulai) * 1000)

    return jsonify({'success': True, 'input': nomor, 'base': dasar, 'variants': varian, 'units': units})


PACKING_MAKS_FPPP = 500
PACKING_PER_QUERY = 100


def teks_tanggal(nilai):
    if nilai is None:
        return None
    if hasattr(nilai, 'isoformat'):
        return nilai.isoformat()[:10]
    teks = str(nilai).strip()
    return teks[:10] if teks else None


@app.route('/packing-status', methods=['POST'])
@butuh_login
def packing_status():
    payload = request.get_json(silent=True) or {}
    daftar = payload.get('fppp_numbers') or []

    if not isinstance(daftar, list):
        return jsonify({'success': False, 'message': 'fppp_numbers harus berupa list'}), 400

    dasar = []
    terlihat = set()
    for nomor in daftar:
        b = fppp_base(str(nomor).strip()).upper()
        if b and b not in terlihat:
            terlihat.add(b)
            dasar.append(b)

    if len(dasar) > PACKING_MAKS_FPPP:
        return jsonify({'success': False, 'message': 'maksimal ' + str(PACKING_MAKS_FPPP) + ' FPPP sekali cek'}), 400

    hasil = {b: {'baris': 0, 'dipacking': 0, 'pertama': None, 'terakhir': None} for b in dasar}

    if not dasar:
        return jsonify({'success': True, 'results': hasil})

    mulai = time.time()

    try:
        connection = connect()
        cursor = connection.cursor(dictionary=True)

        for i in range(0, len(dasar), PACKING_PER_QUERY):
            potong = dasar[i:i + PACKING_PER_QUERY]
            kondisi = ' OR '.join(['fppp_number LIKE %s'] * len(potong))
            params = [escape_like(b) + '%' for b in potong]
            cursor.execute(
                'SELECT fppp_number, COUNT(*) AS baris, SUM(packing_date IS NOT NULL) AS dipacking, '
                'MIN(packing_date) AS pertama, MAX(packing_date) AS terakhir '
                'FROM report WHERE ' + kondisi + ' GROUP BY fppp_number',
                params
            )
            for row in cursor.fetchall():
                nomor = str(row['fppp_number'] or '').strip().upper()
                cocok = [b for b in potong if nomor.startswith(b)]
                if not cocok:
                    continue
                h = hasil[max(cocok, key=len)]
                h['baris'] += int(row['baris'] or 0)
                h['dipacking'] += int(row['dipacking'] or 0)
                pertama = teks_tanggal(row['pertama'])
                terakhir = teks_tanggal(row['terakhir'])
                if pertama and (h['pertama'] is None or pertama < h['pertama']):
                    h['pertama'] = pertama
                if terakhir and (h['terakhir'] is None or terakhir > h['terakhir']):
                    h['terakhir'] = terakhir

        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('cek packing %s FPPP gagal: %s', len(dasar), error)
        return jsonify({'success': False, 'message': str(error)}), 500

    sudah = sum(1 for h in hasil.values() if h['dipacking'] > 0)
    logger.info('cek packing fppp=%s ada_data=%s sudah_packing=%s %.0fms', len(dasar), sum(1 for h in hasil.values() if h['baris'] > 0), sudah, (time.time() - mulai) * 1000)

    return jsonify({'success': True, 'results': hasil})


@app.route('/status/<job_id>', methods=['GET'])
@butuh_login
def job_status(job_id):
    try:
        job = fetch_job(job_id)
    except Exception as error:
        logger.error('gagal membaca status job %s: %s', job_id, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    if job is None:
        return jsonify({'success': False, 'message': 'Job tidak ditemukan'}), 404

    if job.get('summary'):
        job['summary'] = json.loads(job['summary'])
    if job.get('notes'):
        job['notes'] = json.loads(job['notes'])

    return jsonify({'success': True, 'job': job})


@app.route('/jobs', methods=['GET'])
@butuh_login
def job_list():
    try:
        jobs = fetch_recent_jobs(int(request.args.get('limit', 10)))
    except Exception as error:
        logger.error('gagal membaca daftar job: %s', error)
        return jsonify({'success': False, 'message': str(error)}), 500

    return jsonify({'success': True, 'jobs': jobs})


def balasan_health(sehat):
    if sehat:
        return jsonify({'status': 'ok', 'database': 'connected'})
    return jsonify({'status': 'degraded', 'database': 'tidak bisa dihubungi'}), 503


@app.route('/health', methods=['GET'])
def health_check():
    with health_lock:
        if time.time() - health_state['waktu'] < HEALTH_CACHE_SECONDS:
            return balasan_health(health_state['sehat'])

    sehat = True

    try:
        connection = mysql.connector.connect(**HEALTH_MYSQL_CONFIG)
        connection.close()
    except Exception as error:
        sehat = False
        logger.error('health check gagal: %s', error)

    with health_lock:
        health_state['waktu'] = time.time()
        health_state['sehat'] = sehat

    return balasan_health(sehat)


if not APP_PASSWORD:
    logger.error('APP_PASSWORD belum diatur, halaman tidak bisa diakses siapa pun')
if not os.environ.get('SECRET_KEY'):
    logger.warning('SECRET_KEY belum diatur, semua sesi login akan hangus setiap kali container restart')

ensure_job_table()
ensure_migrasi_kolom()
ensure_report_index()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
