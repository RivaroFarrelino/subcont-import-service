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
from datetime import datetime, timedelta
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

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 2000))
NUMERIC_TYPES = {'int', 'bigint', 'smallint', 'mediumint', 'tinyint', 'decimal', 'float', 'double', 'numeric'}
DATE_TYPES = {'date', 'datetime', 'timestamp'}
UPLOAD_DIR = os.environ.get('UPLOAD_DIR', '/files')
CHUNK_MB = int(os.environ.get('CHUNK_MB', 40))
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
            finished_at=datetime.now()
        )
        return

    summary = {}
    notes = []
    total_processed = 0

    try:
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
            notes=json.dumps(notes),
            current_table=None,
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


FPPP_KOLOM_KUNCI = ['position_number', 'position_name', 'opening']
FPPP_KOLOM_ANGKA = [
    'frame_qty', 'sash_qty', 'total_scan', 'cut_qty', 'cutting_qty',
    'assembly_scan_qty', 'sealant_scan_qty', 'packing_scan_qty'
]


@app.route('/fppp', methods=['POST'])
@butuh_login
def fppp_lookup():
    payload = request.get_json(silent=True) or {}
    nomor = str(payload.get('fppp_number', '')).strip()
    opening = str(payload.get('opening', '')).strip()

    if not nomor:
        return jsonify({'success': False, 'message': 'fppp_number wajib diisi'}), 400

    kolom = FPPP_KOLOM_KUNCI + FPPP_KOLOM_ANGKA
    sql = 'SELECT ' + ', '.join(kolom) + ' FROM report WHERE fppp_number = %s'
    params = [nomor]

    if opening:
        sql += ' AND opening = %s'
        params.append(opening)

    try:
        connection = connect()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql, params)
        baris = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('lookup fppp %s gagal: %s', nomor, error)
        return jsonify({'success': False, 'message': str(error)}), 500

    totals = {}
    for nama in FPPP_KOLOM_ANGKA:
        jumlah = 0
        for b in baris:
            nilai = b.get(nama)
            if nilai is not None:
                jumlah += float(nilai)
        totals[nama] = jumlah

    logger.info('lookup fppp %s opening %s menemukan %s baris', nomor, opening or '-', len(baris))

    return jsonify({
        'success': True,
        'fppp_number': nomor,
        'opening': opening,
        'found': len(baris),
        'totals': totals,
        'rows': baris
    })


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


@app.route('/health', methods=['GET'])
def health_check():
    try:
        connection = connect()
        connection.close()
        return jsonify({'status': 'ok', 'database': 'connected'})
    except Exception as error:
        logger.error('health check gagal: %s', error)
        return jsonify({'status': 'degraded', 'database': str(error)}), 503


if not APP_PASSWORD:
    logger.error('APP_PASSWORD belum diatur, halaman tidak bisa diakses siapa pun')
if not os.environ.get('SECRET_KEY'):
    logger.warning('SECRET_KEY belum diatur, semua sesi login akan hangus setiap kali container restart')

ensure_job_table()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
