import base64
import json
import logging
import os
import secrets
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import wraps
from urllib.parse import quote

import mysql.connector
import requests
from flask import Blueprint, jsonify, request

logger = logging.getLogger('inventory-valuation')

iv_bp = Blueprint('inventory_valuation', __name__, url_prefix='/iv')

API_TOKEN = os.environ.get('IV_API_TOKEN') or os.environ.get('API_TOKEN', '')

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('IV_MYSQL_DATABASE') or os.environ.get('MYSQL_DATABASE', 'subcont'),
    'connection_timeout': int(os.environ.get('MYSQL_CONNECT_TIMEOUT', 30))
}

BC_TENANT = os.environ.get('BC_TENANT_ID', '77f799ec-1818-473a-8c6a-62ce5f3b285f')
BC_CLIENT_ID = os.environ.get('BC_CLIENT_ID', '')
BC_CLIENT_SECRET = os.environ.get('BC_CLIENT_SECRET', '')
BC_ENVIRONMENT = os.environ.get('BC_ENVIRONMENT', 'Production')
BC_COMPANY = os.environ.get('BC_COMPANY', 'PT ALLURE ALLUMINIO')
BC_ILE_ENDPOINT = os.environ.get('BC_ILE_ENDPOINT', 'ItemLedgerEntries')
BC_ITEM_ENDPOINT = os.environ.get('BC_ITEM_ENDPOINT', 'itemslist')
BC_PAGE_SIZE = int(os.environ.get('BC_PAGE_SIZE', 5000))
BC_TIMEOUT = int(os.environ.get('BC_TIMEOUT', 600))
BC_MAX_RETRY = int(os.environ.get('BC_MAX_RETRY', 5))
BC_RETRY_MS = int(os.environ.get('BC_RETRY_MS', 5000))

IV_START_MONTH = os.environ.get('IV_START_MONTH', '2023-01')
IV_REFRESH_MONTHS = int(os.environ.get('IV_REFRESH_MONTHS', 3))
IV_INSERT_BATCH = int(os.environ.get('IV_INSERT_BATCH', 2000))
IV_MAX_LOKASI = int(os.environ.get('IV_MAX_LOKASI', 300))
IV_SYNC_LOCK = 'iv_sync'

WIB = timezone(timedelta(hours=7))
NOL_QTY = Decimal('0.000001')
NOL_NILAI = Decimal('0.005')

DDL = [
    'CREATE TABLE IF NOT EXISTS iv_mutasi_bulanan ('
    'location_code VARCHAR(20) NOT NULL, '
    'bulan DATE NOT NULL, '
    'item_no VARCHAR(30) NOT NULL, '
    'qty_masuk DECIMAL(24,5) NOT NULL, '
    'nilai_masuk DECIMAL(24,4) NOT NULL, '
    'qty_keluar DECIMAL(24,5) NOT NULL, '
    'nilai_keluar DECIMAL(24,4) NOT NULL, '
    'PRIMARY KEY (location_code, bulan, item_no), '
    'KEY idx_iv_mutasi_bulan (bulan)'
    ') ENGINE=InnoDB',
    'CREATE TABLE IF NOT EXISTS iv_saldo_tahunan ('
    'tahun SMALLINT NOT NULL, '
    'location_code VARCHAR(20) NOT NULL, '
    'item_no VARCHAR(30) NOT NULL, '
    'qty DECIMAL(24,5) NOT NULL, '
    'nilai DECIMAL(24,4) NOT NULL, '
    'PRIMARY KEY (tahun, location_code, item_no)'
    ') ENGINE=InnoDB',
    'CREATE TABLE IF NOT EXISTS iv_tahun_status ('
    'tahun SMALLINT NOT NULL, '
    'kotor TINYINT NOT NULL, '
    'updated_at DATETIME NOT NULL, '
    'PRIMARY KEY (tahun)'
    ') ENGINE=InnoDB',
    'CREATE TABLE IF NOT EXISTS iv_item ('
    'item_no VARCHAR(30) NOT NULL, '
    'description VARCHAR(250) NULL, '
    'uom VARCHAR(20) NULL, '
    'updated_at DATETIME NOT NULL, '
    'PRIMARY KEY (item_no)'
    ') ENGINE=InnoDB',
    'CREATE TABLE IF NOT EXISTS iv_bulan ('
    'bulan DATE NOT NULL, '
    'entri INT NOT NULL, '
    'baris INT NOT NULL, '
    'job_id VARCHAR(36) NULL, '
    'synced_at DATETIME NOT NULL, '
    'PRIMARY KEY (bulan)'
    ') ENGINE=InnoDB',
    'CREATE TABLE IF NOT EXISTS iv_jobs ('
    'job_id VARCHAR(36) NOT NULL, '
    'mode VARCHAR(20) NOT NULL, '
    'status VARCHAR(20) NOT NULL, '
    'bulan_awal DATE NULL, '
    'bulan_akhir DATE NULL, '
    'bulan_berjalan DATE NULL, '
    'bulan_selesai INT NOT NULL DEFAULT 0, '
    'total_bulan INT NOT NULL DEFAULT 0, '
    'total_entri BIGINT NOT NULL DEFAULT 0, '
    'pesan TEXT NULL, '
    'mulai DATETIME NULL, '
    'selesai DATETIME NULL, '
    'PRIMARY KEY (job_id), '
    'KEY idx_iv_jobs_mulai (mulai)'
    ') ENGINE=InnoDB'
]

TABEL_LAMA = ['iv_daily', 'iv_closing']


class GalatBC(Exception):
    pass


class GalatInput(Exception):
    pass


def sekarang_wib():
    return datetime.now(WIB).replace(tzinfo=None)


def connect():
    return mysql.connector.connect(**MYSQL_CONFIG)


def awal_bulan(tanggal):
    return tanggal.replace(day=1)


def geser_bulan(bulan, jumlah):
    indeks = bulan.year * 12 + bulan.month - 1 + jumlah
    return date(indeks // 12, indeks % 12 + 1, 1)


def daftar_bulan(dari, sampai):
    hasil = []
    bulan = awal_bulan(dari)
    while bulan <= sampai:
        hasil.append(bulan)
        bulan = geser_bulan(bulan, 1)
    return hasil


def parse_bulan(teks):
    try:
        return datetime.strptime(str(teks).strip()[:7], '%Y-%m').date()
    except ValueError:
        raise GalatInput('Format bulan harus YYYY-MM')


def akhir_bulan(bulan):
    return geser_bulan(bulan, 1) - timedelta(days=1)


def siapkan_inventory():
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN (%s, %s)',
            tuple(TABEL_LAMA)
        )
        lama = [baris[0] for baris in cursor.fetchall()]
        if lama:
            for nama in lama:
                cursor.execute(f'DROP TABLE IF EXISTS {nama}')
            cursor.execute('DROP TABLE IF EXISTS iv_bulan')
            logger.info('iv tabel format lama dihapus: %s, data perlu disinkron ulang', ', '.join(lama))
        for perintah in DDL:
            cursor.execute(perintah)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info('iv tabel siap di database %s', MYSQL_CONFIG['database'])
    except Exception as error:
        logger.error('iv gagal menyiapkan tabel: %s', error)
    if not BC_CLIENT_ID or not BC_CLIENT_SECRET:
        logger.warning('iv BC_CLIENT_ID / BC_CLIENT_SECRET belum diatur, sinkronisasi BC tidak bisa berjalan')
    if not API_TOKEN:
        logger.warning('iv API_TOKEN belum diatur, endpoint /iv tidak bisa diakses')


def butuh_token(view):
    @wraps(view)
    def pembungkus(*args, **kwargs):
        if not API_TOKEN:
            return jsonify({'success': False, 'message': 'Layanan belum dikonfigurasi'}), 503
        if not secrets.compare_digest(request.headers.get('X-API-Token', ''), API_TOKEN):
            return jsonify({'success': False, 'message': 'Akses ditolak'}), 401
        return view(*args, **kwargs)
    return pembungkus


class KlienBC:
    def __init__(self):
        self.sesi = requests.Session()
        self.token = None
        self.kedaluwarsa = 0
        self.basis = (
            f'https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENVIRONMENT}'
            f"/ODataV4/Company('{quote(BC_COMPANY)}')/"
        )

    def ambil_token(self, paksa=False):
        if not paksa and self.token and time.time() < self.kedaluwarsa - 300:
            return self.token
        if not BC_CLIENT_ID or not BC_CLIENT_SECRET:
            raise GalatBC('kredensial BC belum diatur')
        respons = self.panggil(
            'post',
            f'https://login.microsoftonline.com/{BC_TENANT}/oauth2/v2.0/token',
            pakai_token=False,
            data={
                'grant_type': 'client_credentials',
                'client_id': BC_CLIENT_ID,
                'client_secret': BC_CLIENT_SECRET,
                'scope': 'https://api.businesscentral.dynamics.com/.default'
            }
        )
        isi = respons.json()
        self.token = isi['access_token']
        try:
            klaim = json.loads(base64.urlsafe_b64decode(self.token.split('.')[1] + '=='))
            logger.info('iv token BC aud=%s tid=%s appid=%s roles=%s', klaim.get('aud'), klaim.get('tid'), klaim.get('appid'), klaim.get('roles'))
        except Exception:
            pass
        self.kedaluwarsa = time.time() + int(isi.get('expires_in', 3600))
        logger.info('iv token BC diperbarui')
        return self.token

    def panggil(self, metode, url, pakai_token=True, **kwargs):
        alasan = ''
        for percobaan in range(1, BC_MAX_RETRY + 1):
            headers = {'Accept': 'application/json'}
            if pakai_token:
                headers['Authorization'] = f'Bearer {self.ambil_token()}'
                headers['Prefer'] = f'odata.maxpagesize={BC_PAGE_SIZE}'
            jeda = BC_RETRY_MS / 1000
            try:
                respons = self.sesi.request(metode, url, headers=headers, timeout=BC_TIMEOUT, **kwargs)
            except requests.RequestException as error:
                alasan = f'koneksi: {error}'
                logger.warning('iv BC percobaan %s/%s gagal %s', percobaan, BC_MAX_RETRY, alasan)
                time.sleep(jeda)
                continue
            if respons.status_code == 401 and pakai_token:
                alasan = f'401 {respons.text[:500]}'
                logger.warning('iv BC percobaan %s/%s 401 %s: %s', percobaan, BC_MAX_RETRY, url[:200], respons.text[:500])
                if percobaan >= 2:
                    raise GalatBC(f'BC menolak akses (401): {respons.text[:500]}')
                self.token = None
                time.sleep(1)
                continue
            if respons.status_code == 429 or respons.status_code >= 500:
                alasan = f'{respons.status_code} {respons.text[:200]}'
                retry_after = respons.headers.get('Retry-After', '')
                if retry_after.isdigit():
                    jeda = max(jeda, int(retry_after))
                logger.warning('iv BC percobaan %s/%s %s, tunggu %ss', percobaan, BC_MAX_RETRY, respons.status_code, jeda)
                time.sleep(jeda)
                continue
            if respons.status_code >= 400:
                raise GalatBC(f'{respons.status_code} {respons.text[:300]}')
            return respons
        raise GalatBC(f'gagal setelah {BC_MAX_RETRY} percobaan: {alasan}')

    def halaman(self, endpoint, parameter):
        kueri = '&'.join(f"{kunci}={quote(nilai, safe=',')}" for kunci, nilai in parameter.items())
        url = f'{self.basis}{endpoint}?{kueri}'
        nomor = 0
        while url:
            nomor += 1
            mulai = time.time()
            isi = self.panggil('get', url).json(parse_float=Decimal)
            baris = isi.get('value', [])
            logger.info('iv %s halaman %s: %s baris (%.1fs)', endpoint, nomor, len(baris), time.time() - mulai)
            yield baris
            url = isi.get('@odata.nextLink')


def buat_job(mode, bulan_awal, bulan_akhir, total_bulan):
    job_id = str(uuid.uuid4())
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(
        'INSERT INTO iv_jobs (job_id, mode, status, bulan_awal, bulan_akhir, total_bulan, mulai) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (job_id, mode, 'queued', bulan_awal, bulan_akhir, total_bulan, sekarang_wib())
    )
    connection.commit()
    cursor.close()
    connection.close()
    return job_id


def update_job(job_id, **kolom):
    try:
        connection = connect()
        cursor = connection.cursor()
        bagian = ', '.join(f'{nama} = %s' for nama in kolom)
        cursor.execute(f'UPDATE iv_jobs SET {bagian} WHERE job_id = %s', (*kolom.values(), job_id))
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        logger.error('iv gagal update job %s: %s', job_id, error)


def sinkron_item(klien, connection):
    cursor = connection.cursor()
    sql = (
        'INSERT INTO iv_item (item_no, description, uom, updated_at) VALUES (%s, %s, %s, %s) '
        'ON DUPLICATE KEY UPDATE description = VALUES(description), uom = VALUES(uom), updated_at = VALUES(updated_at)'
    )
    waktu = sekarang_wib()
    total = 0
    for baris in klien.halaman(BC_ITEM_ENDPOINT, {'$select': 'No,Description,Base_Unit_of_Measure'}):
        nilai = [
            (r['No'][:30], (r.get('Description') or '').strip()[:250], (r.get('Base_Unit_of_Measure') or '')[:20], waktu)
            for r in baris if r.get('No')
        ]
        for awal in range(0, len(nilai), IV_INSERT_BATCH):
            cursor.executemany(sql, nilai[awal:awal + IV_INSERT_BATCH])
        connection.commit()
        total += len(nilai)
    cursor.close()
    logger.info('iv item tersinkron %s', total)


def agregasi_bulan(klien, bulan):
    parameter = {
        '$filter': f'Posting_Date ge {bulan.isoformat()} and Posting_Date le {akhir_bulan(bulan).isoformat()}',
        '$select': 'Entry_No,Location_Code,Item_No,Quantity,Cost_Amount_Actual,Cost_Amount_Expected'
    }
    hasil = {}
    entri = 0
    nol = Decimal(0)
    for baris in klien.halaman(BC_ILE_ENDPOINT, parameter):
        for r in baris:
            lokasi = (r.get('Location_Code') or '').strip()
            item = (r.get('Item_No') or '').strip()
            if not lokasi or not item:
                continue
            qty = Decimal(r.get('Quantity') or 0)
            nilai = Decimal(r.get('Cost_Amount_Actual') or 0) + Decimal(r.get('Cost_Amount_Expected') or 0)
            kunci = (lokasi[:20], item[:30])
            catatan = hasil.get(kunci)
            if catatan is None:
                catatan = [nol, nol, nol, nol]
                hasil[kunci] = catatan
            if qty > 0 or (qty == 0 and nilai >= 0):
                catatan[0] += qty
                catatan[1] += nilai
            else:
                catatan[2] += qty
                catatan[3] += nilai
            entri += 1
        del baris
    return hasil, entri


def simpan_bulan(connection, bulan, hasil, entri, job_id):
    cursor = connection.cursor()
    try:
        cursor.execute('DELETE FROM iv_mutasi_bulanan WHERE bulan = %s', (bulan,))
        sql = (
            'INSERT INTO iv_mutasi_bulanan (location_code, bulan, item_no, qty_masuk, nilai_masuk, qty_keluar, nilai_keluar) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s)'
        )
        baris = [(k[0], bulan, k[1], v[0], v[1], v[2], v[3]) for k, v in hasil.items() if not kosong(v)]
        for awal in range(0, len(baris), IV_INSERT_BATCH):
            cursor.executemany(sql, baris[awal:awal + IV_INSERT_BATCH])
        cursor.execute(
            'REPLACE INTO iv_bulan (bulan, entri, baris, job_id, synced_at) VALUES (%s, %s, %s, %s, %s)',
            (bulan, entri, len(baris), job_id, sekarang_wib())
        )
        cursor.execute(
            'INSERT INTO iv_tahun_status (tahun, kotor, updated_at) VALUES (%s, 1, %s) '
            'ON DUPLICATE KEY UPDATE kotor = 1, updated_at = VALUES(updated_at)',
            (bulan.year, sekarang_wib())
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return len(baris)


def bangun_saldo_tahunan(connection):
    tahun_ini = sekarang_wib().year
    cursor = connection.cursor()
    try:
        cursor.execute('SELECT MIN(tahun) FROM iv_tahun_status WHERE kotor = 1 AND tahun < %s', (tahun_ini,))
        awal = cursor.fetchone()[0]
        if awal is None:
            return 0
        cursor.execute('SELECT MAX(tahun) FROM iv_tahun_status WHERE tahun < %s', (tahun_ini,))
        akhir = cursor.fetchone()[0]
        total = 0
        for tahun in range(awal, akhir + 1):
            mulai = time.time()
            cursor.execute('DELETE FROM iv_saldo_tahunan WHERE tahun = %s', (tahun,))
            cursor.execute(
                'INSERT INTO iv_saldo_tahunan (tahun, location_code, item_no, qty, nilai) '
                'SELECT %s, t.location_code, t.item_no, SUM(t.qty), SUM(t.nilai) FROM ('
                'SELECT location_code, item_no, qty, nilai FROM iv_saldo_tahunan WHERE tahun = %s '
                'UNION ALL '
                'SELECT location_code, item_no, qty_masuk + qty_keluar, nilai_masuk + nilai_keluar '
                'FROM iv_mutasi_bulanan WHERE bulan >= %s AND bulan < %s'
                ') t GROUP BY t.location_code, t.item_no '
                'HAVING ABS(SUM(t.qty)) > 0.000001 OR ABS(SUM(t.nilai)) > 0.005',
                (tahun, tahun - 1, date(tahun, 1, 1), date(tahun + 1, 1, 1))
            )
            jumlah = cursor.rowcount
            cursor.execute(
                'INSERT INTO iv_tahun_status (tahun, kotor, updated_at) VALUES (%s, 0, %s) '
                'ON DUPLICATE KEY UPDATE kotor = 0, updated_at = VALUES(updated_at)',
                (tahun, sekarang_wib())
            )
            connection.commit()
            total += jumlah
            logger.info('iv saldo akhir tahun %s dibangun: %s baris (%.1fs)', tahun, jumlah, time.time() - mulai)
        return total
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def bulan_tersinkron(connection):
    cursor = connection.cursor()
    cursor.execute('SELECT bulan FROM iv_bulan')
    hasil = {baris[0] for baris in cursor.fetchall()}
    cursor.close()
    return hasil


def tentukan_bulan(connection, mode, sejak):
    bulan_ini = awal_bulan(sekarang_wib().date())
    mulai_data = parse_bulan(IV_START_MONTH)
    if mode == 'bangun_ulang':
        awal = max(sejak or mulai_data, mulai_data)
    else:
        awal = geser_bulan(bulan_ini, -IV_REFRESH_MONTHS)
        sudah = bulan_tersinkron(connection)
        for bulan in daftar_bulan(mulai_data, awal):
            if bulan not in sudah:
                awal = bulan
                break
    return daftar_bulan(awal, bulan_ini)


def jalankan_sync(job_id, bulan_list):
    kunci = connect()
    kursor_kunci = kunci.cursor()
    kursor_kunci.execute('SELECT GET_LOCK(%s, 0)', (IV_SYNC_LOCK,))
    dapat = kursor_kunci.fetchone()[0]
    if dapat != 1:
        kursor_kunci.close()
        kunci.close()
        update_job(job_id, status='skipped', pesan='Sinkronisasi lain sedang berjalan', selesai=sekarang_wib())
        logger.warning('iv job %s dilewati, ada sync lain', job_id)
        return
    connection = None
    total_entri = 0
    try:
        logger.info('iv job %s mulai %s bulan (%s s/d %s)', job_id, len(bulan_list), bulan_list[0], bulan_list[-1])
        update_job(job_id, status='running')
        klien = KlienBC()
        connection = connect()
        sinkron_item(klien, connection)
        for urutan, bulan in enumerate(bulan_list, start=1):
            mulai = time.time()
            update_job(job_id, bulan_berjalan=bulan)
            kursor_kunci.execute('SELECT 1')
            kursor_kunci.fetchall()
            hasil, entri = agregasi_bulan(klien, bulan)
            connection.ping(reconnect=True, attempts=3, delay=5)
            baris = simpan_bulan(connection, bulan, hasil, entri, job_id)
            del hasil
            total_entri += entri
            update_job(job_id, bulan_selesai=urutan, total_entri=total_entri)
            logger.info(
                'iv job %s bulan %s selesai: %s entri ILE -> %s baris bulanan (%.1fs)',
                job_id, bulan, entri, baris, time.time() - mulai
            )
        connection.ping(reconnect=True, attempts=3, delay=5)
        bangun_saldo_tahunan(connection)
        update_job(job_id, status='done', bulan_berjalan=None, selesai=sekarang_wib())
        logger.info('iv job %s selesai, total entri %s', job_id, total_entri)
    except Exception as error:
        logger.exception('iv job %s gagal', job_id)
        update_job(job_id, status='failed', pesan=str(error)[:2000], selesai=sekarang_wib())
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass
        try:
            kursor_kunci.execute('SELECT RELEASE_LOCK(%s)', (IV_SYNC_LOCK,))
            kursor_kunci.fetchall()
            kursor_kunci.close()
            kunci.close()
        except Exception:
            pass


def sync_berjalan(cursor):
    cursor.execute('SELECT IS_USED_LOCK(%s)', (IV_SYNC_LOCK,))
    return cursor.fetchone()[0] is not None


def validasi_lokasi(nilai):
    if not isinstance(nilai, list) or not nilai:
        raise GalatInput('Pilih minimal satu lokasi')
    hasil = sorted({str(v).strip()[:20] for v in nilai if str(v).strip()})
    if not hasil:
        raise GalatInput('Pilih minimal satu lokasi')
    if len(hasil) > IV_MAX_LOKASI:
        raise GalatInput(f'Maksimal {IV_MAX_LOKASI} lokasi sekali tampil')
    return hasil


def baca_periode(parameter):
    mode = parameter.get('mode')
    if mode == 'asof':
        bulan = parse_bulan(parameter.get('bulan'))
        return mode, None, bulan
    if mode == 'range':
        dari = parse_bulan(parameter.get('dari'))
        sampai = parse_bulan(parameter.get('sampai'))
        if dari > sampai:
            raise GalatInput('Bulan awal harus sebelum bulan akhir')
        return mode, dari, sampai
    raise GalatInput('Mode periode tidak dikenal')


def snapshot_dipakai(cursor, tahun):
    cursor.execute(
        'SELECT '
        '(SELECT COUNT(*) FROM iv_tahun_status WHERE tahun = %s AND kotor = 0), '
        '(SELECT COUNT(*) FROM iv_tahun_status WHERE tahun <= %s AND kotor = 1)',
        (tahun, tahun)
    )
    bersih, kotor = cursor.fetchone()
    return bersih == 1 and kotor == 0


def saldo_sebelum(cursor, lokasi, bulan, per_item):
    tanda = ', '.join(['%s'] * len(lokasi))
    kolom = 'location_code, item_no' if per_item else 'location_code'
    if snapshot_dipakai(cursor, bulan.year - 1):
        sql = (
            f'SELECT {kolom}, SUM(qty), SUM(nilai) FROM ('
            f'SELECT location_code, item_no, qty, nilai FROM iv_saldo_tahunan '
            f'WHERE tahun = %s AND location_code IN ({tanda}) '
            f'UNION ALL '
            f'SELECT location_code, item_no, qty_masuk + qty_keluar, nilai_masuk + nilai_keluar '
            f'FROM iv_mutasi_bulanan WHERE location_code IN ({tanda}) AND bulan >= %s AND bulan < %s'
            f') t GROUP BY {kolom}'
        )
        cursor.execute(sql, (bulan.year - 1, *lokasi, *lokasi, date(bulan.year, 1, 1), bulan))
    else:
        logger.info('iv saldo %s dihitung tanpa snapshot tahunan', bulan)
        cursor.execute(
            f'SELECT {kolom}, SUM(qty_masuk + qty_keluar), SUM(nilai_masuk + nilai_keluar) '
            f'FROM iv_mutasi_bulanan WHERE location_code IN ({tanda}) AND bulan < %s GROUP BY {kolom}',
            (*lokasi, bulan)
        )
    return cursor.fetchall()


def mutasi_periode(cursor, lokasi, dari, sampai, per_item):
    tanda = ', '.join(['%s'] * len(lokasi))
    kolom = 'location_code, item_no' if per_item else 'location_code'
    cursor.execute(
        f'SELECT {kolom}, SUM(qty_masuk), SUM(nilai_masuk), SUM(qty_keluar), SUM(nilai_keluar) '
        f'FROM iv_mutasi_bulanan WHERE location_code IN ({tanda}) AND bulan BETWEEN %s AND %s '
        f'GROUP BY {kolom}',
        (*lokasi, dari, sampai)
    )
    return cursor.fetchall()


def angka(nilai, digit):
    return float(round(nilai, digit))


def kosong(baris):
    for indeks, nilai in enumerate(baris):
        batas = NOL_QTY if indeks % 2 == 0 else NOL_NILAI
        if abs(nilai) > batas:
            return False
    return True


def hitung_laporan(cursor, lokasi, mode, dari, sampai, per_item):
    lebar = 2 if mode == 'asof' else 6
    data = {}

    def ambil(kunci):
        baris = data.get(kunci)
        if baris is None:
            baris = [Decimal(0)] * lebar
            data[kunci] = baris
        return baris

    if mode == 'asof':
        for baris in saldo_sebelum(cursor, lokasi, geser_bulan(sampai, 1), per_item):
            catatan = ambil(baris[:-2])
            catatan[0] += baris[-2]
            catatan[1] += baris[-1]
    else:
        for baris in saldo_sebelum(cursor, lokasi, dari, per_item):
            catatan = ambil(baris[:-2])
            catatan[0] += baris[-2]
            catatan[1] += baris[-1]
        for baris in mutasi_periode(cursor, lokasi, dari, sampai, per_item):
            catatan = ambil(baris[:-4])
            for posisi in range(4):
                catatan[2 + posisi] += baris[-4 + posisi]
    hasil = {}
    for kunci, baris in data.items():
        if kosong(baris):
            continue
        rapi = [angka(nilai, 5 if indeks % 2 == 0 else 2) for indeks, nilai in enumerate(baris)]
        if per_item:
            hasil.setdefault(kunci[0], []).append([kunci[1], *rapi])
        else:
            hasil[kunci[0]] = rapi
    if per_item:
        for daftar in hasil.values():
            daftar.sort(key=lambda r: r[0])
    return hasil


def info_sinkron(cursor):
    cursor.execute('SELECT MIN(bulan), MAX(bulan), MAX(synced_at) FROM iv_bulan')
    pertama, terakhir, waktu = cursor.fetchone()
    return {
        'bulanPertama': pertama.isoformat()[:7] if pertama else None,
        'bulanTerakhir': terakhir.isoformat()[:7] if terakhir else None,
        'terakhirSinkron': waktu.strftime('%Y-%m-%d %H:%M') if waktu else None
    }


def respons_laporan(per_item):
    mulai = time.time()
    parameter = request.get_json(silent=True) or {}
    try:
        lokasi = validasi_lokasi(parameter.get('lokasi'))
        mode, dari, sampai = baca_periode(parameter)
    except GalatInput as error:
        return jsonify({'success': False, 'message': str(error)}), 400
    connection = connect()
    cursor = connection.cursor()
    try:
        hasil = hitung_laporan(cursor, lokasi, mode, dari, sampai, per_item)
        isi = {
            'success': True,
            'mode': mode,
            'dari': dari.isoformat() if dari else None,
            'sampai': akhir_bulan(sampai).isoformat(),
            'bulanDari': dari.isoformat()[:7] if dari else None,
            'bulanSampai': sampai.isoformat()[:7],
            'lokasi': hasil,
            'sinkron': info_sinkron(cursor)
        }
        if per_item:
            kode = sorted({r[0] for daftar in hasil.values() for r in daftar})
            items = {}
            for awal in range(0, len(kode), 1000):
                potong = kode[awal:awal + 1000]
                cursor.execute(
                    f"SELECT item_no, description, uom FROM iv_item WHERE item_no IN ({', '.join(['%s'] * len(potong))})",
                    potong
                )
                for nomor, deskripsi, uom in cursor.fetchall():
                    items[nomor] = [deskripsi or '', uom or '']
            isi['items'] = items
        logger.info(
            'iv %s %s lokasi=%s mode=%s %s s/d %s (%.2fs)',
            'mutasi' if per_item else 'summary', len(lokasi), ','.join(lokasi[:5]), mode, dari, sampai, time.time() - mulai
        )
        return jsonify(isi)
    except Exception:
        logger.exception('iv laporan gagal')
        return jsonify({'success': False, 'message': 'Data belum bisa ditampilkan, coba lagi sebentar lagi'}), 500
    finally:
        cursor.close()
        connection.close()


@iv_bp.route('/mutasi', methods=['POST'])
@butuh_token
def iv_mutasi():
    return respons_laporan(True)


@iv_bp.route('/summary', methods=['POST'])
@butuh_token
def iv_summary():
    return respons_laporan(False)


@iv_bp.route('/sync', methods=['POST'])
@butuh_token
def iv_sync():
    parameter = request.get_json(silent=True) or {}
    mode = parameter.get('mode', 'rutin')
    if mode not in ('rutin', 'bangun_ulang'):
        return jsonify({'success': False, 'message': 'Mode sinkronisasi tidak dikenal'}), 400
    try:
        sejak = parse_bulan(parameter['sejak']) if parameter.get('sejak') else None
    except GalatInput as error:
        return jsonify({'success': False, 'message': str(error)}), 400
    connection = connect()
    cursor = connection.cursor()
    try:
        if sync_berjalan(cursor):
            return jsonify({'success': False, 'message': 'Pembaruan data masih berjalan'}), 409
        bulan_list = tentukan_bulan(connection, mode, sejak)
    finally:
        cursor.close()
        connection.close()
    job_id = buat_job(mode, bulan_list[0], bulan_list[-1], len(bulan_list))
    threading.Thread(target=jalankan_sync, args=(job_id, bulan_list), daemon=True).start()
    logger.info('iv sync %s dijadwalkan job %s (%s s/d %s)', mode, job_id, bulan_list[0], bulan_list[-1])
    return jsonify({
        'success': True,
        'jobId': job_id,
        'bulanAwal': bulan_list[0].isoformat()[:7],
        'bulanAkhir': bulan_list[-1].isoformat()[:7],
        'totalBulan': len(bulan_list)
    }), 202


@iv_bp.route('/sync/status', methods=['GET'])
@butuh_token
def iv_sync_status():
    connection = connect()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute('SELECT IS_USED_LOCK(%s) AS kunci', (IV_SYNC_LOCK,))
        berjalan = cursor.fetchone()['kunci'] is not None
        if not berjalan:
            cursor.execute(
                "UPDATE iv_jobs SET status = 'interrupted', selesai = %s "
                "WHERE status IN ('running', 'queued') AND mulai < %s",
                (sekarang_wib(), sekarang_wib() - timedelta(minutes=2))
            )
            connection.commit()
        cursor.execute(
            'SELECT job_id, mode, status, bulan_awal, bulan_akhir, bulan_berjalan, bulan_selesai, '
            'total_bulan, total_entri, mulai, selesai FROM iv_jobs ORDER BY mulai DESC LIMIT 1'
        )
        job = cursor.fetchone()
        if job:
            for nama in ('bulan_awal', 'bulan_akhir', 'bulan_berjalan'):
                job[nama] = job[nama].isoformat()[:7] if job[nama] else None
            for nama in ('mulai', 'selesai'):
                job[nama] = job[nama].strftime('%Y-%m-%d %H:%M') if job[nama] else None
        biasa = connection.cursor()
        info = info_sinkron(biasa)
        biasa.close()
        return jsonify({'success': True, 'berjalan': berjalan, 'job': job, 'sinkron': info})
    except Exception:
        logger.exception('iv status gagal')
        return jsonify({'success': False, 'message': 'Status belum bisa dibaca'}), 500
    finally:
        cursor.close()
        connection.close()
