import logging
import os
import secrets
import time
from datetime import date, timedelta
from functools import wraps

from curl_cffi import requests as cffi_requests
from flask import Blueprint, jsonify, request

logger = logging.getLogger('kurs-bi')

kurs_bp = Blueprint('kurs_bi', __name__, url_prefix='/kurs')

API_TOKEN = os.environ.get('KURS_API_TOKEN') or os.environ.get('API_TOKEN', '')
BI_URL = 'https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursLokal2'
MAX_HARI = 14
MAX_COBA = 3


def butuh_token(view):
    @wraps(view)
    def pembungkus(*args, **kwargs):
        if not API_TOKEN:
            return jsonify({'success': False, 'message': 'Layanan belum dikonfigurasi'}), 503
        if not secrets.compare_digest(request.headers.get('X-API-Token', ''), API_TOKEN):
            return jsonify({'success': False, 'message': 'Akses ditolak'}), 401
        return view(*args, **kwargs)
    return pembungkus


def ambil_xml(tgl):
    for coba in range(1, MAX_COBA + 1):
        try:
            res = cffi_requests.get(BI_URL, params={'tgl': tgl}, impersonate='chrome', timeout=30)
            if res.status_code == 200 and 'DataSet' in res.text:
                return res.text
            logger.warning('kurs BI %s percobaan %s status %s', tgl, coba, res.status_code)
        except Exception as error:
            logger.warning('kurs BI %s percobaan %s gagal: %s', tgl, coba, error)
        if coba < MAX_COBA:
            time.sleep(3)
    raise RuntimeError(f'Gagal ambil kurs BI {tgl}')


@kurs_bp.route('/bi', methods=['GET'])
@butuh_token
def kurs_bi():
    try:
        sampai = date.fromisoformat(request.args.get('to', ''))
        dari = date.fromisoformat(request.args.get('from', '')) if request.args.get('from') else sampai
    except ValueError:
        return jsonify({'success': False, 'message': 'Format tanggal harus YYYY-MM-DD'}), 400

    if dari > sampai or (sampai - dari).days > MAX_HARI:
        return jsonify({'success': False, 'message': f'Rentang tanggal maksimal {MAX_HARI} hari'}), 400

    hari = [dari + timedelta(days=i) for i in range((sampai - dari).days + 1)]
    tanggal = [d.isoformat() for d in hari if d.weekday() < 5]

    try:
        xml = [ambil_xml(t) for t in tanggal]
    except RuntimeError as error:
        logger.error('%s', error)
        return jsonify({'success': False, 'message': 'Sumber kurs BI tidak dapat dihubungi'}), 502

    logger.info('kurs BI terkirim %s tanggal (%s s/d %s)', len(xml), dari, sampai)
    return jsonify({'success': True, 'tanggal': tanggal, 'xml': xml})
