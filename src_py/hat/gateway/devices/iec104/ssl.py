from pathlib import Path
import asyncio
import logging
import typing

import cryptography.hazmat.primitives.asymmetric.rsa
import cryptography.x509

from hat import aio
from hat import json
from hat.drivers import iec104
from hat.drivers import ssl


mlog = logging.getLogger(__name__)


SslProtocol: typing.TypeAlias = ssl.SslProtocol


def create_ssl_ctx(conf: json.Data,
                   protocol: ssl.SslProtocol
                   ) -> ssl.SSLContext:
    ctx = ssl.create_ssl_ctx(
        protocol=protocol,
        verify_cert=conf['verify_cert'],
        cert_path=(Path(conf['cert_path']) if conf['cert_path'] else None),
        key_path=(Path(conf['key_path']) if conf['key_path'] else None),
        ca_path=(Path(conf['ca_path']) if conf['ca_path'] else None))

    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers('AES128-SHA256:'
                    'DH-RSA-AES128-SHA256:'
                    'DH-RSA-AES128-GCM-SHA256:'
                    'DHE-RSA-AES128-GCM-SHA256:'
                    'DH-RSA-AES128-GCM-SHA256:'
                    'ECDHE-RSA-AES128-GCM-SHA256:'
                    'ECDHE-RSA-AES256-GCM-SHA384:'
                    'TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:'
                    'TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384')

    if conf.get('strict_mode'):
        ctx.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF

    return ctx


def init_security(conf: json.Data,
                  conn: iec104.Connection):
    if conf.get('strict_mode'):
        cert_bytes = conn.conn.ssl_object.getpeercert(True)
        _check_cert(cert_bytes)

    mlog.info('TLS session successfully established')

    renegotiate_delay = conf.get('renegotiate_delay')
    if renegotiate_delay:
        conn.async_group.spawn(_renegotiate_loop, conn.conn.ssl_object,
                               renegotiate_delay)

    if conf.get('strict_mode') and renegotiate_delay and conf['ca_path']:
        conn.async_group.spawn(_verify_loop, conn.conn.ssl_object,
                               renegotiate_delay * 2, Path(conf['ca_path']))


def _check_cert(cert_bytes):
    if len(cert_bytes) > 8192:
        mlog.warning('TLS certificate size exceeded')

    cert = cryptography.x509.load_der_x509_certificate(cert_bytes)
    key = cert.public_key()

    if isinstance(key, cryptography.hazmat.primitives.asymmetric.rsa.RSAPublicKey):  # NOQA
        if key.key_size < 2048:
            raise Exception('insufficient RSA key length')

        if key.key_size > 8192:
            mlog.warning('RSA key length greater than 8192')


async def _renegotiate_loop(ssl_object, renegotiate_delay):
    executor = aio.Executor()

    try:
        while True:
            await asyncio.sleep(renegotiate_delay)

            try:
                await executor.spawn(_ext_renegotiate, ssl_object)

            except Exception as e:
                mlog.error('renegotiate error: %s', e, exc_info=e)

    except Exception as e:
        mlog.error('renegotiate loop error: %s', e, exc_info=e)

    finally:
        mlog.debug('closing renegotiate loop')
        await aio.uncancellable(executor.async_close())


async def _verify_loop(ssl_object, verify_delay, ca_path):
    executor = aio.Executor()

    try:
        while True:
            await asyncio.sleep(verify_delay)

            try:
                await executor.spawn(_ext_verify, ssl_object, ca_path)

            except Exception as e:
                mlog.error('verify error: %s', e, exc_info=e)

    except Exception as e:
        mlog.error('verify loop error: %s', e, exc_info=e)

    finally:
        mlog.debug('closing verify loop')
        await aio.uncancellable(executor.async_close())


def _ext_renegotiate(ssl_object):
    if ssl_object.version() == 'TLSv1.3':
        ssl.key_update(ssl_object, ssl.KeyUpdateType.UPDATE_REQUESTED)

    else:
        ssl.renegotiate(ssl_object)

    ssl_object.do_handshake()


def _ext_verify(ssl_object, ca_path):
    cert_bytes = ssl_object.getpeercert(True)
    cert = cryptography.x509.load_der_x509_certificate(cert_bytes)

    crl = cryptography.x509.load_pem_x509_crl(ca_path.read_bytes())

    if crl.get_revoked_certificate_by_serial_number(cert.serial_number):
        mlog.warning('current certificate in CRL')
