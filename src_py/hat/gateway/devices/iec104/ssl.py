from pathlib import Path
import logging
import ssl

import cryptography.hazmat.primitives.asymmetric.rsa
import cryptography.x509

from hat import json
from hat.drivers import tcp


mlog = logging.getLogger(__name__)


def create_ssl_ctx(conf: json.Data,
                   protocol: tcp.SslProtocol
                   ) -> ssl.SSLContext:
    ctx = tcp.create_ssl_ctx(
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


def check_cert(cert_bytes: bytes):
    if len(cert_bytes) > 8192:
        mlog.warning('TLS certificate size exceeded')

    cert = cryptography.x509.load_der_x509_certificate(cert_bytes)
    key = cert.public_key()

    if isinstance(key, cryptography.hazmat.primitives.asymmetric.rsa.RSAPublicKey):  # NOQA
        if key.key_size < 2048:
            raise Exception('insufficient RSA key length')

        if key.key_size > 8192:
            mlog.warning('RSA key length greater than 8192')
