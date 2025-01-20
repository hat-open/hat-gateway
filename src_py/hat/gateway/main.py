"""Gateway main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import logging.config
import sys

import appdirs

from hat import aio
from hat import json

from hat.gateway import common
from hat.gateway.runner import MainRunner


mlog: logging.Logger = logging.getLogger('hat.gateway.main')
"""Module logger"""

user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-gateway://main.yaml "
             "(default $XDG_CONFIG_HOME/hat/gateway.{yaml|yml|toml|json})")
    return parser


def main():
    """Gateway"""
    parser = create_argument_parser()
    args = parser.parse_args()
    conf = json.read_conf(args.conf, user_conf_dir / 'gateway')
    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    aio.init_asyncio()

    validator = json.DefaultSchemaValidator(common.json_schema_repo)
    validator.validate('hat-gateway://main.yaml', conf)

    for device_conf in conf['devices']:
        info = common.import_device_info(device_conf['module'])
        if info.json_schema_repo and info.json_schema_id:
            validator = json.DefaultSchemaValidator(info.json_schema_repo)
            validator.validate(info.json_schema_id, device_conf)

    log_conf = conf.get('log')
    if log_conf:
        logging.config.dictConfig(log_conf)

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf))


async def async_main(conf: json.Data):
    """Async main entry point"""
    main_runner = MainRunner(conf)

    async def cleanup():
        await main_runner.async_close()
        await asyncio.sleep(0.1)

    try:
        await main_runner.wait_closing()

    finally:
        await aio.uncancellable(cleanup())


if __name__ == '__main__':
    sys.argv[0] = 'hat-gateway'
    sys.exit(main())
