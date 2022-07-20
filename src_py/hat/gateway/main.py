"""Gateway main"""

from pathlib import Path
import argparse
import asyncio
import contextlib
import functools
import importlib
import logging.config
import sys
import typing

import appdirs

from hat import aio
from hat import json
from hat.gateway import common
from hat.gateway.engine import create_engine
import hat.event.eventer_client
import hat.event.common
import hat.monitor.client


user_conf_dir: Path = Path(appdirs.user_config_dir('hat'))
"""User configuration directory path"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf', metavar='PATH', type=Path, default=None,
        help="configuration defined by hat-gateway://main.yaml# "
             "(default $XDG_CONFIG_HOME/hat/gateway.{yaml|yml|json})")
    return parser


def main():
    """Gateway"""
    parser = create_argument_parser()
    args = parser.parse_args()

    conf_path = args.conf
    if not conf_path:
        for suffix in ('.yaml', '.yml', '.json'):
            conf_path = (user_conf_dir / 'gateway').with_suffix(suffix)
            if conf_path.exists():
                break

    if conf_path == Path('-'):
        conf = json.decode_stream(sys.stdin)
    else:
        conf = json.decode_file(conf_path)

    sync_main(conf)


def sync_main(conf: json.Data):
    """Sync main entry point"""
    loop = aio.init_asyncio()

    common.json_schema_repo.validate('hat-gateway://main.yaml#', conf)

    for device_conf in conf['devices']:
        module = importlib.import_module(device_conf['module'])
        if module.json_schema_repo and module.json_schema_id:
            module.json_schema_repo.validate(module.json_schema_id,
                                             device_conf)

    logging.config.dictConfig(conf['log'])

    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main(conf), loop=loop)


async def async_main(conf: json.Data):
    """Async main entry point"""
    async_group = aio.Group()

    try:
        subscriptions = [('gateway', conf['gateway_name'], '?', '?',
                          'system', '*')]

        if 'monitor' in conf:
            monitor = await hat.monitor.client.connect(conf['monitor'])
            _bind_resource(async_group, monitor)

            component = hat.monitor.client.Component(
                monitor, run_with_monitor, conf, monitor, subscriptions)
            component.set_ready(True)
            _bind_resource(async_group, component)

            await async_group.wait_closing()

        else:
            client = await hat.event.eventer_client.connect(
                conf['event_server_address'], subscriptions)
            _bind_resource(async_group, client)

            await async_group.spawn(run_with_event, conf, client)

    finally:
        await aio.uncancellable(async_group.async_close())


async def run_with_monitor(component: hat.monitor.client.Component,
                           conf: json.Data,
                           monitor: hat.monitor.client.Client,
                           subscriptions: typing.List[hat.event.common.EventType]):  # NOQA
    """Run monitor component"""
    run_cb = functools.partial(run_with_event, conf)
    await hat.event.eventer_client.run_eventer_client(
        monitor, conf['event_server_group'], run_cb, subscriptions)


async def run_with_event(conf: json.Data,
                         client: hat.event.eventer_client.EventerClient):
    """Run event client"""
    engine = await create_engine(conf, client)
    try:
        await engine.wait_closing()
    finally:
        await aio.uncancellable(engine.async_close())


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_cancel, resource.async_close)
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)


if __name__ == '__main__':
    sys.argv[0] = 'hat-gateway'
    sys.exit(main())
