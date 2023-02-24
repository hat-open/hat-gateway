#!/bin/sh

. $(dirname -- "$0")/env.sh

LOG_LEVEL=DEBUG
CONF_PATH=$DATA_PATH/event2.yaml

cat > $CONF_PATH << EOF
type: event
log:
    version: 1
    formatters:
        console_formatter:
            format: "[%(asctime)s %(levelname)s %(name)s] %(message)s"
    handlers:
        console_handler:
            class: logging.StreamHandler
            formatter: console_formatter
            level: DEBUG
    loggers:
        hat.event:
            level: $LOG_LEVEL
    root:
        level: INFO
        handlers: ['console_handler']
    disable_existing_loggers: false
monitor:
    name: event2
    group: event
    monitor_address: "tcp+sbs://127.0.0.1:24010"
backend:
    module: hat.event.server.backends.dummy
engine:
    server_id: 2
    modules: []
eventer_server:
    address: "tcp+sbs://localhost:24012"
syncer_server:
    address: "tcp+sbs://localhost:24013"
synced_restart_engine: false
EOF

exec $PYTHON -m hat.event.server \
    --conf $CONF_PATH \
    "$@"
