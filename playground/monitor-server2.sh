#!/bin/sh

. $(dirname -- "$0")/env.sh

LOG_LEVEL=DEBUG
CONF_PATH=$DATA_PATH/monitor2.yaml

cat > $CONF_PATH << EOF
type: monitor
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
        hat.monitor:
            level: $LOG_LEVEL
    root:
        level: INFO
        handlers: ['console_handler']
    disable_existing_loggers: false
server:
    address: "tcp+sbs://127.0.0.1:24010"
    default_rank: 1
master:
    address: "tcp+sbs://127.0.0.1:24011"
    default_algorithm: BLESS_ONE
    group_algorithms: {}
slave:
    parents: ["tcp+sbs://127.0.0.1:23011"]
    connect_timeout: 5
    connect_retry_count: 3
    connect_retry_delay: 5
ui:
    address: "http://127.0.0.1:24022"
EOF

exec $PYTHON -m hat.monitor.server \
    --conf $CONF_PATH \
    "$@"
