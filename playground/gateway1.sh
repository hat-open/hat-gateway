#!/bin/sh

. $(dirname -- "$0")/env.sh

LOG_LEVEL=DEBUG
CONF_PATH=$DATA_PATH/gateway1.yaml

cat > $CONF_PATH << EOF
type: gateway
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
        hat:
            level: $LOG_LEVEL
    root:
        level: INFO
        handlers: ['console_handler']
    disable_existing_loggers: false
monitor:
    name: gateway1
    group: gateway
    monitor_address: "tcp+sbs://127.0.0.1:23010"
event_server_group: event
gateway_name: gateway1
devices: []
EOF

exec $PYTHON -m hat.gateway \
    --conf $CONF_PATH \
    "$@"
