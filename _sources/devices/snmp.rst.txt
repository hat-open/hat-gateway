SNMP devices
============

Together with SNMP specific events, generic `enable` and `running` events
are also supported.

SNMP specific events don't contain `source_timestamp`.

OIDs and IP addresses are encoded as strings containing ``.`` delimited
decimal numbers.

``ARBITRARY`` data is encoded as strings where byte is represented with
two hexadecimal lowercase characters.


Manager device
--------------

SNMP manager device configuration is specified by
``hat-gateway://snmp.yaml#/definitions/manager``.

According to :ref:`gateway specification <gateway>`, all SNMP manager device
event types have prefix::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, <source>, ...

Once manager device is enabled, it will try to open UDP endpoint and start
communication with remote agent. During this time, connection status is
considered ``CONNECTING``. When first agent's response is successfully
received, connection status will transfer to ``CONNECTED`` state. During
``CONNECTED`` state, manager expects to receive response messages in
configured `request_timeout` period. If response is not received in this
period, manager will try to re-transmit request ``request_retry_count`` number
of times. If no response is received during this period, manager will
close UDP endpoint, transfer to ``DISCONNECTED`` state and wait configured
``connect_delay`` time before trying to reestablish connection.

Manager reads remote data as a result of received read requests or based on
configured polling list. OIDs configured in polling list are read periodically
and compared to cache of previously read data. If cache doesn't contain
previous data value, new read result is generated with cause set to
``INTERROGATE``. If new data value is changed, new read result is generated
with cause set to ``CHANGE``. Connection's transfer to ``DISCONNECTED`` state
clears data cache. All agent's responses, which are result of `system` read
requests, always generate new read result events with cause set to
``REQUESTED`` (event is registered even if received value is the same as
previously cached value).

If configured ``polling_oids`` list is empty, manager will try to periodically
read value of "0.0" OID. Result of this read is used only for connection
state detection and it's value is discarded.

Events, representing `system` requests for `read` and `write` actions, contain
arbitrary `session_id` value. This value is included in `gateway` events
representing `read` and `write` action results.


Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, 'gateway', ...

Available gateway events are:

    * ..., 'status'

        Connection status.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/definitions/events/manager/gateway/status``.

    * ..., 'read', <oid>

        Get data result.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/definitions/events/manager/gateway/read``.

    * ..., 'write', <oid>

        Set data result.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/definitions/events/manager/gateway/write``.


System events
'''''''''''''

Events registered by other Hat components, which are consumed by gateway, have
event type starting with::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, 'system', ...

Available system events are:

    * ..., 'read', <oid>

        Get data request.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/definitions/events/manager/system/read``.

    * ..., 'write', <oid>

        Write data request.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/definitions/events/manager/system/write``.


Trap listener device
--------------------

SNMP trap listener device configuration is specified by
``hat-gateway://snmp.yaml#/definitions/trap_listener``.

According to :ref:`gateway specification <gateway>`, all SNMP trap listener
device event types have prefix::

    'gateway', <gateway_name>, 'snmp_trap_listener', <device_name>, <source>, ...


Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    'gateway', <gateway_name>, 'snmp_trap_listener', <device_name>, 'gateway', ...

Available gateway events are:

    * ..., 'data', <oid>


Agent device
------------

...


Trap sender device
------------------

...


Configurations and event payloads
---------------------------------

.. literalinclude:: ../../schemas_json/snmp.yaml
    :language: yaml
