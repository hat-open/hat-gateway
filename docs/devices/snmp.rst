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
``hat-gateway://snmp.yaml#/$defs/manager``.

According to :ref:`gateway specification <gateway>`, all SNMP manager device
event types have prefix::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, <source>, ...

Once manager device is enabled, it will try to open UDP endpoint and start
communication with remote agent - periodical polling of OIDs configured as
`polling_oids` each `polling_delay` number of seconds. From the moment device
is enabled, until first oid is read succesfully, connection status is
considered ``CONNECTING``. When first agent's response is successfully
received, connection status will transfer to ``CONNECTED`` state. During
``CONNECTED`` state, manager on each read request expects to receive response
in configured `request_timeout` period. If response is not received in this
period, manager will try to re-transmit request ``request_retry_count`` number
of times. If no response is received during this period, manager will close UDP
endpoint, transfer to ``DISCONNECTED`` state and wait configured
`connect_delay` time before trying to reestablish connection.
If connection was ``CONNECTED`` and then transferred to ``DISCONNECTED``
`connect_delay` is not waited, but ``CONNECTING`` state starts immediately.

OIDs configured in polling list are read periodically and compared to cache of
previously read data. If cache doesn't contain previous data value, new gateway
read event is generated with cause set to ``INTERROGATE``. If new data value is
changed, new gateway read event is generated with cause set to ``CHANGE``.
Connection's transfer to ``DISCONNECTED`` state clears data cache. All agent's
responses, which are result of `system` events, always generate new read result
events with cause set to ``REQUESTED`` (event is registered even if received
value is the same as previously cached value).

If configured ``polling_oids`` list is empty, manager will try to periodically
read value of "0.0" OID. Result of this read is used only for connection
state detection and its value is discarded.

In case received oid is ``1.3.6.1.6.3.15.1.1.1`` and that oid was not
requested, endpoint is closed, and state is changed to `DISCONNECTED`.

Gateway read event will have `data/type` equal to ``ERROR`` in the following
scenarios:

* response is `Error`. `data/value` is set to corresponding error type.
* response is empty. `data/value` is set to `GEN_ERR`.
* response contains `Data` whose oid does not match requested oid. In case
  response oid is one of the following (Statistics for the User-based Security
  Model, RFC 3414), `data/value` is set to the corresponding value, otherwise
  is `GEN_ERR`:

  * 1.3.6.1.6.3.15.1.1.2 - 'NOT_IN_TIME_WINDOWS'
  * 1.3.6.1.6.3.15.1.1.3 - 'UNKNOWN_USER_NAMES'
  * 1.3.6.1.6.3.15.1.1.4 - 'UNKNOWN_ENGINE_IDS'
  * 1.3.6.1.6.3.15.1.1.5 - 'WRONG_DIGESTS'
  * 1.3.6.1.6.3.15.1.1.6 - 'DECRYPTION_ERRORS'

* response `Data` is one of one of the following types: `EmptyData`,
  `UnspecifiedData`, `NoSuchObjectData`, `NoSuchInstanceData`,
  `EndOfMibViewData`. In these cases `data/value` is set to the following,
  respectively: ``EMPTY``, ``UNSPECIFIED``, ``NO_SUCH_OBJECT``,
  ``NO_SUCH_INSTANCE``, ``END_OF_MIB_VIEW``.

Gateway write event will have `success` ``False`` in the following
scenarios:

* response is `Error` that is not of type ``NO_ERROR``,
* response does not contain `Data` with oid from request
* response `Data` is one of the following types: `EmptyData`, `UnspecifiedData`,
  `NoSuchObjectData`, `NoSuchInstanceData`, `EndOfMibViewData`.

In all other cases, `success` is ``True``.

Events, representing `system` requests for `read` and `write` actions, contain
arbitrary `session_id` value. This value is included in `gateway` events
representing `read` and `write` action results. If gateway `read` events are
result of polling request, `session_id` is ``null``. 

Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, 'gateway', ...

Available gateway events are:

    * ..., 'status'

        Connection status.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/$defs/events/manager/gateway/status``.
        registered by the module on each status change.

    * ..., 'read', <oid>

        Get data result.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/$defs/events/manager/gateway/read``.

    * ..., 'write', <oid>

        Set data result.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/$defs/events/manager/gateway/write``.


System events
'''''''''''''

Events registered by other Hat components, which are consumed by gateway, have
event type starting with::

    'gateway', <gateway_name>, 'snmp_manager', <device_name>, 'system', ...

Available system events are:

    * ..., 'read', <oid>

        Get data request.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/$defs/events/manager/system/read``.

    * ..., 'write', <oid>

        Write data request.

        Payload is defined by
        ``hat-gateway://snmp.yaml#/$defs/events/manager/system/write``.


Trap listener device
--------------------

...

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
