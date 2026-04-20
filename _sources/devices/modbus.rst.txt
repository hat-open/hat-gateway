Modbus devices
==============

According to :ref:`gateway specification <gateway>`, all modbus device event
types have prefix::

    gateway/<device_type>/<device_name>/<source>/...

where `<device_type>` is ``modbus_master`` in case of modbus master device or
``modbus_slave`` in case of modbus slave device.

Together with modbus specific events, generic `enable` and `running` events
are also supported.

Modbus specific events don't contain `source_timestamp`.


Data value
----------

All modbus native data types are encoded as 1bit or 16bit addressable values.
Because of different data encoding schemas, single user defined data values
can occupy only parts of modbus registers or span across multiple registers.
To accommodate these different encoding rules, modbus gateway devices represent
single data point values as list of bits, encoded as single unsigned integer,
with length specified by configuration parameters.

Among other parameters, each data specifies:

    * data type (1bit or 16bit register size)
    * bit count
    * starting register address
    * bit offset

List of bit values is created by iterative reading of register values starting
with `starting register address` and incrementing register address by 1. Bits
in each register are read starting with most significant and advancing to the
least significant bit. Starting register bit defines bit offset - number of
bits which are skipped during start of this iterative procedure. These skipped
bits are not included in resulting value and are not taken into account in
bit count calculation.


Encoding/decoding example
'''''''''''''''''''''''''

In case of 16bit registers with content:

    +---------+--------+
    | Address | Value  |
    +=========+========+
    | 1000    | 0x1234 |
    +---------+--------+
    | 1001    | 0x5678 |
    +---------+--------+
    | 1002    | 0x9abc |
    +---------+--------+

When data item is configured with properties:

    * bit count: 32
    * starting register address: 1000
    * bit offset: 8

Data value is unlimited unsigned integer 0x3456789a.


Modbus master
-------------

Modbus master device configuration is specified by
``hat-gateway://modbus.yaml#/$defs/master``.

Once enabled, modbus master device will try to open communication connection
defined by configuration parameters. While connection is established,
this device repeatedly reads modbus registers (in accordance to configured
polling `interval` defined for each data in ``READ`` direction) and reports
any changes as read responses. Also, received write requests are propagated to
remote devices which result in write responses. If connection is lost or could
not be established, device repeatedly tries to establish new connection after
`connect_delay` seconds defined in configuration.

Together with read and write processing, device continuously reports current
status of communication connection and each modbus device identified by
`device_id` data parameter. Status of modbus device identified by `device_id`
is calculated according to availability of data associated with that
`device_id`. Available remote device statuses:

    * ``DISABLED``

        Data polling is not enabled

    * ``CONNECTING``

        Data polling is enabled and none of the read requests have completed.

    * ``CONNECTED``

        Data polling is enabled and at least one data read responses have been
        received.

    * ``DISCONNECTED``

        Transitive status signaling transition from
        ``CONNECTING``/``CONNECTED`` to ``CONNECTING``.

Polling of data values can be enabled/disabled based on `device_id`. Initially,
polling for all remote devices is disabled and has to be explicitly enabled.


Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    gateway/modbus_master/<device_name>/gateway/...

Available gateway events are:

    * .../status

        Connection status of a modbus master device.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/gateway/status``.

    * .../remote_device/<device_id>/status

        Status of a remote modbus device, where `<device_id>` is
        remote modbus device identifier `device_id` defined in
        configuration for each data.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/gateway/remote_device_status``.

    * .../remote_device/<device_id>/read/<data_name>

        Read response reporting current data value. Data value and cause
        of event reporting are available only in case of successful read.
        `<data_name>` is configuration defined `name` for each data.

        First successful read of data value will be reported with
        ``INTERROGATE`` `cause` and `result` ``SUCCESS``, while all the
        following successful reads, where value is different from previous
        are reported with ``CHANGE`` cause.
        If read is not successful, `result` equals to any other string
        that indicate reason of failure (`value` and `cause` properties
        are not included). After value is invalidated, next successful
        read will be reported with `INTERROGATED` cause.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/gateway/read``.

    * .../remote_device/<device_id>/write/<data_name>

        Write response reporting resulting success value. Together with
        `result` that contains success value, this event contains the same
        `request_id` as provided in associated write request event.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/gateway/write``.

        .. note:: in case response did not arrive within configuration
          defined `request_timeout`, response with `result` ``TIMEOUT``
          is registered.


System events
'''''''''''''

Events registered by other Hat components, which are consumed by gateway, have
event type starting with::

    gateway/modbus_master/<device_name>/system/...

Available system events are:

    * .../remote_device/<device_id>/enable

        Enable polling of data associated with remote modbus device with
        `<device_id>` identifier.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/system/enable``.

    * .../remote_device/<device_id>/write/<data_name>

        Write request containing data value and session identifier used
        for pairing of request/response messages.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/master/system/write``.


Modbus slave
------------

Modbus slave device configuration is specified by
``hat-gateway://modbus.yaml#/$defs/slave``.

According to :ref:`gateway specification <gateway>`, all modbus slave device
event types have prefix::

    gateway/modbus_slave/<device_name>/<source>/...

Modbus slave device provides implementation of modbus slave which accepts
multiple TCP master connections or a single serial master connection.
A change in the list of active master connections triggers the registration
of a new gateway 'connections' event containing the current state of the
connections list. A serial master connection is considered active only after
the first modbus request is made. Also, serial connections list will contain
one connection at the most.

Modbus slave device is configured with list of available data. Once started,
device queries last system 'data' events corresponding to the configured data.
This local cache is continuously updated with the latest data values based on
received system 'data' events. Each configured data has a device id specified.
For each modbus request (read, write or write mask) using a device id that
is not configured, a TCP modbus slave slave shall return an
``INVALID_DATA_ADDRESS`` error, whereas a serial modbus slave shall return
``None``. Broadcast device id 0 is not supported.

A keep-alive mechanism is supported. It can be optionally configured for TCP
slave devices, and is mandatory for serial slave devices. If configured, a
master connecting starts a keep-alive timer. Each master modbus request for a
configured device id resets the timer. If the timer expires, the connection
is considered inactive, and the registration of a gateway 'connections' event
is triggered.

When slave receives a modbus master read request, it will immediately respond
with read response based on the current state of local data cache. If the
modbus read request tries to read values that do not belong to any configured
data, an ``INVALID_DATA_ADDRESS`` error is returned.

Modbus slave device also supports modbus write and modbus write mask requests.
If slave receives a write request that at least partially hits configured data
(at least one bit), it registers a gateway 'write' event containing the new
data value. If multiple data items are affected, only one gateway 'write' event
is registered, containing all updated values.
If the request attempts to write on data that is not configured,
an ``INVALID_DATA_ADDRESS`` error is returned.

Modbus write mask request behaves similarly. If slave receives a write mask
request that at least partially hits configured data (at least one bit),
it registers a gateway 'write' event containing the new data value. If multiple
data items are affected, only one gateway 'write' event is registered,
containing all updated values.
If the request tries to write on data that is not configured,
an ``INVALID_DATA_ADDRESS`` error is returned.
Also, if not one bit of the hit data is affected by write mask request mask, a
``Success`` is returned without registering a gateway 'write' event.
A bit is considered affected if the applied and_mask used on that bit is not
equal to 1.

Both write and write mask requests take the current local cache data values
into consideration, and the new data value is calculated based on the previous
one.

In both write and write mask request, when gateway 'write' event is generated,
return value is based on paired system 'write' event's `success` value. If
`success` is ``True`` `Success` is returned, ``FUNCTION_ERROR`` otherwise.
If no paired system 'write' event is received within the configured
`response_timeout` seconds, ``FUNCTION_ERROR`` is returned.


Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    gateway/modbus_slave/<device_name>/gateway/...

Available gateway events are:

    * .../connections

        Represents change in list of all active connections. When device
        is disabled, empty list of connections is assumed (when device is
        disabled, new `connections` event should be registered but user
        should not depend on this behavior).

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/slave/gateway/connections``.

    * .../write

        Represents single Modbus write request.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/slave/gateway/write``.


System events
'''''''''''''

Events registered by other Hat components, which are consumed by gateway, have
event type starting with::

    gateway/modbus_slave/<device_name>/system/...

Available system events are:

    * .../data/<data_name>

        Represents data change.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/slave/system/data``.

    * .../write

        Represents single Modbus write response.

        Payload is defined by
        ``hat-gateway://modbus.yaml#/$defs/events/slave/system/write``.



Configurations and event payloads
---------------------------------

.. literalinclude:: ../../schemas_json/modbus.yaml
    :language: yaml
