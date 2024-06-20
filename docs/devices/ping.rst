Ping device
===========

Ping device provides testing of remote device availability based on ICMP Echo
communication.

Ping device configuration is specified by
``hat-gateway://ping.yaml#/$defs/device``.

According to :ref:`gateway specification <gateway>`, all Ping device
event types have prefix::

    'gateway', <gateway_name>, 'ping', <device_name>, <source>, ...

Together with Ping specific events, generic `enable` and `running` events
are also supported.

Ping specific events don't contain `source_timestamp`.


Communication
-------------

.. todo::

    ...


Gateway events
--------------

Events registered by gateway have event type starting with::

    'gateway', <gateway_name>, 'ping', <device_name>, 'gateway', ...

Available gateway events are:

    * ..., 'status', <name>

        Remote host availability status.

        Payload is defined by
        ``hat-gateway://ping.yaml#/$defs/events/status``.


System events
-------------

Ping device doesn't support additional system events.


Configurations and event payloads
---------------------------------

.. literalinclude:: ../../schemas_json/ping.yaml
    :language: yaml
