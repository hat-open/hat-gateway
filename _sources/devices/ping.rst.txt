Ping device
===========

Ping device provides testing of remote device availability based on ICMP Echo
communication.

Ping device configuration is specified by
``hat-gateway://ping.yaml#/$defs/device``.

According to :ref:`gateway specification <gateway>`, all Ping device
event types have prefix::

    gateway/ping/<device_name>/<source>/...

Together with Ping specific events, generic `enable` and `running` events
are also supported.

Ping specific events don't contain `source_timestamp`.


Communication
-------------

On device creation, icmp `Endpoint` is created. For each configured
`remote_device` devices starts a loop. Initially, ``NOT_AVAILABLE`` status
event is registered for each device and immediately first ping (echo message)
is send to the remote device. In case remote device successfully responds,
status event with payload ``AVAILABLE`` is registered and `ping_delay` seconds
are waited before the next ping.

In case ping did not succeed due to `ping_timeout` exceeded or any other reason,
device retries ping for `retry_count` number of times with `retry_delay` of
seconds between each retry. After `retry_count` number of unsuccessful
retries, ``NOT_AVAILABLE`` status event is registered and `ping_delay` seconds
are waited before the next ping.



Gateway events
--------------

Events registered by gateway have event type starting with::

    gateway/ping/<device_name>/gateway/...

Available gateway events are:

    * .../status/<name>

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
