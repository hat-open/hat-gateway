SMPP devices
============

Client device
-------------

Client device configuration is specified by
``hat-gateway://smpp.yaml#/$defs/client``.

.. todo::

    ...


Gateway events
''''''''''''''

Events registered by gateway have event type starting with::

    gateway/smpp_client/<device_name>/gateway/...

Available gateway events are:

    * .../status

        Represents change in connection status. Upon enabling device,
        new ``CONNECTING`` status should be registered. Once device is
        disabled, ``DISCONNECTED`` status should be assumed regardless
        of last registered `status` event (registration of
        ``DISCONNECTED`` status event during device disabling is mandatory
        but should not be relied upon).

        Source timestamp is ``None``.

        Payload is specified by
        ``hat-gateway://smpp.yaml#/$defs/events/client/gateway/status``.


System events
'''''''''''''

Events registered by other Hat components, which are consumed by gateway, have
event type starting with::

    gateway/smpp_client/<device_name>/system/...

Available system events are:

    * .../message

        Represents request for sending new message.

        Source timestamp is ``None``.

        Payload is specified by
        ``hat-gateway://smpp.yaml#/$defs/events/client/system/message``.


Configurations and event payloads
---------------------------------

.. literalinclude:: ../../schemas_json/smpp.yaml
    :language: yaml
