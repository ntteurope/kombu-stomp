def register_transport():
    """Register STOMP transport with Kombu.

    You will have to manually call this function before being able to use STOMP
    protocol as Kombu transport.
    """
    # update TRANSPORT_ALIASES so it finds our own transport
    # ugly hack, but I couldn't find a better way
    # import here so we can import this without installing requirements
    from kombu import transport as _transport
    _transport.TRANSPORT_ALIASES['stomp'] = 'kombu_stomp.transport:Transport'
