"""Target application service. Parameter names stay in the device wire format."""


def invoke(client, method, params, timeout):
    # add/set/delete are never retried by this layer: a timeout is ambiguous.
    return client.call(method, params, timeout=timeout)
