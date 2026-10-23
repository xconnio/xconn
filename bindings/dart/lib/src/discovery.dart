import "package:xconn_core/src/callbacks.dart" show fields;
import "package:xconn_core/src/native.dart" as native;

/// Connection hints and capabilities, without secret credentials.
class Endpoint {
  const Endpoint({required this.id, this.addresses = const [], this.capabilities = const [], this.metadata = const {}});

  factory Endpoint.fromMap(Map<String, dynamic> value) => Endpoint(
    id: value["id"] as String,
    addresses: (value["addresses"] as List).cast<String>(),
    capabilities: (value["capabilities"] as List).cast<String>(),
    metadata: (value["metadata"] as Map).cast<String, Object?>(),
  );

  final String id;
  final List<String> addresses;
  final List<String> capabilities;
  final Map<String, dynamic> metadata;

  Map<String, Object?> toMap() => {
    "id": id,
    "addresses": addresses,
    "capabilities": capabilities,
    "metadata": metadata,
  };
}

/// Native client for an application-selected discovery URI prefix.
class Discovery {
  Discovery(this._handle, this._prefix);

  final int _handle;
  final String _prefix;

  Future<Object?> _request(String action, [List<Object?> args = const []]) => native.executeAsync(
    "service_discovery_request",
    {"session": _handle, "prefix": _prefix, "action": action, "args": args},
  );

  Future<void> advertise(Endpoint endpoint) async {
    await _request("advertise", [endpoint.toMap()]);
  }

  Future<void> withdraw(String id) async {
    await _request("withdraw", [id]);
  }

  Future<Endpoint> get(String id) async => Endpoint.fromMap(fields(await _request("get", [id])));

  Future<List<Endpoint>> list() async =>
      ((await _request("list"))! as List).map((value) => Endpoint.fromMap(fields(value))).toList();
}
