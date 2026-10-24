import "dart:convert";
import "dart:typed_data";

import "package:xconn_core/src/callbacks.dart" show fields;
import "package:xconn_core/src/client.dart";
import "package:xconn_core/src/native_transport.dart";
import "package:xconn_core/src/session.dart";

class WebRTCOptions {
  const WebRTCOptions({
    required this.offerProcedure,
    required this.remoteCandidatesTopic,
    required this.localCandidatesTopic,
    this.iceServers = const [],
    this.connectTimeout = const Duration(seconds: 20),
    this.openTimeout = const Duration(seconds: 20),
    this.handshakeTimeout = const Duration(seconds: 5),
  });

  final String offerProcedure;
  final String remoteCandidatesTopic;
  final String localCandidatesTopic;
  final List<Map<String, Object?>> iceServers;
  final Duration connectTimeout;
  final Duration openTimeout;
  final Duration handshakeTimeout;

  Map<String, Object?> toMap() => {
    "offer_procedure": offerProcedure,
    "remote_candidates_topic": remoteCandidatesTopic,
    "local_candidates_topic": localCandidatesTopic,
    "ice_servers": iceServers,
    "connect_timeout": seconds(connectTimeout),
    "open_timeout": seconds(openTimeout),
    "handshake_timeout": seconds(handshakeTimeout),
  };
}

/// Raw text/binary packets or ordered, reliable chunked messages.
class DataChannel extends NativeTransportResource {
  DataChannel._(Map<String, dynamic> response, this.connection)
    : label = response["label"] as String,
      protocol = response["protocol"] as String,
      super(response["handle"] as int);

  final WebRTCConnection connection;
  final String label;
  final String protocol;
  Future<void>? _closing;

  Future<void> send(Object data) async {
    switch (data) {
      case String():
        await request("webrtc_send_text", {"text": data});
      case Uint8List():
        await request("webrtc_write_packet", {"data": data});
      default:
        throw ArgumentError.value(data, "data", "must be a String or Uint8List");
    }
  }

  Future<Object> receive() async {
    final packet = fields(await request("webrtc_read_data_packet"));
    final data = packet["data"] as Uint8List;
    return packet["is_string"] == true ? utf8.decode(data) : data;
  }

  /// Current SCTP transport backlog, excluding caller-owned buffers.
  Future<int> get bufferedAmount async => (await request("webrtc_buffered_amount"))! as int;

  /// Completes when the remote or local endpoint closes this channel.
  Future<void> get closed async {
    await request("webrtc_channel_wait");
  }

  Future<void> waitBufferedBelow(int threshold, {Duration timeout = const Duration(seconds: 20)}) async {
    RangeError.checkNotNegative(threshold, "threshold");
    if (timeout.isNegative) {
      throw ArgumentError.value(timeout, "timeout", "must not be negative");
    }
    await request("webrtc_buffered_threshold", {"threshold": threshold});
    var previous = 0;
    while (await bufferedAmount > threshold) {
      final low = await request("webrtc_buffered_low", {"previous": previous, "operation_timeout": seconds(timeout)});
      previous = low! as int;
    }
  }

  Future<void> writeMessage(Uint8List data) async {
    await request("webrtc_write_message", {"data": data});
  }

  Future<Uint8List> readMessage() async => (await request("webrtc_read_message"))! as Uint8List;

  @override
  Future<void> close() => _closing ??= () async {
    try {
      await request("webrtc_channel_close");
    } finally {
      await super.close();
    }
  }();
}

/// Shared peer connection; closing a WAMP session leaves this connection open.
class WebRTCConnection extends NativeTransportResource {
  WebRTCConnection._(super.handle);

  Future<void>? _closing;

  static Future<WebRTCSession> connect(Session signaling, String realm, WebRTCOptions options, {Client? client}) =>
      connectNative<WebRTCSession>(client ?? Client(), "webrtc_connect", {
        "signaling": nativeSessionHandle(signaling),
        "realm": realm,
        ...options.toMap(),
      }, sessionFactory: (response, callbacks) => WebRTCSession._(response, callbacks: callbacks));

  Future<WebRTCSession> openSession(String realm, {Client? client}) => connectNative<WebRTCSession>(
    client ?? Client(),
    "webrtc_open_session",
    {"handle": handle, "realm": realm},
    sessionFactory: (response, callbacks) => WebRTCSession._(response, callbacks: callbacks, connection: this),
  );

  Future<DataChannel> openChannel(
    String label, {
    bool ordered = true,
    int? maxRetransmits,
    int? maxPacketLifeTime,
    Duration timeout = const Duration(seconds: 20),
  }) async => DataChannel._(
    fields(
      await request("webrtc_channel_open", {
        "label": label,
        "open_timeout": seconds(timeout),
        "operation_timeout": seconds(timeout),
        "options": {"ordered": ordered, "max_retransmits": ?maxRetransmits, "max_packet_life_time": ?maxPacketLifeTime},
      }),
    ),
    this,
  );

  Future<DataChannel> acceptChannel({Duration timeout = const Duration(seconds: 20)}) async => DataChannel._(
    fields(
      await request("webrtc_channel_accept", {"open_timeout": seconds(timeout), "operation_timeout": seconds(timeout)}),
    ),
    this,
  );

  @override
  Future<void> close() => _closing ??= () async {
    try {
      await request("webrtc_connection_close");
    } finally {
      await super.close();
    }
  }();
}

class WebRTCSession extends Session {
  WebRTCSession._(super.response, {super.callbacks, WebRTCConnection? connection})
    : connection = connection ?? WebRTCConnection._(response["connection"] as int),
      super.fromNative();

  final WebRTCConnection connection;
}
