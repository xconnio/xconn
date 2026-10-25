/// Native Dart client APIs backed by Rust XConn.
library;

export "package:xconn_core/auth.dart"
    show AnonymousAuthenticator, CryptoSignAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator;
export "package:xconn_core/serializers.dart" show CBORSerializer, JSONSerializer, MsgPackSerializer, Serializer;
export "package:xconn_core/src/call.dart" show Call;
export "package:xconn_core/src/client.dart" show Client, connectAnonymous, connectCRA, connectCryptosign, connectTicket;
export "package:xconn_core/src/discovery.dart" show Discovery, Endpoint;
export "package:xconn_core/src/exception.dart" show ApplicationError, ProtocolError;
export "package:xconn_core/src/payload.dart" show PayloadError;
export "package:xconn_core/src/requests.dart" show CallRequest, PublishRequest, RegisterRequest, SubscribeRequest;
export "package:xconn_core/src/session.dart" show Session;
export "package:xconn_core/src/types.dart"
    show
        BaseSession,
        ClientConfig,
        Event,
        Invocation,
        Peer,
        PeerBaseSession,
        Progress,
        Registration,
        Result,
        SessionDetails,
        Subscription;
