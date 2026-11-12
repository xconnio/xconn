/// Native Dart client APIs backed by Rust XConn.
library;

export "package:xconn/auth.dart"
    show AnonymousAuthenticator, CryptoSignAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator;
export "package:xconn/serializers.dart" show CBORSerializer, JSONSerializer, MsgPackSerializer, Serializer;
export "package:xconn/src/call.dart" show Call;
export "package:xconn/src/client.dart" show Client, connectAnonymous, connectCRA, connectCryptosign, connectTicket;
export "package:xconn/src/discovery.dart" show Discovery, Endpoint;
export "package:xconn/src/exception.dart" show ApplicationError, ProtocolError;
export "package:xconn/src/payload.dart" show PayloadError;
export "package:xconn/src/requests.dart" show CallRequest, PublishRequest, RegisterRequest, SubscribeRequest;
export "package:xconn/src/rpc_streams.dart" show RPCStream;
export "package:xconn/src/session.dart" show Session;
export "package:xconn/src/types.dart"
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
