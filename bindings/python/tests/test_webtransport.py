import pytest
from xconn_core.webtransport import AsyncWebTransportConnection, WebTransportConnection


def test_webtransport_verifies_certificates_and_transports_rpc_and_bytes(transport_peer):
    url = transport_peer["webtransport"]
    with pytest.raises(RuntimeError):
        WebTransportConnection.connect(url)
    with WebTransportConnection.connect(
        url, certificate=bytes.fromhex(transport_peer["webtransport_certificate"])
    ) as peer:
        session = peer.open_session("realm")
        try:
            assert session.call("echo").arg("HTTP/3").execute().args == ["HTTP/3"]
            with peer.open_stream() as stream:
                stream.write_all(b"webtransport")
                stream.write_eof()
                assert stream.read() == b"webtransport"
                assert stream.read() == b""
        finally:
            session.close()


@pytest.mark.asyncio
async def test_async_webtransport_session_and_stream(transport_peer):
    async with await AsyncWebTransportConnection.connect(
        transport_peer["webtransport"], certificate=bytes.fromhex(transport_peer["webtransport_certificate"])
    ) as peer:
        session = await peer.open_session("realm")
        try:
            assert (await session.call("echo").arg(42).execute()).args == [42]
            async with await peer.open_stream() as stream:
                await stream.write_all(b"async")
                await stream.write_eof()
                assert await stream.read() == b"async"
                assert await stream.read() == b""
        finally:
            await session.close()
