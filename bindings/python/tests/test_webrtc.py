import asyncio

import pytest
from xconn_core import AsyncClient, Client
from xconn_core.webrtc import WebRTCOptions, connect, connect_async

OPTIONS = WebRTCOptions("offer", "to-client", "to-provider")


def test_webrtc_sessions_and_large_raw_messages(transport_peer):
    signaling = Client().connect(transport_peer["ws"], "realm")
    try:
        peer = connect(signaling, "realm", OPTIONS)
        with peer.connection as connection:
            with pytest.raises(RuntimeError, match="timed out"):
                connection.accept_channel(timeout=0.01)
            second = connection.open_session("realm")
            try:
                assert peer.call("echo").arg("rtc").execute().args == ["rtc"]
                peer.close()
                assert second.call("echo").arg(42).execute().args == [42]
                with connection.open_channel("files") as channel:
                    data = b"payload" * 20000
                    channel.write_message(data)
                    assert channel.read_message() == data
                    channel.write_message(b"")
                    assert channel.read_message() == b""
            finally:
                peer.close()
                second.close()
        assert signaling.call("echo").arg("still connected").execute().args == ["still connected"]
    finally:
        signaling.close()


@pytest.mark.asyncio
async def test_async_webrtc_and_cancelled_channel_read(transport_peer):
    signaling = await AsyncClient().connect(transport_peer["ws"], "realm")
    try:
        peer = await connect_async(signaling, "realm", OPTIONS)
        async with peer.connection as connection:
            with pytest.raises(RuntimeError, match="timed out"):
                await connection.accept_channel(timeout=0.01)
            try:
                assert (await peer.call("echo").arg(42).execute()).args == [42]
                async with await connection.open_channel("files") as channel:
                    with pytest.raises(asyncio.TimeoutError):
                        await asyncio.wait_for(channel.read_message(), 0.02)
                    await channel.write_message(b"after cancellation")
                    assert await channel.read_message() == b"after cancellation"
            finally:
                await peer.close()
    finally:
        await signaling.close()
