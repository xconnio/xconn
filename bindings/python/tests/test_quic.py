import asyncio

import pytest
from xconn_core.quic import AsyncQUICConnection, QUICConnection


def test_quic_sessions_streams_and_certificate_validation(transport_peer):
    with pytest.raises(RuntimeError):
        QUICConnection.connect(
            transport_peer["quic"],
            server_name="wrong.invalid",
            certificate=bytes.fromhex(transport_peer["quic_certificate"]),
        )
    with QUICConnection.connect(
        transport_peer["quic"], server_name="localhost", certificate=bytes.fromhex(transport_peer["quic_certificate"])
    ) as connection:
        first = connection.open_session("realm")
        second = connection.open_session("realm")
        try:
            assert first.call("echo").arg("first").execute().args == ["first"]
            first.close()
            assert second.call("echo").arg("second").execute().args == ["second"]
            with connection.open_stream() as stream:
                stream.write_all(b"hello")
                stream.write_eof()
                assert stream.read() == b"hello"
                assert stream.read() == b""
        finally:
            first.close()
            second.close()


@pytest.mark.asyncio
async def test_async_quic_and_cancelled_read(transport_peer):
    async with await AsyncQUICConnection.connect(
        transport_peer["quic"], server_name="localhost", certificate=bytes.fromhex(transport_peer["quic_certificate"])
    ) as connection:
        session = await connection.open_session("realm")
        try:
            assert (await session.call("echo").arg(42).execute()).args == [42]
            async with await connection.open_stream() as stream:
                await stream.write_all(b"hi")
                assert await stream.read() == b"hi"
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(stream.read(), 0.02)
                await stream.write_eof()
                assert await stream.read() == b""
        finally:
            await session.close()
