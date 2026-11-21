use socket2::SockRef;
use std::{
    future::Future,
    io,
    net::{SocketAddr, TcpListener, UdpSocket},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use webrtc::runtime::{
    AsyncInterval, AsyncTcpListener, AsyncTcpStream, AsyncUdpSocket, JoinHandle, Runtime, TokioRuntime,
};

const UDP_RECEIVE_BUFFER: usize = 1024 * 1024;

#[derive(Debug)]
pub(crate) struct WebRtcRuntime;

impl Runtime for WebRtcRuntime {
    fn wrap_udp_socket(&self, socket: UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        let options = SockRef::from(&socket);
        // Give the connection driver room to receive bursts while it handles other events.
        // Preserve a larger OS default and let the OS cap the request where necessary.
        if options.recv_buffer_size().is_ok_and(|size| size < UDP_RECEIVE_BUFFER) {
            let _ = options.set_recv_buffer_size(UDP_RECEIVE_BUFFER);
        }
        TokioRuntime.wrap_udp_socket(socket)
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) -> Box<dyn JoinHandle> {
        TokioRuntime.spawn(future)
    }

    fn spawn_reactor(
        &self,
        reactor_pool_size: usize,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Box<dyn JoinHandle> {
        TokioRuntime.spawn_reactor(reactor_pool_size, future)
    }

    fn wrap_tcp_listener(&self, listener: TcpListener) -> io::Result<Arc<dyn AsyncTcpListener>> {
        TokioRuntime.wrap_tcp_listener(listener)
    }

    fn connect_tcp(
        &self,
        remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<Arc<dyn AsyncTcpStream>>> + Send + '_>> {
        TokioRuntime.connect_tcp(remote_addr)
    }

    fn resolve_host<'a>(
        &'a self,
        host: &'a str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>> {
        TokioRuntime.resolve_host(host)
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        TokioRuntime.sleep(duration)
    }

    fn interval(&self, period: Duration) -> Box<dyn AsyncInterval> {
        TokioRuntime.interval(period)
    }

    fn block_on(&self, future: Pin<Box<dyn Future<Output = ()> + '_>>) {
        TokioRuntime.block_on(future)
    }

    fn yield_now(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        TokioRuntime.yield_now()
    }

    fn name(&self) -> &'static str {
        TokioRuntime.name()
    }
}
