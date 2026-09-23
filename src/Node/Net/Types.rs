// Native networking support. TCP sockets and servers are built on std::net and
// surfaced through the node-streams model: a socket *is* a duplex stream, and
// its net-specific state lives in the stream extension. Background threads only
// push jobs into the runtime's microtask queue, so every PureScript callback
// runs on the main thread.
use std::io::Read;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use Purs_Node_EventEmitter::{purust_emitter_emit, EventEmitter};

/// A socket is an event emitter; the duplex view shares the same value.
pub type Socket = EventEmitter;
/// A server is an event emitter.
pub type Server = EventEmitter;

pub type ErrorFactory = fn(String) -> crate::UnknownType;

// ---------------------------------------------------------------------------
// Microtask delivery and liveness
// ---------------------------------------------------------------------------

pub fn current_queue() -> Option<Rc<purust_core::microtasks::Queue>> {
    std::panic::catch_unwind(purust_core::microtasks::current).ok()
}

pub fn queue_value() -> Option<crate::UnknownType> {
    let queue = current_queue();    queue.map(|queue| crate::Value::Class(Rc::new(queue)))
}

/// Runs `job` on the thread that owns the captured microtask queue.
pub fn deliver(queue: &Option<crate::UnknownType>, job: impl FnOnce() + Send + Sync + 'static) {
    match queue {
        Some(queue) => {
            let queue = queue
                .unwrap_class::<Rc<purust_core::microtasks::Queue>>()
                .clone();            queue.enqueue(job);
        }
        None => {
            job()
        }
    }
}

static ACTIVE_HANDLES: AtomicI64 = AtomicI64::new(0);

fn pump_state() -> &'static Arc<(Mutex<u64>, Condvar)> {
    static PUMP: OnceLock<Arc<(Mutex<u64>, Condvar)>> = OnceLock::new();
    PUMP.get_or_init(|| Arc::new((Mutex::new(0), Condvar::new())))
}

/// Wakes the keep-alive pump as soon as a background thread has news.
pub fn wake() {
    let (lock, condvar) = &**pump_state();
    *lock.lock().unwrap() += 1;
    condvar.notify_all();
}

pub fn handle_open() {
    ACTIVE_HANDLES.fetch_add(1, Ordering::SeqCst);
}

pub fn handle_close() {
    ACTIVE_HANDLES.fetch_sub(1, Ordering::SeqCst);
    wake();
}

static PUMP_STARTED: OnceLock<()> = OnceLock::new();

/// Keeps the program alive while sockets or servers are open, like Node's
/// active handles. The job re-queues itself so events queued meanwhile are
/// processed between waits.
pub fn ensure_pump(queue: &Option<crate::UnknownType>) {
    if PUMP_STARTED.set(()).is_err() {
        return;
    }
    enqueue_pump(queue.clone());
}

fn enqueue_pump(queue: Option<crate::UnknownType>) {
    let has_queue = queue.is_some();
    let queue_for_job = queue.clone();
    deliver(&queue, move || {
        if ACTIVE_HANDLES.load(Ordering::SeqCst) <= 0 {
            return;
        }
        if has_queue {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        enqueue_pump(queue_for_job.clone());
    });
}

// ---------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------

pub fn family_of(address: &IpAddr) -> &'static str {
    if address.is_ipv4() {
        "IPv4"
    } else {
        "IPv6"
    }
}

pub fn family_number(address: &IpAddr) -> i64 {
    if address.is_ipv4() {
        4
    } else {
        6
    }
}

pub fn address_record(address: &SocketAddr) -> crate::UnknownType {
    let mut fields = purust_core::RecordFields::new();
    fields.insert("port".to_owned(), crate::mk_int(address.port() as i64));
    fields.insert(
        "family".to_owned(),
        crate::Value::String(family_of(&address.ip()).to_owned()),
    );
    fields.insert(
        "address".to_owned(),
        crate::Value::String(address.ip().to_string()),
    );
    crate::Value::DynamicRecord(perceus_ptr::PerceusPtr::new(fields))
}

pub fn nullable_handle(value: Option<crate::UnknownType>) -> crate::UnknownType {
    let nullable = match value {
        Some(value) => Purs_Data_Nullable::Data_Nullable_notNull(value),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    };
    crate::Value::Class(Rc::new(nullable))
}

pub fn class_nullable(value: Option<crate::UnknownType>) -> crate::UnknownType {
    nullable_handle(value)
}

pub fn option_field(options: &crate::UnknownType, key: &str) -> Option<crate::UnknownType> {
    if matches!(options.resolve(), crate::Value::Unit) {
        return None;
    }
    options.__purust_foreign_object().get(key)
}

pub fn option_string(options: &crate::UnknownType, key: &str) -> Option<String> {
    option_field(options, key).map(|value| value.unwrap_string())
}

pub fn option_int(options: &crate::UnknownType, key: &str) -> Option<i64> {
    option_field(options, key).map(|value| match value.resolve() {
        crate::Value::Int(number) => *number,
        crate::Value::Number(number) => *number as i64,
        _ => 0,
    })
}

pub fn option_bool(options: &crate::UnknownType, key: &str) -> Option<bool> {
    option_field(options, key).map(|value| value.unwrap_bool())
}

/// Resolves a host/port pair, optionally restricting the IP family (4 or 6).
pub fn resolve(host: &str, port: u16, family: Option<i64>) -> std::io::Result<SocketAddr> {
    let addresses: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    let filtered: Vec<SocketAddr> = addresses
        .into_iter()
        .filter(|address| match family {
            Some(4) => address.is_ipv4(),
            Some(6) => address.is_ipv6(),
            _ => true,
        })
        .collect();
    filtered.into_iter().next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no address for host")
    })
}

// ---------------------------------------------------------------------------
// Socket state
// ---------------------------------------------------------------------------

pub struct SocketState {
    pub queue: Option<crate::UnknownType>,
    pub error_factory: ErrorFactory,
    pub ready_state: String,
    pub connecting: bool,
    pub pending: bool,
    pub destroyed: bool,
    pub had_error: bool,
    pub close_emitted: bool,
    pub bytes_read: i64,
    pub bytes_written: i64,
    pub local: Option<SocketAddr>,
    pub remote: Option<SocketAddr>,
    pub timeout_ms: Option<i64>,
    pub allow_half_open: bool,
    pub fd: Option<i32>,
    /// Higher layers (HTTP) attach their per-connection state here.
    pub extension: Option<crate::UnknownType>,
}

pub type SocketHandle = Arc<Mutex<SocketState>>;

pub fn socket_new_value(
    queue: Option<crate::UnknownType>,
    error_factory: ErrorFactory,
    allow_half_open: bool,
) -> Rc<Socket> {
    let stream = Purs_Node_Stream::purust_stream_duplex();
    let state = Arc::new(Mutex::new(SocketState {
        queue: queue.clone(),
        error_factory,
        ready_state: "opening".to_owned(),
        connecting: false,
        pending: true,
        destroyed: false,
        had_error: false,
        close_emitted: false,
        bytes_read: 0,
        bytes_written: 0,
        local: None,
        remote: None,
        timeout_ms: None,
        allow_half_open,
        fd: None,
        extension: None,
    }));
    Purs_Node_Stream::purust_stream_set_extension(
        &stream,
        crate::Value::Class(Rc::new(state.clone())),
    );
    // Ending the writable side shuts the socket down for writing so peers see
    // EOF, matching `socket.end()`.
    let shutdown_fd = state.clone();
    Purs_Node_Stream::purust_stream_set_end_hook(
        &stream,
        Arc::new(move || {
            let fd = shutdown_fd.lock().unwrap().fd;
            if let Some(fd) = fd {
                unsafe {
                    libc::shutdown(fd, libc::SHUT_WR);
                }
            }
        }),
    );
    handle_open();
    ensure_pump(&queue);
    stream
}

pub fn socket_set_extension(stream: &Rc<Socket>, value: crate::UnknownType) {
    socket_state(stream).lock().unwrap().extension = Some(value);
}

pub fn socket_get_extension(stream: &Rc<Socket>) -> Option<crate::UnknownType> {
    socket_state(stream).lock().unwrap().extension.clone()
}

pub fn server_set_extension(server: &Rc<Server>, value: crate::UnknownType) {
    server_state(server).lock().unwrap().extension = Some(value);
}

pub fn server_get_extension(server: &Rc<Server>) -> Option<crate::UnknownType> {
    server_state(server).lock().unwrap().extension.clone()
}

pub fn socket_state(stream: &Rc<Socket>) -> SocketHandle {
    Purs_Node_Stream::purust_stream_extension(stream)
        .expect("Node.Net: socket without native state")
        .unwrap_class::<SocketHandle>()
        .clone()
}

fn emit_socket_with(socket: &Rc<Socket>, event: &str, args: Vec<crate::UnknownType>) {
    purust_emitter_emit(socket, event, args);
}

pub fn emit_socket(socket: &Rc<Socket>, event: &str, args: Vec<crate::UnknownType>) {
    let queue = socket_state(socket).lock().unwrap().queue.clone();
    let socket = socket.clone();
    let event = event.to_owned();
    deliver(&queue, move || {
        emit_socket_with(&socket, &event, args);
    });
}

/// Marks the socket closed, emits `close` once and releases the handle.
pub fn socket_finish(socket: &Rc<Socket>) {
    let queue = {
        let state = socket_state(socket);
        let mut state = state.lock().unwrap();
        if state.close_emitted {
            return;
        }
        state.close_emitted = true;
        state.destroyed = true;
        state.ready_state = "closed".to_owned();
        state.pending = false;
        let had_error = state.had_error;
        let queue = state.queue.clone();
        (queue, had_error)
    };
    let (queue, had_error) = queue;
    let socket_for_close = socket.clone();
    deliver(&queue, move || {
        purust_emitter_emit(
            &socket_for_close,
            "close",
            vec![crate::mk_bool(had_error)],
        );
    });
    handle_close();
}

/// Destroys a socket immediately (Node's `destroy`).
pub fn socket_destroy(socket: &Rc<Socket>, error: Option<String>) {
    let (queue, error_factory) = {
        let state = socket_state(socket);
        let state = state.lock().unwrap();
        (state.queue.clone(), state.error_factory)
    };
    if let Some(message) = error {
        {
            let state = socket_state(socket);
            state.lock().unwrap().had_error = true;
        }
        let socket_for_error = socket.clone();
        deliver(&queue, move || {
            purust_emitter_emit(&socket_for_error, "error", vec![error_factory(message)]);
        });
    }
    {
        let state = socket_state(socket);
        let fd = state.lock().unwrap().fd;
        if let Some(fd) = fd {
            unsafe {
                libc::shutdown(fd, libc::SHUT_RDWR);
            }
        }
    }
    socket_finish(socket);
}

// ---------------------------------------------------------------------------
// Connecting
// ---------------------------------------------------------------------------

pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub family: Option<i64>,
    pub local_address: Option<String>,
    pub local_port: Option<u16>,
    pub no_delay: bool,
    pub keep_alive: bool,
}

fn socket_allow_half_open(socket: &Rc<Socket>) -> bool {
    socket_state(socket).lock().unwrap().allow_half_open
}

/// Shuts down the writable half of the connection (Node's automatic `end` on
/// peer EOF when `allowHalfOpen` is false).
fn socket_shutdown_write(socket: &Rc<Socket>) {
    let fd = socket_state(socket).lock().unwrap().fd;
    if let Some(fd) = fd {
        unsafe {
            libc::shutdown(fd, libc::SHUT_WR);
        }
    }
}

fn start_read_loop(socket: Rc<Socket>, mut reader: TcpStream) {
    let queue = socket_state(&socket).lock().unwrap().queue.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 65536];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => {
                    // Node ends the writable side on EOF unless allowHalfOpen is
                    // set, which also wakes the peer's read loop and releases
                    // the handle so the process can exit.
                    if !socket_allow_half_open(&socket) {
                        socket_shutdown_write(&socket);
                    }
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Ok(count) => {
                    {
                        let state = socket_state(&socket);
                        state.lock().unwrap().bytes_read += count as i64;
                    }
                    let bytes = buffer[..count].to_vec();
                    let target = socket.clone();
                    deliver(&queue, move || {
                        Purs_Node_Stream::purust_stream_push(&target, bytes);
                    });
                }
                Err(_) => break,
            }
        }
        let target = socket.clone();
        deliver(&queue, move || {
            Purs_Node_Stream::purust_stream_end(&target);
        });
        socket_finish(&socket);
    });
}

fn socket_attached_fd(socket: &Rc<Socket>, stream: &TcpStream) {
    let raw = stream.as_raw_fd();
    let duplicated = unsafe { libc::dup(raw) };
    if duplicated >= 0 {
        {
            let state = socket_state(socket);
            state.lock().unwrap().fd = Some(duplicated);
        }
        Purs_Node_Stream::purust_stream_set_write_fd(socket, duplicated);
    }
}

pub fn socket_connect(socket: Rc<Socket>, options: ConnectOptions) {
    let queue = socket_state(&socket).lock().unwrap().queue.clone();
    {
        let state = socket_state(&socket);
        let mut state = state.lock().unwrap();
        state.connecting = true;
        state.pending = true;
        state.ready_state = "opening".to_owned();
    }
    let socket_for_thread = socket.clone();
    std::thread::spawn(move || {
        let family = options.family;
        let host = options.host.clone();
        let resolved = resolve(&host, options.port, family);
        match resolved {
            Err(error) => {
                let target = socket_for_thread.clone();
                let message = error.to_string();
                let host_for_lookup = host.clone();
                deliver(&queue, move || {
                    let error_factory = socket_state(&target).lock().unwrap().error_factory;
                    purust_emitter_emit(
                        &target,
                        "lookup",
                        vec![
                            class_nullable(Some(error_factory(message.clone()))),
                            crate::Value::String(String::new()),
                            class_nullable(None),
                            crate::Value::String(host_for_lookup.clone()),
                        ],
                    );
                });
                socket_destroy(&socket_for_thread, Some(format!("connect ECONNREFUSED {host}")));
                return;
            }
            Ok(address) => {
                let target = socket_for_thread.clone();
                let address_text = address.ip().to_string();
                let family_number = family_number(&address.ip());
                let host_text = host.clone();
                deliver(&queue, move || {
                    purust_emitter_emit(
                        &target,
                        "lookup",
                        vec![
                            class_nullable(None),
                            crate::Value::String(address_text.clone()),
                            class_nullable(Some(crate::mk_int(family_number))),
                            crate::Value::String(host_text.clone()),
                        ],
                    );
                });
                let connected = match (options.local_address.as_ref(), options.local_port) {
                    (Some(local_address), Some(local_port)) => {
                        resolve(local_address, local_port, family)
                            .and_then(|local| connect_with_local(address, local))
                    }
                    _ => TcpStream::connect(address),
                };
                match connected {
                    Ok(stream) => {
                        let _ = stream.set_nodelay(options.no_delay);
                        if options.keep_alive {
                            let fd = stream.as_raw_fd();
                            let enable: libc::c_int = 1;
                            unsafe {
                                libc::setsockopt(
                                    fd,
                                    libc::SOL_SOCKET,
                                    libc::SO_KEEPALIVE,
                                    &enable as *const _ as *const libc::c_void,
                                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                                );
                            }
                        }
                        let local = stream.local_addr().ok();
                        let remote = stream.peer_addr().ok();
                        {
                            let state = socket_state(&socket_for_thread);
                            let mut state = state.lock().unwrap();
                            state.local = local;
                            state.remote = remote;
                            state.connecting = false;
                            state.pending = false;
                            state.ready_state = "open".to_owned();
                        }
                        socket_attached_fd(&socket_for_thread, &stream);
                        let target = socket_for_thread.clone();
                        deliver(&queue, move || {
                            purust_emitter_emit(&target, "connect", Vec::new());
                            purust_emitter_emit(&target, "ready", Vec::new());
                        });
                        start_read_loop(socket_for_thread.clone(), stream);
                    }
                    Err(error) => {
                        socket_destroy(
                            &socket_for_thread,
                            Some(format!("connect ECONNREFUSED {host}: {error}")),
                        );
                    }
                }
            }
        }
    });
}

fn sockaddr_bytes(address: &SocketAddr) -> (*const libc::sockaddr, libc::socklen_t) {
    match address {
        SocketAddr::V4(address) => {
            let raw = libc::sockaddr_in {
                sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: address.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from(*address.ip()).to_be(),
                },
                sin_zero: [0; 8],
            };
            let boxed = Box::leak(Box::new(raw));
            (
                boxed as *const libc::sockaddr_in as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(address) => {
            let raw = libc::sockaddr_in6 {
                sin6_len: std::mem::size_of::<libc::sockaddr_in6>() as u8,
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: address.port().to_be(),
                sin6_flowinfo: address.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: address.ip().octets(),
                },
                sin6_scope_id: address.scope_id(),
            };
            let boxed = Box::leak(Box::new(raw));
            (
                boxed as *const libc::sockaddr_in6 as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

/// Connects after binding to a specific local address (Node's localAddress).
fn connect_with_local(address: SocketAddr, local: SocketAddr) -> std::io::Result<TcpStream> {
    use std::os::unix::io::FromRawFd;
    let family = if address.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    let (local_ptr, local_length) = sockaddr_bytes(&local);
    if unsafe { libc::bind(fd, local_ptr, local_length) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let (remote_ptr, remote_length) = sockaddr_bytes(&address);
    if unsafe { libc::connect(fd, remote_ptr, remote_length) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(stream)
}

pub fn socket_options(options: &crate::UnknownType) -> ConnectOptions {
    ConnectOptions {
        host: option_string(options, "host").unwrap_or_else(|| "localhost".to_owned()),
        port: option_int(options, "port").unwrap_or(0) as u16,
        family: option_int(options, "family"),
        local_address: option_string(options, "localAddress").filter(|value| !value.is_empty()),
        local_port: option_int(options, "localPort").map(|value| value as u16),
        no_delay: option_bool(options, "noDelay").unwrap_or(false),
        keep_alive: option_bool(options, "keepAlive").unwrap_or(false),
    }
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

pub struct ServerState {
    pub queue: Option<crate::UnknownType>,
    pub error_factory: ErrorFactory,
    pub listener: Option<TcpListener>,
    pub listening: bool,
    pub closed: bool,
    pub close_emitted: bool,
    pub port: u16,
    pub host: String,
    pub max_connections: i64,
    pub connections: i64,
    pub allow_half_open: bool,
    /// Higher layers (HTTP) attach their per-server state here.
    pub extension: Option<crate::UnknownType>,
}

pub type ServerHandle = Arc<Mutex<ServerState>>;

pub fn server_new_value(queue: Option<crate::UnknownType>, error_factory: ErrorFactory) -> Rc<Server> {
    let server = Rc::new(EventEmitter::new_native());
    let state = Arc::new(Mutex::new(ServerState {
        queue: queue.clone(),
        error_factory,
        listener: None,
        listening: false,
        closed: false,
        close_emitted: false,
        port: 0,
        host: String::new(),
        max_connections: -1,
        connections: 0,
        allow_half_open: false,
        extension: None,
    }));
    server.set_user_data(crate::Value::Class(Rc::new(state)));
    handle_open();
    ensure_pump(&queue);
    server
}

pub fn server_state(server: &Rc<Server>) -> ServerHandle {
    server
        .user_data()
        .expect("Node.Net: server without native state")
        .unwrap_class::<ServerHandle>()
        .clone()
}

fn accepted_socket(
    queue: Option<crate::UnknownType>,
    error_factory: ErrorFactory,
    stream: TcpStream,
    allow_half_open: bool,
) -> Rc<Socket> {
    // BSD/macOS accepted sockets inherit O_NONBLOCK from the listener; the
    // blocking read loop below expects a blocking stream.
    let _ = stream.set_nonblocking(false);
    let socket = socket_new_value(queue, error_factory, allow_half_open);
    {
        let state = socket_state(&socket);
        let mut state = state.lock().unwrap();
        state.local = stream.local_addr().ok();
        state.remote = stream.peer_addr().ok();
        state.connecting = false;
        state.pending = false;
        state.ready_state = "open".to_owned();
    }
    socket_attached_fd(&socket, &stream);
    start_read_loop(socket.clone(), stream);
    socket
}

pub fn server_listen(server: &Rc<Server>, host: String, port: u16, backlog: i32, ipv6_only: bool) {
    let (queue, error_factory, allow_half_open) = {
        let state = server_state(server);
        let state = state.lock().unwrap();
        (state.queue.clone(), state.error_factory, state.allow_half_open)
    };
    let bind_host = if host.is_empty() { "0.0.0.0".to_owned() } else { host.clone() };
    let listener = resolve(&bind_host, port, None).and_then(|address| {
        let listener = if address.is_ipv4() {
            TcpListener::bind(address)
        } else {
            match std::net::TcpListener::bind(address) {
                Ok(listener) => Ok(listener),
                Err(error) => Err(error),
            }
        };
        let _ = ipv6_only;
        let _ = backlog;
        listener
    });
    match listener {
        Ok(listener) => {
            if let Ok(local) = listener.local_addr() {
                let state = server_state(server);
                let mut state = state.lock().unwrap();
                state.port = local.port();
                state.host = local.ip().to_string();
            }
            {
                let state = server_state(server);
                let mut state = state.lock().unwrap();
                state.listener = Some(listener.try_clone().expect("clone listener"));
                state.listening = true;
            }
            let server_for_emit = server.clone();
            deliver(&queue, move || {
                purust_emitter_emit(&server_for_emit, "listening", Vec::new());
            });
            let server_for_accept = server.clone();
            std::thread::spawn(move || {
                let _ = listener.set_nonblocking(true);
                loop {
                    let closed = {
                        let state = server_state(&server_for_accept);
                        let state = state.lock().unwrap();
                        !state.listening || state.closed
                    };
                    if closed {
                        break;
                    }
                    match listener.accept() {
                        Ok((stream, _peer)) => {
                            let socket = accepted_socket(
                                queue.clone(),
                                error_factory,
                                stream,
                                allow_half_open,
                            );
                            {
                                let state = server_state(&server_for_accept);
                                state.lock().unwrap().connections += 1;
                            }
                            let server_for_connection = server_for_accept.clone();
                            deliver(&queue, move || {
                                purust_emitter_emit(
                                    &server_for_connection,
                                    "connection",
                                    vec![crate::Value::Class(Rc::new(socket))],
                                );
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });
        }
        Err(error) => {
            let server_for_error = server.clone();
            let message = format!("listen {bind_host}:{port}: {error}");
            deliver(&queue, move || {
                purust_emitter_emit(
                    &server_for_error,
                    "error",
                    vec![error_factory(message)],
                );
            });
        }
    }
}

pub fn server_close(server: &Rc<Server>) {
    let (queue, already_closed) = {
        let state = server_state(server);
        let mut state = state.lock().unwrap();
        if state.close_emitted {
            (state.queue.clone(), true)
        } else {
            state.listening = false;
            state.closed = true;
            state.close_emitted = true;
            state.listener = None;
            (state.queue.clone(), false)
        }
    };
    if already_closed {
        return;
    }
    let server = server.clone();
    deliver(&queue, move || {
        purust_emitter_emit(&server, "close", Vec::new());
    });
    handle_close();
}

// ---------------------------------------------------------------------------
// Socket addresses and block lists
// ---------------------------------------------------------------------------

pub struct SocketAddressState {
    pub address: String,
    pub port: i64,
    pub family: String,
    pub flow_label: Option<i64>,
}

pub type SocketAddress = SocketAddressState;

pub struct BlockListState {
    pub rules: Vec<BlockRule>,
}

pub type BlockList = Mutex<BlockListState>;

pub enum BlockRule {
    Address { address: IpAddr, family: String },
    Subnet { network: IpAddr, prefix: u32, family: String },
    Range { start: IpAddr, end: IpAddr, family: String },
}

pub fn ip_from_value(value: &crate::UnknownType) -> Option<IpAddr> {
    if let crate::Value::Class(payload) = value.resolve() {
        if let Some(address) = payload.downcast_ref::<Rc<SocketAddressState>>() {
            return address.address.parse().ok();
        }
    }
    match value.resolve() {
        crate::Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

pub fn family_name(value: &crate::UnknownType) -> String {
    value.unwrap_string()
}

fn parse_prefix(network: IpAddr, prefix: u32) -> IpAddr {
    // Mask the network address down to the prefix so `check` can compare.
    match network {
        IpAddr::V4(address) => {
            let bits = u32::from(address);
            let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix.min(32)) };
            IpAddr::V4(std::net::Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(address) => {
            let bits = u128::from(address);
            let mask = if prefix == 0 { 0 } else { u128::MAX << (128 - prefix.min(128)) };
            IpAddr::V6(std::net::Ipv6Addr::from(bits & mask))
        }
    }
}

fn ip_bits(address: &IpAddr) -> u128 {
    match address {
        IpAddr::V4(value) => u32::from(*value) as u128,
        IpAddr::V6(value) => u128::from(*value),
    }
}

pub fn block_list_check(rules: &[BlockRule], address: IpAddr, family: &str) -> bool {
    rules.iter().any(|rule| match rule {
        BlockRule::Address { address: known, family: rule_family } => {
            rule_family == family && *known == address
        }
        BlockRule::Subnet { network, prefix, family: rule_family } => {
            rule_family == family && match (network, address) {
                (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_)) => {
                    let mask: u128 = if *prefix == 0 {
                        0
                    } else if network.is_ipv4() {
                        (u32::MAX << (32 - (*prefix).min(32))) as u128
                    } else {
                        u128::MAX << (128 - (*prefix).min(128))
                    };
                    ip_bits(network) & mask == ip_bits(&address) & mask
                }
                _ => false,
            }
        }
        BlockRule::Range { start, end, family: rule_family } => {
            rule_family == family
                && match (start, end, address) {
                    (IpAddr::V4(_), IpAddr::V4(_), IpAddr::V4(_))
                    | (IpAddr::V6(_), IpAddr::V6(_), IpAddr::V6(_)) => {
                        let value = ip_bits(&address);
                        value >= ip_bits(start) && value <= ip_bits(end)
                    }
                    _ => false,
                }
        }
    })
}

pub fn block_list_rule_text(rule: &BlockRule) -> String {
    match rule {
        BlockRule::Address { address, family } => format!("Address: {address} {family}"),
        BlockRule::Subnet { network, prefix, family } => {
            format!("Subnet: {network}/{prefix} {family}")
        }
        BlockRule::Range { start, end, family } => {
            format!("Range: {start}-{end} {family}")
        }
    }
}

// ---------------------------------------------------------------------------
// Native views shared with the PureScript wrappers
// ---------------------------------------------------------------------------

pub fn Node_Net_Types_toEventEmitterImpl(socket: Rc<Socket>) -> Rc<EventEmitter> {
    socket
}

pub fn Node_Net_Types_toDuplexImpl(socket: Rc<Socket>) -> crate::UnknownType {
    crate::Value::Class(Rc::new(socket))
}
