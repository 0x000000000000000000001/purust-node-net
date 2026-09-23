// `Node.Net.Socket` FFIs, backed by the native socket machinery.
use std::net::SocketAddr;
use std::rc::Rc;

use Purs_Node_Net_Types::{
    address_record, option_bool, queue_value, socket_connect, socket_destroy, socket_finish,
    socket_new_value, socket_options, socket_state, Socket,
};

fn error(message: String) -> crate::UnknownType {
    Purs_Effect_Exception::Effect_Exception_error(message)
}

fn unbox_socket(value: &crate::UnknownType) -> Rc<Socket> {
    value.unwrap_class::<Rc<Socket>>().clone()
}

fn socket_from_options(options: &crate::UnknownType) -> Rc<Socket> {
    let allow_half_open = option_bool(options, "allowHalfOpen").unwrap_or(false);
    socket_new_value(queue_value(), error, allow_half_open)
}

fn milliseconds(value: &crate::UnknownType) -> i64 {
    match value.resolve() {
        crate::Value::Int(number) => *number,
        crate::Value::Number(number) => *number as i64,
        crate::Value::Class(payload) => payload
            .downcast_ref::<i64>()
            .copied()
            .unwrap_or(0),
        _ => 0,
    }
}

fn start_timeout_timer(socket: &Rc<Socket>, milliseconds: i64) {
    let socket = socket.clone();
    {
        let state = socket_state(&socket);
        state.lock().unwrap().timeout_ms = Some(milliseconds);
    }
    if milliseconds <= 0 {
        return;
    }
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(milliseconds as u64));
        let still_open = {
            let state = socket_state(&socket);
            let state = state.lock().unwrap();
            !state.destroyed && state.timeout_ms == Some(milliseconds)
        };
        if still_open {
            let target = socket.clone();
            deliver(&socket_state(&socket).lock().unwrap().queue.clone(), move || {
                Purs_Node_EventEmitter::purust_emitter_emit(&target, "timeout", Vec::new());
            });
        }
    });
}

fn deliver(queue: &Option<crate::UnknownType>, job: impl FnOnce() + Send + Sync + 'static) {
    Purs_Node_Net_Types::deliver(queue, job)
}

pub fn Node_Net_Socket_newImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|options| {
        let socket = socket_from_options(&options);
        crate::Value::Class(Rc::new(socket))
    })))
}

pub fn Node_Net_Socket_createConnectionImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|options| {
        let socket = socket_from_options(&options);
        socket_connect(socket.clone(), socket_options(&options));
        crate::Value::Class(Rc::new(socket))
    })))
}

pub fn Node_Net_Socket_connectTcpImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|socket, options| {
        let socket = unbox_socket(&socket);
        socket_connect(socket.clone(), socket_options(&options));
        crate::Value::Class(Rc::new(socket))
    })))
}

pub fn Node_Net_Socket_connectIpcImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|socket, path| {
        let socket = unbox_socket(&socket);
        let path = path.unwrap_string();
        let target = socket.clone();
        let queue = socket_state(&socket).lock().unwrap().queue.clone();
        deliver(&queue, move || {
            Purs_Node_EventEmitter::purust_emitter_emit(
                &target,
                "error",
                vec![error(format!("IPC sockets are not supported: {path}"))],
            );
        });
        socket_finish(&socket);
        crate::Value::Class(Rc::new(socket))
    })))
}

pub fn Node_Net_Socket_addressImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        let address = socket_state(&socket).lock().unwrap().local;
        match address {
            Some(address) => address_record(&address),
            None => address_record(&SocketAddr::from(([0, 0, 0, 0], 0))),
        }
    })))
}

pub fn Node_Net_Socket_bytesReadImpl() -> crate::UnknownType {
    socket_int(|state| state.bytes_read)
}

pub fn Node_Net_Socket_bytesWrittenImpl() -> crate::UnknownType {
    socket_int(|state| state.bytes_written)
}

fn socket_int(project: impl Fn(&Purs_Node_Net_Types::SocketState) -> i64 + 'static) -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |value| {
        let socket = unbox_socket(&value);
        let amount = {
            let state = socket_state(&socket);
            let state = state.lock().unwrap();
            project(&state)
        };
        crate::mk_int(amount)
    })))
}

pub fn Node_Net_Socket_connectingImpl() -> crate::UnknownType {
    socket_bool(|state| state.connecting)
}

pub fn Node_Net_Socket_pendingImpl() -> crate::UnknownType {
    socket_bool(|state| state.pending)
}

fn socket_bool(project: impl Fn(&Purs_Node_Net_Types::SocketState) -> bool + 'static) -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |value| {
        let socket = unbox_socket(&value);
        let flag = {
            let state = socket_state(&socket);
            let state = state.lock().unwrap();
            project(&state)
        };
        crate::mk_bool(flag)
    })))
}

pub fn Node_Net_Socket_destroySoonImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        socket_finish(&socket);
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_resetAndDestroyImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        socket_destroy(&socket, None);
        crate::Value::Unit
    })))
}

fn socket_text(project: impl Fn(&Purs_Node_Net_Types::SocketState) -> String + 'static) -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |value| {
        let socket = unbox_socket(&value);
        let text = {
            let state = socket_state(&socket);
            let state = state.lock().unwrap();
            project(&state)
        };
        crate::Value::String(text)
    })))
}

pub fn Node_Net_Socket_localAddressImpl() -> crate::UnknownType {
    socket_text(|state| state.local.map(|address| address.ip().to_string()).unwrap_or_default())
}

pub fn Node_Net_Socket_localFamilyImpl() -> crate::UnknownType {
    socket_text(|state| {
        state
            .local
            .map(|address| Purs_Node_Net_Types::family_of(&address.ip()).to_owned())
            .unwrap_or_default()
    })
}

pub fn Node_Net_Socket_remoteAddressImpl() -> crate::UnknownType {
    socket_text(|state| state.remote.map(|address| address.ip().to_string()).unwrap_or_default())
}

pub fn Node_Net_Socket_remoteFamilyImpl() -> crate::UnknownType {
    socket_text(|state| {
        state
            .remote
            .map(|address| Purs_Node_Net_Types::family_of(&address.ip()).to_owned())
            .unwrap_or_default()
    })
}

fn socket_port(project: impl Fn(&Purs_Node_Net_Types::SocketState) -> i64 + 'static) -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |value| {
        let socket = unbox_socket(&value);
        let port = {
            let state = socket_state(&socket);
            let state = state.lock().unwrap();
            project(&state)
        };
        crate::mk_int(port)
    })))
}

pub fn Node_Net_Socket_localPortImpl() -> crate::UnknownType {
    socket_port(|state| state.local.map(|address| address.port() as i64).unwrap_or(0))
}

pub fn Node_Net_Socket_remotePortImpl() -> crate::UnknownType {
    socket_port(|state| state.remote.map(|address| address.port() as i64).unwrap_or(0))
}

pub fn Node_Net_Socket_refImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| crate::Value::Unit))
}

pub fn Node_Net_Socket_unrefImpl() -> crate::UnknownType {
    Node_Net_Socket_refImpl()
}

fn set_option(socket: &Rc<Socket>, option: libc::c_int, value: libc::c_int) {
    let state = socket_state(socket);
    let fd = state.lock().unwrap().fd;
    if let Some(fd) = fd {
        unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                option,
                &value as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

pub fn Node_Net_Socket_setKeepAliveImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        let enable: libc::c_int = 1;
        let state = socket_state(&socket);
        let fd = state.lock().unwrap().fd;
        if let Some(fd) = fd {
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
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_setKeepAliveBooleanImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, enable| {
        let socket = unbox_socket(&value);
        let enable: libc::c_int = if enable.unwrap_bool() { 1 } else { 0 };
        let state = socket_state(&socket);
        let fd = state.lock().unwrap().fd;
        if let Some(fd) = fd {
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
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_setKeepAliveInitialDelayImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Static(|_, _| crate::Value::Unit))
}

pub fn Node_Net_Socket_setKeepAliveAllImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Static(|_, _, _| crate::Value::Unit))
}

pub fn Node_Net_Socket_setNoDelayImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        set_option(&socket, libc::TCP_NODELAY, 1);
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_setNoDelayBooleanImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, enable| {
        let socket = unbox_socket(&value);
        set_option(&socket, libc::TCP_NODELAY, if enable.unwrap_bool() { 1 } else { 0 });
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_setTimeoutImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, timeout| {
        let socket = unbox_socket(&value);
        start_timeout_timer(&socket, milliseconds(&timeout));
        crate::Value::Unit
    })))
}

pub fn Node_Net_Socket_timeoutImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let socket = unbox_socket(&value);
        let timeout = socket_state(&socket).lock().unwrap().timeout_ms;
        let nullable = match timeout {
            Some(timeout) => Purs_Data_Nullable::Data_Nullable_notNull(crate::mk_int(timeout)),
            None => Purs_Data_Nullable::Data_Nullable_null(),
        };
        crate::Value::Class(Rc::new(nullable))
    })))
}

pub fn Node_Net_Socket_readyStateImpl() -> crate::UnknownType {
    socket_text(|state| state.ready_state.clone())
}
