// `Node.Net.Server` FFIs.
use std::rc::Rc;

use Purs_Node_Net_Types::{
    address_record, class_nullable, option_bool, option_int, option_string, server_close,
    server_listen, server_new_value, server_state, Server,
};

fn error(message: String) -> crate::UnknownType {
    Purs_Effect_Exception::Effect_Exception_error(message)
}

fn unbox_server(value: &crate::UnknownType) -> Rc<Server> {
    value.unwrap_class::<Rc<Server>>().clone()
}

fn server_from_options(options: &crate::UnknownType) -> Rc<Server> {
    let server = server_new_value(Purs_Node_Net_Types::queue_value(), error);
    {
        let allow_half_open = option_bool(options, "allowHalfOpen").unwrap_or(false);
        let state = server_state(&server);
        state.lock().unwrap().allow_half_open = allow_half_open;
    }
    server
}

pub fn Node_Net_Server_newServerImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| {
        crate::Value::Class(Rc::new(server_from_options(&crate::Value::Unit)))
    }))
}

pub fn Node_Net_Server_newServerOptionsImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|options| {
        crate::Value::Class(Rc::new(server_from_options(&options)))
    })))
}

pub fn Node_Net_Server_addressTcpImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let server = unbox_server(&value);
        let address = {
            let state = server_state(&server);
            let state = state.lock().unwrap();
            state.listener.as_ref().and_then(|listener| listener.local_addr().ok())
        };
        class_nullable(address.map(|address| address_record(&address)))
    })))
}

pub fn Node_Net_Server_addressIpcImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| class_nullable(None)))
}

pub fn Node_Net_Server_closeImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let server = unbox_server(&value);
        server_close(&server);
        crate::Value::Unit
    })))
}

pub fn Node_Net_Server_getConnectionsImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, callback| {
        let server = unbox_server(&value);
        let count = {
            let state = server_state(&server);
            let state = state.lock().unwrap();
            state.connections
        };
        // Node reports `null` on success; the PureScript type is a plain Error.
        callback.unwrap_func2()(class_nullable(None), crate::mk_int(count));
        crate::Value::Unit
    })))
}

pub fn Node_Net_Server_listenImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, options| {
        let server = unbox_server(&value);
        let port = option_int(&options, "port").unwrap_or(0) as u16;
        let host = option_string(&options, "host").unwrap_or_default();
        let backlog = option_int(&options, "backlog").unwrap_or(511) as i32;
        let ipv6_only = option_bool(&options, "ipv6Only").unwrap_or(false);
        server_listen(&server, host, port, backlog, ipv6_only);
        crate::Value::Unit
    })))
}

pub fn Node_Net_Server_listeningImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let server = unbox_server(&value);
        let listening = {
            let state = server_state(&server);
            let state = state.lock().unwrap();
            state.listening
        };
        crate::mk_bool(listening)
    })))
}

pub fn Node_Net_Server_maxConnectionsImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let server = unbox_server(&value);
        let max = {
            let state = server_state(&server);
            let state = state.lock().unwrap();
            state.max_connections
        };
        crate::mk_int(max)
    })))
}

pub fn Node_Net_Server_refImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| crate::Value::Unit))
}

pub fn Node_Net_Server_unrefImpl() -> crate::UnknownType {
    Node_Net_Server_refImpl()
}
