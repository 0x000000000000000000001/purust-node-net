// `Node.Net.SocketAddress` FFIs.
use std::rc::Rc;

use Purs_Node_Net_Types::{option_int, option_string, SocketAddress, SocketAddressState};

pub fn Node_Net_SocketAddress_newImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|options| {
        let family = option_string(&options, "family").unwrap_or_else(|| "ipv4".to_owned());
        let default_address = if family == "ipv6" { "::" } else { "127.0.0.1" };
        let address = option_string(&options, "address").unwrap_or_else(|| default_address.to_owned());
        let port = option_int(&options, "port").unwrap_or(0);
        let flow_label = option_int(&options, "flowLabel");
        crate::Value::Class(Rc::new(Rc::new(SocketAddressState {
            address,
            port,
            family,
            flow_label,
        })))
    })))
}

pub fn Node_Net_SocketAddress_address(value: Rc<SocketAddress>) -> String {
    value.address.clone()
}

pub fn Node_Net_SocketAddress_familyImpl(value: Rc<SocketAddress>) -> String {
    value.family.clone()
}

pub fn Node_Net_SocketAddress_flowLabelImpl(value: Rc<SocketAddress>) -> Rc<Purs_Data_Nullable::Nullable> {
    match value.flow_label {
        Some(flow_label) => Purs_Data_Nullable::Data_Nullable_notNull(crate::mk_int(flow_label)),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }
}

pub fn Node_Net_SocketAddress_port(value: Rc<SocketAddress>) -> i64 {
    value.port
}
