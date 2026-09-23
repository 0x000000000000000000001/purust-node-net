// `Node.Net.BlockList` FFIs.
use std::rc::Rc;

use Purs_Node_Net_Types::{
    block_list_check, block_list_rule_text, family_name, ip_from_value, parse_prefix, BlockList,
    BlockListState, BlockRule,
};

fn unbox(value: &crate::UnknownType) -> Rc<BlockList> {
    value.unwrap_class::<Rc<BlockList>>().clone()
}

fn address_family(value: &crate::UnknownType, family: &str) -> Option<std::net::IpAddr> {
    let address = ip_from_value(value)?;
    let expected = if family == "ipv6" { "IPv6" } else { "IPv4" };
    if Purs_Node_Net_Types::family_of(&address) == expected {
        Some(address)
    } else {
        None
    }
}

pub fn Node_Net_BlockList_addAddressImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|block_list, value, family| {
        let block_list = unbox(&block_list);
        let family = family_name(&family);
        if let Some(address) = address_family(&value, &family) {
            block_list.lock().unwrap().rules.push(BlockRule::Address { address, family });
        }
        crate::Value::Unit
    })))
}

pub fn Node_Net_BlockList_addRangeImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |block_list, start, end, family| {
            let block_list = unbox(&block_list);
            let family = family_name(&family);
            if let (Some(start), Some(end)) = (
                address_family(&start, &family),
                address_family(&end, &family),
            ) {
                block_list.lock().unwrap().rules.push(BlockRule::Range {
                    start,
                    end,
                    family,
                });
            }
            crate::Value::Unit
        },
    )))
}

pub fn Node_Net_BlockList_addSubnetImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |block_list, value, prefix, family| {
            let block_list = unbox(&block_list);
            let family = family_name(&family);
            if let Some(network) = address_family(&value, &family) {
                let prefix = prefix.unwrap_int().max(0) as u32;
                block_list.lock().unwrap().rules.push(BlockRule::Subnet {
                    network: parse_prefix(network, prefix),
                    prefix,
                    family,
                });
            }
            crate::Value::Unit
        },
    )))
}

pub fn Node_Net_BlockList_checkImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|block_list, value, family| {
        let block_list = unbox(&block_list);
        let family = family_name(&family);
        let matched = match address_family(&value, &family) {
            Some(address) => {
                let rules = block_list.lock().unwrap();
                block_list_check(&rules.rules, address, if family == "ipv6" { "IPv6" } else { "IPv4" })
            }
            None => false,
        };
        crate::mk_bool(matched)
    })))
}

pub fn Node_Net_BlockList_rulesImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let block_list = unbox(&value);
        let rules = block_list.lock().unwrap();
        let values = rules
            .rules
            .iter()
            .map(|rule| crate::Value::String(block_list_rule_text(rule)))
            .collect();
        crate::mk_array(values)
    })))
}
