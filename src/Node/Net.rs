// `net.isIP` and friends.
pub fn Node_Net_isIPImpl(value: String) -> i64 {
    match value.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(_)) => 4,
        Ok(std::net::IpAddr::V6(_)) => 6,
        Err(_) => 0,
    }
}

pub fn Node_Net_isIPv4(value: String) -> bool {
    value.parse::<std::net::Ipv4Addr>().is_ok()
}

pub fn Node_Net_isIPv6(value: String) -> bool {
    value.parse::<std::net::Ipv6Addr>().is_ok()
}
