use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::process::Command;

// function to get the local ip for the local network
pub fn get_v4() -> String {
    match get().unwrap() {
        IpAddr::V4(s) => s.to_string(),
        IpAddr::V6(s) => s.to_string(),
    }
}

//credit - https://github.com/ivanceras/machine-ip/
pub fn get() -> Option<IpAddr> {
    let output = match Command::new("hostname").args(&["-I"]).output() {
        Ok(ok) => ok,
        Err(_) => {
            return None;
        }
    };

    let stdout = match String::from_utf8(output.stdout) {
        Ok(ok) => ok,
        Err(_) => {
            return None;
        }
    };

    let ips: Vec<&str> = stdout.trim().split(" ").collect::<Vec<&str>>();
    let first = ips.first();
    match first {
        Some(first) => {
            if !first.is_empty() {
                if let Ok(addr) = first.parse::<Ipv4Addr>() {
                    return Some(IpAddr::V4(addr));
                } else if let Ok(addr) = first.parse::<Ipv6Addr>() {
                    return Some(IpAddr::V6(addr));
                } else {
                    None
                }
            } else {
                None
            }
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let res = get();
        println!("res: {:?}", res);
        assert!(res.is_some());
        println!("res string {:?}", get_v4());
    }
}
