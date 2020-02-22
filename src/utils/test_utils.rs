use std::net::TcpListener;

pub fn get_free_port() -> u16 {
    let mut port = 0;
    for _ in 0..100 {
        port = crate::utils::get_dynamic_port();
        if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
            return port;
        }
    }
    panic!("failed to find free port while testing");
}
