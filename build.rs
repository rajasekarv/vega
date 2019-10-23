extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/capnp/serialized_data.capnp")
        .run()
        .expect("capnpc compiling issue");
}
