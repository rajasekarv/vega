# native_spark

[![Join the chat at https://gitter.im/fast_spark/community](https://badges.gitter.im/fast_spark/community.svg)](https://gitter.im/fast_spark/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A new arguably faster implementation of Apache Spark from scratch in Rust. WIP

Just install [Cap'n proto](https://capnproto.org/install.html) and you are good to go. Code is tested only on Linux and requires nightly version. It is tested for version 1.39 only, there are some breaking changes in specialization from version to version, so use 1.39 only for now.

Use this command: `cargo +nightly-2019-09-11 build --release`

Refer make_rdd.rs and other examples in example code to get the basic idea.

You need to have hosts.conf in the format present inside config folder in the home directory of all of the machines when running in distributed mode and all of them should be ssh-able from master.
The master port can be configured in hosts.conf and 10500 in executors should be free. Ports 5000-6000 is reserved for shuffle manager. It will be handled internally soon.

Since File readers are not done, you have to use manual file reading for now (like manually reading from S3 or hack around local files by distributing copies of all files to all machines and make rdd using filename list).

Ctrl-C handling and panic handling is not done yet, so if there is some problem in runtime, executors won't be shut down automatically and you have to manually kill the processes.

One of the limitations of current implementation is that the input and return types of all closures and all input to make_rdd should be owned data.

## Configuration

You can specify the local IP address using the environmental variable `SPARK_LOCAL_IP`.

## ToDo

- [x] Error Handling(Priority)
- [ ] Fault tolerance

### RDD

Most of these except file reader and writer are trivial to implement

- [x] map
- [x] flat_map
- [x] filter
- [x] group_by
- [x] reduce_by
- [x] distinct
- [x] count
- [ ] take_sample
- [ ] union
- [ ] glom
- [ ] cartesian
- [ ] pipe
- [ ] map_partitions
- [ ] for_each
- [x] collect
- [x] reduce
- [ ] fold
- [ ] aggregate
- [x] take
- [x] first
- [x] sample
- [ ] save_as_text_file(can save only as text file in executors local file system)  

### Config Files

- [ ] Replace hard coded values
