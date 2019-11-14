# native_spark

[![Join the chat at https://gitter.im/fast_spark/community](https://badges.gitter.im/fast_spark/community.svg)](https://gitter.im/fast_spark/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/rajasekarv/native_spark.svg?branch=master)](https://travis-ci.org/rajasekarv/native_spark)

A new arguably faster implementation of Apache Spark from scratch in Rust. WIP

Code is tested only on Linux and requires nightly version.

Refer make_rdd.rs and other examples in example code to get the basic idea.

## Instructions for running the code in Local and Distributed mode
First, Install [Cap'n Proto](https://capnproto.org/install.html)
Then, run these following steps:(Tested only on Linux)
* `git clone https://github.com/rajasekarv/native_spark/`
* `cd native_spark`
* You need to have [hosts.conf](https://github.com/rajasekarv/native_spark/blob/master/config_files/hosts.conf) in the format present inside config folder in the home directory of all the machines when running in distributed mode and all of them should be ssh-able from master. Change master-ip as 0.0.0.0 in case of **local mode**. Setup appropriate IPs of master and executor nodes for distributed mode.
* The master port can be configured in hosts.conf and port 10500 in executors should be free. Ports 5000-6000 is reserved for shuffle manager. It will be handled internally soon.
* `export SPARK_LOCAL_IP=0.0.0.0` when running in local mode and appropriate IP in all machines when running in distributed mode.
* `cargo run --example make_rdd`

Since File readers are not done, you have to use manual file reading for now (like manually reading from S3 or hack around local files by distributing copies of all files to all machines and make rdd using filename list).

Ctrl-C handling and panic handling is not done yet, so if there is some problem in runtime, executors won't shut down automatically and you have to manually kill the processes.

One of the limitations of current implementation is that the input and return types of all closures and all input to make_rdd should be owned data.

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
- [x] take_sample
- [ ] union
- [x] glom
- [ ] cartesian
- [ ] pipe
- [x] map_partitions
- [x] for_each
- [x] collect
- [x] reduce
- [x] fold
- [ ] aggregate
- [x] take
- [x] first
- [x] sample
- [ ] zip
- [ ] save_as_text_file(can save only as text file in executors local file system)  

### Config Files

- [ ] Replace hard coded values