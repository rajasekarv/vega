# native_spark

[![Join the chat at https://gitter.im/fast_spark/community](https://badges.gitter.im/fast_spark/community.svg)](https://gitter.im/fast_spark/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/rajasekarv/native_spark.svg?branch=master)](https://travis-ci.org/rajasekarv/native_spark)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**[Documentation](https://rajasekarv.github.io/native_spark/)**

A new, arguably faster, implementation of Apache Spark from scratch in Rust. WIP

Framework tested only on Linux, requires nightly Rust. Read how to get started in the [documentation](https://rajasekarv.github.io/native_spark/chapter_1.html).

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
- [x] union
- [x] glom
- [x] cartesian
- [ ] pipe
- [x] map_partitions
- [x] for_each
- [x] collect
- [x] reduce
- [x] fold
- [x] aggregate
- [x] take
- [x] first
- [x] sample
- [ ] zip
- [ ] save_as_text_file (can save only as text file in executors local file system)  

### Config Files

- [ ] Replace hard coded values