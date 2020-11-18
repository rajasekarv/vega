# Introduction

`vega` is a distributed computing framework inspired by Apache Spark.

## Getting started

### Setting up Rust

Vega requires Rust Nightly channel because it depends on libraries that require Nightly (`serde_traitobject` -> `metatype`).
Ensure that you have and are using a Nightly toolchain when
building examples.

```doc
$ rustup toolchain install nightly
```
    
Then set the default, or pass the toolchain in when invoking Cargo:

```doc
$ rustup default nightly
```

### Installing Vega

Right now, the framework lacks any sort of cluster manager of submit program/script.

In order to use the framework, you have to clone the repository and add the local dependency or add the upstream GitHub repository to your Rust project (the crate is not yet published on [crates.io](https://crates.io/)). E.g. add to your application Cargo.toml or:

```doc
[dependencies]
vega = { path = "/path/to/local/git/repo" }
# or
vega = { git = "https://github.com/rajasekarv/vega", branch = "master }
```

It is _not recommended_ to use the application for any sort of production code yet as it's under heavy development.

Check [examples](https://github.com/rajasekarv/vega/tree/master/examples) and [tests](https://github.com/rajasekarv/vega/tree/master/tests) in the source code to get a basic idea of how the framework works.

## Executing an application

In order to execute application code some preliminary setup is required. (So far only tested on Linux.)

* Install [Cap'n Proto](https://capnproto.org/install.html). Required for serialization/deserialziation and IPC between executors.
* If you want to execute examples, tests or contribute to development, clone the repository `git clone https://github.com/rajasekarv/vega/`, if you want to use the library in your own application you can just add the depency as indicated in the installation paragraph.
* You need to have [hosts.conf](https://github.com/rajasekarv/vega/blob/master/config_files/hosts.conf) in the format present inside config folder in the home directory of the user deploying executors in any of the machines.
    * In `local` mode this means in your current user home, e.g.:
    > $ cp vega/config_files/hosts.conf $HOME
    * In `distributed` mode the same file is required in each host that may be deploying executors (the ones indicated in the `hosts.conf` file) and the master. E.g.:
    ```doc
    $ ssh remote_user@172.0.0.10 # this machine IP is in hosts.conf
    # create the same hosts.conf file in every machine:
    $ cd ~ && vim hosts.conf ...
    ```
* The environment variable `VEGA_LOCAL_IP` must be set for the user executing application code.
    * In `local` it suffices to set up for the current user:
    > $ export VEGA_LOCAL_IP=0.0.0.0
    * In `distributed` the variable is required, aditionally, to be set up for the users remotely connecting. Depending on the O.S. and ssh defaults this may require some additional configuration. E.g.:
    ```doc
    $ ssh remote_user@172.0.0.10
    $ sudo echo "VEGA_LOCAL_IP=172.0.0.10" >> .ssh/environment
    $ sudo echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config
    $ service ssh restart 
    ```

Now you are ready to execute your application code; if you want to try the provided 
examples just run them. In `local`:
> cargo run --example make_rdd

In `distributed`:
> export VEGA_DEPLOYMENT_MODE=distributed
>
> cargo run --example make_rdd

## Deploying with Docker

There is a docker image and docker-compose script in order to ease up trying testing 
and deploying distributed mode on your local host. In order to use them:

1. Build the examples image under the repository `docker` directory:
> bash docker/build_image.sh

2. When done, you can deploy a testing cluster:
> bash testing_cluster.sh

This will execute all the necessary steeps to to deploy a working network of containers where you can execute the tests. When finished you can attach a shell to the master and run the examples:
```doc
$ docker exec -it docker_vega_master_1 bash
$ ./make_rdd
```

## Setting execution mode

In your application you can set the execution mode (`local` or `distributed`) in one of the following ways:

1. Set it explicitly while creating the context, e.g.:
```doc
    use vega::DeploymentMode;

    let context = Context::with_mode(DeploymentMode::Local)?;
```
2. Set the DEPLOYMENT_MODE environment variable (e.g.: `DEPLOYMENT_MODE=local`).

### Additional notes

* Depending on the source you intend to use you may have to write your own source reading rdd (like manually reading from S3) if it's not yet available.
* Ctrl-C and panic handling are not compeltely done yet, so if there is a problem during runtime, executors won't shut down automatically and you will have to manually kill the processes.
* One of the limitations of current implementation is that the input and return types of all closures and all input to make_rdd should be owned data.
