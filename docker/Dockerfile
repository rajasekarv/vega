## SPDX-License-Identifier: Apache-2.0
# Stage 0
## Set up and compilation stage
FROM ubuntu:18.04 AS building
ENV tempPkgs='\
    build-essential \
    ca-certificates \
    curl \
    libssl-dev \
    pkg-config \
    file \
    capnproto \
    '
RUN set -e; \
    apt-get update -yq; \
    apt-get install -yq --no-install-recommends $tempPkgs; \
    # Install and set up rustup
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-09-11 --no-modify-path; 
WORKDIR /home
ENV PATH="/root/.cargo/bin:$PATH"
COPY . native_spark
RUN set -e; cd native_spark; \
    echo "PATH: ${PATH}"; \
    # Build executables
    cargo build --release --examples; \
    cp config_files/hosts.conf /root/; \
    mkdir /home/release; \
    # Copy all examples binaries
    find ./target/release/examples -exec file {} \; \
    | grep "shared object" \
    | cut -d: -f1 \
    | grep -v .*- \
    | xargs -I{} cp "{}" /home/release

# Stage 1
## Self-contained build with only necessary utils and binaries
FROM ubuntu:18.04
WORKDIR /home/release
COPY --from=building /home/release .
RUN set -e; \
    # Install requirements
    apt-get update -yq; \
    apt-get install -yq locales ssh capnproto; \
    # Locales
    locale-gen en_US.UTF-8; \
    # Cleanup
    apt-get purge -y --auto-remove $tempPkgs; \
    apt-get autoremove -q -y; \
    apt-get clean -yq; \
    rm -rf /var/lib/apt/lists/* 
ENV LANG=en_US.UTF-8  \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8
