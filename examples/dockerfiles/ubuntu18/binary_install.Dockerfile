# This is an *unsupported* and *unofficial* Dockerfile.  The purpose of this Dockerfile is to demonstrate the steps
# required to install the Couchbase Python SDK >= 4.0.2.  No optimization has been done.
#
# The 4.0.2 release of the Couchbase Python SDK provides manylinux wheels.  A Python package wheel provides a pre-built
# binary that enables significantly faster install and users do not need to worry about setting up the appropriate
# build system.
#
# **NOTE:** All versions of the 4.x Python SDK, require OpenSSL >= 1.1.1 and Python >= 3.7
#
# Example usage:
#   build:
#       docker build -t <name of image> -f <path including Dockerfile> <path to Dockerfile directory>
#   run:
#       docker run --rm --name <name of running container> -it <name of image> /bin/bash
#

FROM --platform=linux/amd64 ubuntu:18.04

# can update to a different timezone if desired
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Update the following ARGs to desired specification

# Always required:
#   - Python >= 3.7
#   - OpenSSL >= 1.1.1

# Python must be >= 3.7
ARG PYTHON_VERSION=3.8.10
# NOTE:  the Python version chosen will impact what python executable to use when pip
#           installing packages (see commands at bottom)
ARG PYTHON_EXE=python3.8
# OpenSSL must be >= 1.1.1
ARG OPENSSL_VERSION=1.1.1l
ARG COUCHBASE_PYTHON_SDK=4.0.2

# basic setup
RUN apt-get update && \
    apt-get install -yq git-all python3-dev python3-pip \
    python3-setuptools build-essential libssl-dev make

# OPTIONAL: useful tools
RUN apt-get install -y wget vim zip unzip
# OPTIONAL: more useful tools
# RUN apt-get install -y lsof lshw sysstat net-tools

# OPTIONAL:  update OpenSSL
#   - Ubuntu 18.04 *should* come with a compatible version of OpenSSL (>= v1.1.1)

# RUN apt-get install -y zlib1g-dev build-essential checkinstall
# RUN cd /usr/src && \
#     wget https://www.openssl.org/source/old/1.1.1/openssl-$OPENSSL_VERSION.tar.gz && \
#     tar -xvf openssl-$OPENSSL_VERSION.tar.gz && \
#     mv openssl-$OPENSSL_VERSION openssl && \
#     cd openssl && \
#     ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl shared zlib && \
#     make -j4 && \
#     make install && \
#     echo "/usr/local/openssl/lib" > /etc/ld.so.conf.d/openssl-$OPENSSL_VERSION.conf && \
#     ldconfig -v && \
#     mv /usr/bin/openssl /usr/bin/openssl-backup && \
#     mv /usr/bin/c_rehash /usr/bin/c_rehash-backup

# Install Couchbase Python SDK 4.x
RUN $PYTHON_EXE -m pip install --upgrade pip setuptools wheel
# If installed/updated OpenSSL, might need to set PYCBC_OPENSSL_DIR
RUN $PYTHON_EXE -m pip install couchbase==$COUCHBASE_PYTHON_SDK

# cleanup
RUN apt-get autoremove && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
