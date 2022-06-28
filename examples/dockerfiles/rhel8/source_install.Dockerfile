# This is an *unsupported* and *unofficial* Dockerfile.  The purpose of this Dockerfile is to demonstrate the steps
# required to have a working build system to build/install the Couchbase Python 4.x SDK.  No optimization has been
# done.
#
# Build System checklist:
#   - Compiler that supports C++ 17
#   - CMake >= 3.18
#   - OpenSSL >= 1.1.1
#   - Python >= 3.7
#
# The 4.0.2 release of the Couchbase Python SDK provides manylinux wheels.  A Python package wheel provides a pre-built
# binary that enables significantly faster install and users do not need to worry about setting up the appropriate
# build system (i.e. no need to install/update compiler and/or CMake).
#
# **NOTE:** All versions of the 4.x Python SDK, require OpenSSL >= 1.1.1 and Python >= 3.7
#
# Example usage:
#   build:
#       docker build -t <name of image> -f <path including Dockerfile> <path to Dockerfile directory>
#   run:
#       docker run --rm --name <name of running container> -it <name of image> /bin/bash
#

FROM --platform=linux/amd64 registry.access.redhat.com/ubi8/ubi:latest

# Update to match RH subscription
ARG RH_USER=username
ARG RH_PW='password'

# Update the following ARGs to desired specification

# CMake must be >= 3.18
ARG CMAKE_VERSION=3.19.8
# Python must be >= 3.7
ARG PYTHON_VERSION=3.8.10
# NOTE:  the Python version chosen will impact what python executable to use when pip
#           installing packages (see commands at bottom)
ARG PYTHON_EXE=python3.8
# OpenSSL must be >= 1.1.1
ARG OPENSSL_VERSION=1.1.1l
ARG COUCHBASE_PYTHON_SDK=4.0.2

RUN subscription-manager register --username=$RH_USER --password=$RH_PW --auto-attach

# basic setup
RUN yum install -y python3-devel python3-pip python3-setuptools gcc gcc-c++ openssl openssl-devel make cmake

# OPTIONAL: useful tools
RUN yum install -y wget vim zip unzip
# OPTIONAL: more useful tools
# RUN yum install -y lsof lshw sysstat net-tools tar

# OPTIONAL: install/update CMake
#   - RHEL8 *should* offer a compatible version of CMake (>= v3.18)

# RUN cd /tmp && \
#     curl -L -o cmake-$CMAKE_VERSION.tar.gz https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz && \
#     tar xf cmake-$CMAKE_VERSION.tar.gz && \
#     cd cmake-$CMAKE_VERSION && \
#     ./bootstrap && \
#     make -j4 && \
#     make install

# OPTIONAL:  update OpenSSL
#   - RHEL8 *should* come with a compatible version of OpenSSL (>= v1.1.1)

# RUN yum install -y pcre-devel zlib-devel gd-devel perl-ExtUtils-Embed libxslt-devel perl-Test-Simple
# RUN cd /usr/src && \
#     curl -L -o openssl-$OPENSSL_VERSION.tar.gz https://www.openssl.org/source/old/1.1.1/openssl-$OPENSSL_VERSION.tar.gz && \
#     tar -xvf openssl-$OPENSSL_VERSION.tar.gz && \
#     mv openssl-$OPENSSL_VERSION openssl && \
#     cd openssl && \
#     ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl --libdir=/lib64 shared zlib-dynamic && \
#     make -j4 && \
#     make install && \
#     mv /usr/bin/openssl /usr/bin/openssl-backup && \
#     ln -s /usr/local/openssl/bin/openssl /usr/bin/openssl

# install new Python version
RUN yum install -y libffi-devel
RUN cd /tmp && \
    curl -L -o Python-$PYTHON_VERSION.tgz https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    tar -xf Python-$PYTHON_VERSION.tgz && \
    cd Python-$PYTHON_VERSION && \
    ./configure --enable-optimizations && \
    make altinstall

# Install Couchbase Python SDK 4.x
# To do a source install:
#   - make sure the build system has been setup appropriately
#   - use the --no-binary option to force an install from source
#   - need to dynamically link against stdc++ static libs on RHEL8, so set COUCHBASE_CXX_CLIENT_STATIC_STDLIB to OFF
RUN COUCHBASE_CXX_CLIENT_STATIC_STDLIB=OFF $PYTHON_EXE -m pip install couchbase==$COUCHBASE_PYTHON_SDK --no-binary couchbase

RUN subscription-manager unregister
