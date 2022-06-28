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

FROM --platform=linux/amd64 debian:stretch

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

# basic setup
RUN apt-get update && \
    apt-get install -yq git-all python3-dev python3-pip \
    python3-setuptools build-essential libssl-dev make zlib1g-dev

# OPTIONAL: useful tools
RUN apt-get install -y wget vim zip unzip

# update GCC, use gcc >= 9 for c++ 17
ARG ARCH=x86_64
ARG CORES=4
ARG GCC_VERSION=9.3.0
ARG GCC_LIBS=lib64
RUN cd /usr/src && \
    wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz && \
    tar -xvf gcc-$GCC_VERSION.tar.gz && \
    cd /usr/src/gcc-$GCC_VERSION && \
    sed -i 's\ftp://gcc.gnu.org\https://gcc.gnu.org\g' contrib/download_prerequisites && \
    contrib/download_prerequisites && \
    mkdir build && \
    cd build && \
    ../configure -v \
        --build=$ARCH-linux-gnu \
        --host=$ARCH-linux-gnu \
        --target=$ARCH-linux-gnu \
        --prefix=/usr/local/gcc-$GCC_VERSION \
        --enable-checking=release \
        --enable-languages=c,c++,fortran \
        --disable-multilib \
        --program-suffix=-$GCC_VERSION && \
    make -j$CORES && \
    make install-strip && \
    cd /usr/src && \
    rm -rf /usr/src/gcc-$GCC_VERSION /usr/src/gcc.tar.gz

ENV CC=/usr/local/gcc-$GCC_VERSION/bin/gcc-$GCC_VERSION \
    CXX=/usr/local/gcc-$GCC_VERSION/bin/g++-$GCC_VERSION \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/gcc-$GCC_VERSION/$GCC_LIBS

# install/update CMake
RUN cd /usr/src && \
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz && \
    tar -xvf cmake-$CMAKE_VERSION.tar.gz && cd cmake-$CMAKE_VERSION && \
    ./bootstrap && \
    make -j$CORES && make install && \
    cd /usr/src && \
    rm -rf /usr/src/cmake-$CMAKE_VERSION /usr/src/cmake-$CMAKE_VERSION.tar.gz

# update OpenSSL to $OPENSSL_VERSION
RUN apt-get install -y build-essential checkinstall

RUN cd /usr/src && \
    wget https://www.openssl.org/source/old/1.1.1/openssl-$OPENSSL_VERSION.tar.gz && \
    tar -xvf openssl-$OPENSSL_VERSION.tar.gz && \
    mv openssl-$OPENSSL_VERSION openssl && \
    cd openssl && \
    ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl shared zlib && \
    make -j$CORES && \
    make install && \
    echo "/usr/local/openssl/lib" > /etc/ld.so.conf.d/openssl-$OPENSSL_VERSION.conf && \
    ldconfig -v && \
    mv /usr/bin/openssl /usr/bin/openssl-backup && \
    mv /usr/bin/c_rehash /usr/bin/c_rehash-backup && \
    cd /usr/src && \
    rm -rf /usr/src/openssl-$OPENSSL_VERSION /usr/src/openssl-$OPENSSL_VERSION.tar.gz

# install new Python version
RUN apt-get install -y libffi-dev
RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    tar -xf Python-$PYTHON_VERSION.tgz && \
    cd Python-$PYTHON_VERSION && \
    ./configure --enable-optimizations && \
    make altinstall && \
    cd /usr/src && \
    rm -rf /usr/src/Python-$PYTHON_VERSION /usr/src/Python-$PYTHON_VERSION.tgz

# Install Couchbase Python SDK 4.x
RUN $PYTHON_EXE -m pip install --upgrade pip setuptools wheel
# To do a source install:
#   - make sure the build system has been setup appropriately
#   - let the build system know where OpenSSL is installed by setting PYCBC_OPENSSL_DIR
#   - use the --no-binary option to force an install from source
RUN PYCBC_OPENSSL_DIR=/usr/local/openssl $PYTHON_EXE -m pip install couchbase==$COUCHBASE_PYTHON_SDK --no-binary couchbase

# cleanup
RUN apt-get autoremove && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
