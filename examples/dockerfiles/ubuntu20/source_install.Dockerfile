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

FROM --platform=linux/amd64 ubuntu:20.04

# can update to a different timezone if desired
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# basic setup
RUN apt-get update && \
    apt-get install -yq git-all python3-dev python3-pip \
    python3-setuptools build-essential libssl-dev make zlib1g-dev

# OPTIONAL: useful tools
RUN apt-get install -y wget vim zip unzip
# OPTIONAL: more useful tools
# RUN apt-get install -y lsof lshw sysstat net-tools

# Install Couchbase Python SDK 4.x
ARG COUCHBASE_PYTHON_SDK=4.0.2
RUN python3 -m pip install --upgrade pip setuptools wheel
# To do a source install:
#   - make sure the build system has been setup appropriately
#   - use the --no-binary option to force an install from source
RUN python3 -m pip install couchbase==$COUCHBASE_PYTHON_SDK --no-binary couchbase

# cleanup
RUN apt-get autoremove && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
