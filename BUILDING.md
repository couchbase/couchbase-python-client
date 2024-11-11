# Building Couchbase Python Client

>**NOTE:** This is the documentation for the 4.x version of the client. This is mostly compatible with the older 3.x version. Please refer to the *release32* branch for the older 3.x version.

>**NOTE:** We highly recommend using a Python virtual environment (see [venv](https://docs.python.org/3/library/venv.html) for details).

## Prerequisites<a id="prerequisites"></a>
- Supported Python version (see [Python Version Compatibility](https://docs.couchbase.com/python-sdk/current/project-docs/compatibility.html#python-version-compatibility) for details)
- A C++ compiler supporting C++ 17
- CMake (version >= 3.18)
- Git
- Optional: OpenSSL
    - The Couchbase Python Client can statically link against BoringSSL as of v4.1.9.  The examples below are also using BoringSSL.

## Clone the repository
```console
git clone --depth 1 --branch <tag_name> --recurse-submodules https://github.com/couchbase/couchbase-python-client.git
```

If the ```--recurse-submodules``` option was not used when cloning the Python SDK repository, run (after moving into the cloned repository directory) ```git submodule update --init --recursive``` to recursively update and initialize the submodules.

## Set CPM Cache
The C++ core utilizes the CMake Package Manager (CPM) to include depencies.  These can be set to a cache directory and can be used for future builds.  Periodically the dependencies should be updated.  So, in general it is good practice to configure the build environment by setting the CPM cache.

```console
PYCBC_SET_CPM_CACHE=ON PYCBC_USE_OPENSSL=OFF python setup.py configure_ext
```

## Build the SDK
```console
PYCBC_USE_OPENSSL=OFF python setup.py build_ext --inplace
```

## Available Build Options
>Note: Section under construction

## Alternate build from PyPI source distribution

Make sure [minimum requirements](prerequisites) have been installed.

>**NOTE:** After the source distribution has been obtained from PyPI, the build should be able to successfully complete without an internet connection.

### Build with BoringSSL
```console
export CB_VERSION=<desired Python SDK version> SOURCE_DIR=couchbase-python-client
python -m pip download --no-deps --no-binary couchbase --no-cache-dir couchbase==$CB_VERSION
tar -xvf couchbase-$CB_VERSION.tar.gz
mkdir $SOURCE_DIR
mv couchbase-$CB_VERSION/* $SOURCE_DIR
cd $SOURCE_DIR
PYCBC_USE_OPENSSL=OFF python setup.py build_ext --inplace
```

### Build with system OpenSSL
```console
export CB_VERSION=<desired Python SDK version> SOURCE_DIR=couchbase-python-client
python -m pip download --no-deps --no-binary couchbase --no-cache-dir couchbase==$CB_VERSION
tar -xvf couchbase-$CB_VERSION.tar.gz
mkdir $SOURCE_DIR
mv couchbase-$CB_VERSION/* $SOURCE_DIR
cd $SOURCE_DIR
python setup.py build_ext --inplace
```
