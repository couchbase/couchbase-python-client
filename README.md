# Couchbase Python Client
Python client for [Couchbase](https://couchbase.com)
>**NOTE:** This is the documentation for the 3.x version of the client. This is mostly compatible with the older version. Please refer to the *release25* branch for the older version.

# Contents<a id="contents"></a>
- [Prerequisites](#prerequisites)
- [Installing](#installing)
- [Building](#building)
- [Using the SDK](#using-the-sdk)
- [Building Documentation](#building-documentation)
- [Testing](#testing)
- [Contributing](#contributing)
- [Support & Additional Resources](#support-additional-resources)
- [License](#license)
- [Appendix](#appendix)

# Prerequisites<a id="prerequisites"></a>

- [Couchbase Server](http://couchbase.com/download)
- You may need a C compiler and Python development files, unless a
  binary wheel is available for your platform. Currently, wheels are only available on Windows for Python 3.7, 3.8 and 3.9. We will endeavor to add more.
- Git, if a binary wheel is not available.

## Debian and Ubuntu<a id="pre-deb-ubuntu"></a>

First-time setup:
```console
$ sudo apt install git-all python3-dev python3-pip python3-setuptools cmake build-essential
```

For TLS/SSL support:
```console
$ sudo apt install libssl-dev
```

See [Debian and Ubuntu](#install-deb-ubuntu) install section to install SDK.

## RHEL and CentOS<a id="pre-rhel-centos"></a>

First-time setup:
```console
$ sudo yum install git-all gcc gcc-c++ python3-devel python3-pip python3-setuptools cmake
```

>**NOTE:** The minimum version of CMake support is 3.5.1.  Check out the steps [here](https://idroot.us/install-cmake-centos-8/) to update CMake.

For TLS/SSL support:
```console
$ sudo yum install openssl-devel
```

See [RHEL and Centos](#install-rhel-centos) install section to install SDK.

## Mac OS<a id="pre-macos"></a>

It is not recommended to use the vendor-supplied Python that ships with OS X. Best practice is to use a Python virtual environment such as pyenv or venv (after another version of Python that is not vendor-supplied has been installed) to manage multiple versions of Python.

>:exclamation:**IMPORTANT**:exclamation:<br>There can be a problem when using the Python (3.8.2) that ships with Xcode on Catalina.    It is advised to install Python with one of the following:
>- [pyenv](#macos-pyenv)
>- [Homebrew](#macos-homebrew)
>- Install Python via [python.org](https://www.python.org/downloads)

### pyenv<a id="macos-pyenv"></a>

See detailed walk through in [Appendix](#appendix-pyenv).  Also, see pyenv install [docs](https://github.com/pyenv/pyenv#homebrew-on-macos) for further details.

### Homebrew<a id="macos-homebrew"></a>

See Homebrew install [docs](https://brew.sh/) for further details.

Get the latest packages:
```console
$ brew update
```

Install Python:
```console
$ brew install python
```

Update path:

- **zsh:**
    ```console
    $ echo 'export PATH="/usr/local/bin:"$PATH' >> ~/.zshrc
    ```
- **bash:**
    ```console
    $ echo 'export PATH="/usr/local/bin:"$PATH' >> ~/.bash_profile
    ```

For TLS/SSL support:
```console
$ brew install openssl
```

See [Mac OS](#install-macos) install section to install SDK.

## Windows<a id="pre-windows"></a>

Wheels are available on Windows for Python 3.7, 3.8 and 3.9.
>**NOTE:** Python 3.9 wheel was released with v 3.1.2 of the SDK  

Best practice is to use a Python virtual environment such as venv or pyenv (checkout the [pyenv-win](https://github.com/pyenv-win/pyenv-win) project) to manage multiple versions of Python.

If wanting to install from source, see the [Windows building](#building-windows) section for details.

See [Windows install](#install-windows) section to install SDK.

# Installing<a id="installing"></a>
[Back to Contents](#contents)

You can always get the latest supported release version from [pypi](https://pypi.org/project/couchbase/).

>**NOTE:** If you have a recent version of *pip*, you may use the latest development version by issuing the following incantation:
>```console
>pip install git+https://github.com/couchbase/couchbase-python-client.git
>```

>**NOTE:** Currently the Python Client source distribution requires the OpenSSL headers and libraries that the Python client itself was built against to be installed prior to the client itself for TLS support to be provided. Additionally the installer relies on PEP517 which older versions of PIP do not support. If you experience issues installing it is advised to upgrade your PIP/setuptools installation as follows:<br>
>```console
>python3 -m pip install --upgrade pip setuptools wheel
>```


## Debian and Ubuntu<a id="install-deb-ubuntu"></a>

First, make sure the [prerequisites](#pre-deb-ubuntu) have been installed.

Install the SDK:
```console
$ python3 -m pip install couchbase
```

## RHEL and CentOS<a id="install-rhel-centos"></a>

First, make sure the [prerequisites](#pre-rhel-centos) have been installed.

Install the SDK:
```console
$ python3 -m pip install couchbase
```

## Mac OS<a id="install-macos"></a>

First, make sure the [prerequisites](#pre-macos) have been installed.

Install the SDK:
```console
$ python -m pip install couchbase
```


## Windows<a id="install-windows"></a>

First, make sure the [prerequisites](#pre-windows) have been installed.

>**NOTE:** Commands assume user is working within a virtual environment.  For example, the following commands have been executed after downloading and installing Python from [python.org](https://www.python.org/downloads/):<br>
>-```C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python -m venv C:\python\python39```<br>
>-```C:\python\python39\Scripts\activate```

Install the SDK (if using Python 3.7, 3.8 or 3.9):
```console
python -m pip install couchbase
```

### Alternative Installation Methods<a id="install-windows-alt"></a>

In order to successfully install with the following methods, ensure a proper build system is in place (see the [Windows building](#building-windows) section for details).

#### Source Install (i.e. no wheel)

First, ensure all the [requirements](#building-windows) for a build system are met. 

Install the SDK:
```console
python -m pip install couchbase --no-binary couchbase
```

#### Local Install

First, ensure all the [requirements](#building-windows) for a build system are met.

Clone this Python SDK repository:
```console
git clone --depth 1 --branch <tag_name> https://github.com/couchbase/couchbase-python-client.git
```

>Where tag_name is equal to the latest release.<br>
Example: ```git clone --depth 1 --branch 3.1.2 https://github.com/couchbase/couchbase-python-client.git```

Move into the directory created after cloning the Python SDK repository:
```console
cd couchbase-python-client
```

Install the SDK from source:
```console
python -m pip install .
```

## Anaconda/Miniconda<a id="install-anaconda"></a>

To use the SDK within the Anaconda/Miniconda platform, make sure the prerequisites for the desired Operating System are met:
- [Debian and Ubuntu](#pre-deb-ubuntu)
- [RHEL and Centos](#pre-rhel-centos)
- [Mac OS](#pre-macos)
- [Windows](#pre-windows)

In the *Anaconda Prompt*, create a new environment:
```console
(base) C:\Users\user1>conda create -n test_env python=3.9
```

Activate the environment
```console
(base) C:\Users\user1>conda activate test_env
```

Install the SDK:
```console
(test_env) C:\Users\user1>python -m pip install couchbase
```

>**NOTE:** If using Windows, and no wheel is available, see the [Alternative Install Methods Windows](#install-windows-alt) section.  The same process should work within the Anaconda/Miniconda platform.

# Building<a id="building"></a>
[Back to Contents](#contents)

>**NOTE:** This section only applies to building from source.

## Build System Setup
### Linux<a id="building-linux"></a>

Make sure the prerequisites have been installed:
- [Debian and Ubuntu](#pre-deb-ubuntu)
- [RHEL and Centos](#pre-rhel-centos)

### Mac OS<a id="building-macos"></a>
First, make sure the [prerequisites](#pre-macos) have been installed.

Install cmake:
```console
$ brew install cmake
```

Install command line developer tools:
```
$ xcode-select --install
```

>**NOTE:** It is possible that installing or updating to the the latest version of [Xcode](https://developer.apple.com/download) is needed.

If setuptools is not installed:
```console
$ python -m pip install setuptools
```

### Windows<a id="building-windows"></a>
#### Requirements
- Download and install [Git](https://git-scm.com/downloads)
- Download and install [Visual Studio 2019](https://visualstudio.microsoft.com/downloads/)
    + Check *Desktop development with C++* prior to installing
- Download and install [CMake](https://cmake.org/download/) >= v 3.5.1
- Download and install [Python](https://www.python.org/downloads/)

#### VS2019 Notes

If seeing issues when trying to build (steps in [](#)), some things to check/try:
- Try running the build commands within the *Developer Command Prompt for VS2019*
- Make sure *MSBuild* can find the correct *VCTargetsPath*
    + It is possible the *VCTargetsPath* environment variable needs to be set.  The below example is based on a typical path, but the actual setting should match that of your current environment setup.
        * ```set VCTargetsPath=C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\MSBuild\Microsoft\VC\v160```
- Make sure *CMake* is picking up the correct generator
    + It is possible the *CMAKE_GENERATOR* environment variable needs to be set
        * ```set CMAKE_GENERATOR=Visual Studio 16 2019```

#### VS2015 & VS2017 Notes

While it is possible to use VS2015 or VS2017 build tools, it is recommended to use the VS2019 path.  Some things to note if using the VS2015 or VS2017:
- Make sure *CMake* is picking up the correct generator
    + It is possible the *CMAKE_GENERATOR* environment variable needs to be set
        * VS2015 example:  ```set CMAKE_GENERATOR=Visual Studio 14 2015```
- Make sure *MSBuild* can be found
    + It is possible an environment variable needs to be set
- Make sure the correct [Windows SDK](https://developer.microsoft.com/en-us/windows/downloads/sdk-archive/) is installed
- Make sure *MSBuild* can find the correct *VCTargetsPath*
    + It is possible the *VCTargetsPath* environment variable needs to be set.  The below example is based on a typical path, but the actual setting should match that of your current environment setup.
        * ```set VCTargetsPath=C:\Program Files (x86)\MSBuild\Microsoft.Cpp\v4.0\v140```
- MSBuild can run into [issues with MAX_PATH limits](https://github.com/dotnet/msbuild/issues/53#issuecomment-459062618) when using ```python -m pip install couchbase```.  This isn't fixed until MSBuild 16.0 so it is recommended to move to VS2019 (comes with MSBuild 16.0).

## Build the Python SDK
Clone this Python SDK repository:
```console
git clone https://github.com/couchbase/couchbase-python-client.git
```

Move into the directory created after cloning the Python SDK repository:
```console
cd couchbase-python-client
```

The following will compile the module locally:
```console
python setup.py build_ext --inplace
```

If you have a libcouchbase install already (in, for example, /opt/local/libcouchbase), you may build using it by setting PYCBC_BUILD=DISTUTILS and some add extra directives, like so:
```console
$ export PYCBC_BUILD=DISTUTILS
$ python setup.py build_ext --inplace \
    --library-dir /opt/local/libcouchbase/lib \
    --include-dir /opt/local/libcouchbase/include
```

You can also modify the environment ```CFLAGS``` and ```LDFLAGS``` variables.

>:exclamation:**WARNING:** If you do not intend to install this module, ensure you set the ```PYTHONPATH``` environment variable to this directory before running any scripts depending on it. Failing to do so may result in your script running against an older version of this module (if installed), or throwing an exception stating that the ```couchbase``` module could not be found.

## Install
```console
pip install .
```

>:exclamation:**WARNING:** If you are on Linux/Mac OS you may need to remove the build directory: ```rm -rf ./build``` before installing with pip: ```pip3 install .```.

# Using the SDK<a id="using-the-sdk"></a>
[Back to Contents](#contents)

## Connecting<a id="sdk-connecting"></a>

See [official documentation](https://docs.couchbase.com/python-sdk/current/howtos/managing-connections.html) for further details on connecting.

```python
# needed for any cluster connection
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator

# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions

# get a reference to our cluster
cluster = Cluster('couchbase://localhost', ClusterOptions(
  PasswordAuthenticator('Administrator', 'password')))
```

>**NOTE:** Authentication is handled differently depending on what version of Couchbase Server you are using.  In Couchbase Server >= 5.0, Role-Based Access Control (RBAC) provides discrete username and passwords for an application that allow fine-grained control. The authenticator is always required.

## Basic Operations<a id="sdk-basic-ops"></a>

See official documentation for further details on [Working with Data](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html).

Building upon the example code in the [Connecting](#sdk-connecting) section:

```python
# get a reference to our bucket
cb = cluster.bucket('travel-sample')

# get a reference to the default collection
cb_coll = cb.default_collection()

# get a document
result = cb_coll.get('airline_10')
print(result.content_as[dict])

# using SQL++ (a.k.a N1QL)
call_sign = 'CBS'
sql_query = 'SELECT VALUE name FROM `travel-sample` WHERE type = "airline" AND callsign = $1'
row_iter = cluster.query(sql_query, QueryOptions(positional_parameters=[call_sign]))
for row in row_iter: 
    print(row)
```

## Async Operations<a id="sdk-async-ops"></a>
The Python Couchbase SDK supports asynchronous I/O through the use of the asyncio (Python standard library) or the Twisted async framework.

>**NOTE:** Currently, the gcouchbase API is not available in the 3.x version of the Python SDK.

### Asyncio

To use asyncio, import ```acouchbase.cluster``` instead of ```couchbase.cluster```.  The ```acouchbase``` API offers an API similar to the ```couchbase``` API.

```python
from acouchbase.cluster import Cluster, get_event_loop
from couchbase.cluster import ClusterOptions
from couchbase.auth import PasswordAuthenticator


async def write_and_read(key, value):
    cluster = Cluster('couchbase://localhost',
                      ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    cb = cluster.bucket('default')
    await cb.on_connect()
    cb_coll = cb.default_collection()
    await cb_coll.upsert(key, value)
    result = await cb_coll.get(key)
    cluster.disconnect()
    return result

loop = get_event_loop()
rv = loop.run_until_complete(write_and_read('foo', 'bar'))
print(rv.content_as[str])
```
### Twisted

To use with Twisted, import ```txcouchbase.cluster``` instead of ```couchbase.cluster```.  The ```txcouchbase``` API offers an API similar to the ```couchbase``` API.

```python
from twisted.internet import reactor, defer

from txcouchbase.cluster import TxCluster
from couchbase.cluster import ClusterOptions
from couchbase.auth import PasswordAuthenticator


def after_upsert(res, key, d):
    print('Set key.  Result CAS: ', res.cas)
    # trigger get_document callback
    d.callback(key)

def upsert_document(key, doc):
    d = defer.Deferred()
    res = cb.upsert(key, doc)
    res.addCallback(after_upsert, key, d)
    return d

def on_get(res, _type=str):
    print('Got res: \n', res.content_as[_type])
    reactor.stop()

def get_document(key):
    res = cb.get(key)
    res.addCallback(on_get)


# create a cluster object
cluster = TxCluster('couchbase://localhost',
                    ClusterOptions(PasswordAuthenticator('Administrator', 'password')))

# create a bucket object
bucket = cluster.bucket('default')
# create a collection object
cb = bucket.default_collection()

d = upsert_document('testDoc_1', {'id': 1, 'type': 'testDoc', 'info': 'fake document'})
d.addCallback(get_document)

reactor.run()
```
# Building Documentation<a id="building-documentation"></a>
[Back to Contents](#contents)

The documentation is using Sphinx and also needs the numpydoc Sphinx extension. In order for the documentation to build properly, the C extension must have been built, since there are embedded docstrings in there as well.

To build the documentation, go into the docs directory and run:
```console
make html
```
The HTML output can be found in docs/build/html/.

Alternatively, you can also build the documentation (after building the module itself) from the top-level directory:
```console
python setup.py build_sphinx
```

Once built, the docs will be in in build/sphinx/html.

# Testing<a id="testing"></a>
[Back to Contents](#contents)

For running the tests, you need the standard unittest module, shipped with Python. Additionally, the testresources package is required.

To run them, use either py.test, unittest or trial.

The tests need a running Couchbase instance. For this, a tests.ini file must be present, containing various connection parameters. An example of this file may be found in tests.ini.sample. You may copy this file to tests.ini and modify the values as needed.

To run the tests:
```console
nosetests
```

# Contributing<a id="contributing"></a>
[Back to Contents](#contents)

We welcome contributions from the community!  Please see follow the steps outlined [here](https://github.com/couchbase/couchbase-python-client/blob/master/CONTRIBUTING.md) to get started.

# License
[Back to Contents](#contents)

The Couchbase Python SDK is licensed under the Apache License 2.0.

# Support & Additional Resources<a id="support-additional-resources"></a>
[Back to Contents](#contents)

If you found an issue, please file it in our [JIRA](http://couchbase.com/issues/browse/pycbc). You can ask questions in our [forums](https://forums.couchbase.com/).

The [official documentation](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) can be consulted as well for general Couchbase concepts and offers a more didactic approach to using the SDK.

# Appendix<a id="appendix"></a>
[Back to Contents](#contents)

### Mac OS pyenv Install<a id="appendix-pyenv"></a>
See pyenv install [docs](https://github.com/pyenv/pyenv#homebrew-on-macos) for further details.

Get the latest packages:
```console
$ brew update
```

For TLS/SSL support:
```console
$ brew install openssl
```

Install pyenv:
```console
$ brew install pyenv
```

>**NOTE:** It is possible that Xcode might need to be reinstalled.  Try **one** of the following:<br>
>- Use command ```xcode-select --install```
>- Install the latest version of [Xcode](https://developer.apple.com/download)

For *Zsh*, run the following commands to update *.zprofile* and *.zshrc*.  See pyenv install [docs](https://github.com/pyenv/pyenv#homebrew-on-macos) for further details on other shells.

```console
$ echo 'eval "$(pyenv init --path)"' >> ~/.zprofile
```

```console
$ echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

>**NOTE:** You need to restart your login session for changes to take affect.  On MacOS, restarting terminal windows should suffice.

Install Python version:
```console
$ pyenv install 3.9.4
```

Set local shell to installed Python version:
```console
$  pyenv local 3.9.4
```

To use virtualenvwrapper with pyenv, install pyenv-virtualenvwrapper:
```console
$ brew install pyenv-virtualenvwrapper
```

To setup a virtualenvwrapper in your pyenv shell, run either ```pyenv virtualenvwrapper``` or ```pyenv virtualenvwrapper_lazy```

>**NOTE:** If issues with ```pyenv virtualenvwrapper```, using ```python -m pip install virtualenvwrapper``` should accomplish the same goal.

Make a virtualenv:
```console
$ mkvirtualenv python-3.9.4-test
```

Install the SDK:
```console
$ python -m pip install couchbase
```