# Couchbase Python Client
Python client for [Couchbase](https://couchbase.com)
>**NOTE:** This is the documentation for the 4.x version of the client. This is mostly compatible with the older 3.x version. Please refer to the *3.2.7* tag for the older 3.x version.


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
- You may need a C++ compiler supporting C++ 17 and Python development files, unless a
  binary wheel is available for your platform. With the 4.0.2 release, wheels are available on Windows, MacOS and Linux (via manylinux) for Python 3.7 - 3.10.
- CMake (version >= 3.18), unless a binary wheel is available for your platform.
- Git, unless a binary wheel is available for your platform.
- OpenSSL is now required for the 4.x Python SDK.
- If using the Twisted Framework and the txcouchbase API, Twisted >= 21.7.0 is required.

## Debian and Ubuntu<a id="pre-deb-ubuntu"></a>

First-time setup:
```console
$ sudo apt install git-all python3-dev python3-pip python3-setuptools cmake build-essential libssl-dev
```

>**NOTE:** We have provided *Dockerfiles* to demonstrate steps to achieve a working setup for various linux platforms. See the [dockerfiles folder](https://github.com/couchbase/couchbase-python-client/tree/master/examples/dockerfiles) in the Python SDK examples folder for details.

See [Debian and Ubuntu](#install-deb-ubuntu) install section to install SDK.

## RHEL and CentOS<a id="pre-rhel-centos"></a>

First-time setup:
```console
$ sudo yum install git-all gcc gcc-c++ python3-devel python3-pip python3-setuptools cmake openssl-devel
```

>:exclamation:**IMPORTANT**:exclamation:<br>Some of the defaults for older operating systems like Centos/RHEL 7 and 8 have defaults to do not meet the 4.x Python SDK [minimum requirements](prerequisites). Be sure to update to the minimum requirements prior to installing the SDK.  Most notably be sure to check the following:
>- The default Python version might be less than 3.7.  If so, the Python version _will_ need to be udpated.
>- The default OpenSSL version might be less than 1.1.1.  If so, the OpenSSL version _will_ need to be updated.
>- The gcc version must provide C++17 support.  If the installed gcc version does not support C++17, gcc _will_ need to be updated.
>- The installed CMake version might be less than 3.17.  If so, the CMake version _will_ need to be updated.  Check out the steps [here](https://idroot.us/install-cmake-centos-8/) to update CMake.

>**NOTE:** We have provided *Dockerfiles* to demonstrate steps to achieve a working setup for various linux platforms. See the [dockerfiles folder](https://github.com/couchbase/couchbase-python-client/tree/master/examples/dockerfiles) in the Python SDK examples folder for details.

See [RHEL and Centos](#install-rhel-centos) install section to install SDK.

## Mac OS<a id="pre-macos"></a>

It is not recommended to use the vendor-supplied Python that ships with OS X. Best practice is to use a Python virtual environment such as pyenv or venv (after another version of Python that is not vendor-supplied has been installed) to manage multiple versions of Python.

>:exclamation:**IMPORTANT**:exclamation:<br>There can be a problem when using the Python (3.8.2) that ships with Xcode on Catalina.    It is advised to install Python with one of the following:
>- [pyenv](#macos-pyenv)
>- [Homebrew](#macos-homebrew)
>- Install Python via [python.org](https://www.python.org/downloads)

### pyenv<a id="macos-pyenv"></a>

See detailed walk through in [Appendix](#appendix-pyenv).  Also, see pyenv install [docs](https://github.com/pyenv/pyenv#homebrew-on-macos) for further details.

>**NOTE:** If using pyenv, make sure the python interpreter is the pyenv default, or a virtual environment has been activiated.  Otherwise cmake might not be able to find the correct version of Python3 to use when building.

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

Install OpenSSL:
```console
$ brew install openssl@1.1
```

To get OpenSSL to be found by cmake on macos, find where openssl was
installed via homebrew:

```
brew info openssl@1.1
```

This will show you how to get it seen by pkg-config. To check that it
worked, do this:

```
pkg-config --modversion openssl
```

See [Mac OS](#install-macos) install section to install SDK.

## Windows<a id="pre-windows"></a>

Wheels are available on Windows for Python 3.7, 3.8, 3.9 and 3.10.

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

>**NOTE:** The Python Client installer relies on PEP517 which older versions of PIP do not support. If you experience issues installing it is advised to upgrade your PIP/setuptools installation as follows:<br>
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

Install the SDK (if using Python 3.7, 3.8, 3.9 or 3.10):
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
git clone --depth 1 --branch <tag_name> --recurse-submodules https://github.com/couchbase/couchbase-python-client.git
```

>Where tag_name is equal to the latest release.<br>
Example: ```git clone --depth 1 --branch 4.0.0 --recurse-submodules https://github.com/couchbase/couchbase-python-client.git```

Move into the directory created after cloning the Python SDK repository:
```console
cd couchbase-python-client
```

>**NOTE:** If the ```--recurse-submodules``` option was not used when cloning the Python SDK repository, run (after moving into the cloned repository directory) ```git submodule update --init --recursive``` to recursively update and initialize the submodules.

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
- Download and install [CMake](https://cmake.org/download/) >= v 3.18
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

## Build the Python SDK
Clone this Python SDK repository:
```console
git clone --depth 1 --recurse-submodules https://github.com/couchbase/couchbase-python-client.git
```

>**NOTE:** If the ```--recurse-submodules``` option was not used when cloning the Python SDK repository, run (after moving into the cloned repository directory) ```git submodule update --init --recursive``` to recursively update and initialize the submodules.

Move into the directory created after cloning the Python SDK repository:
```console
cd couchbase-python-client
```

The following will compile the module locally:
```console
python setup.py build_ext --inplace
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
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator

# options for a cluster and SQL++ (N1QL) queries
from couchbase.options import ClusterOptions, QueryOptions

# get a reference to our cluster
cluster = Cluster.connect('couchbase://localhost', ClusterOptions(
  PasswordAuthenticator('Administrator', 'password')))
```

>**NOTE:** The authenticator is always required.

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
query_res = cluster.query(sql_query, QueryOptions(positional_parameters=[call_sign]))
for row in query_res:
    print(row)
```

## Async Operations<a id="sdk-async-ops"></a>
The Python Couchbase SDK supports asynchronous I/O through the use of the asyncio (Python standard library) or the Twisted async framework.

### Asyncio

To use asyncio, import ```acouchbase.cluster``` instead of ```couchbase.cluster```.  The ```acouchbase``` API offers an API similar to the ```couchbase``` API.

```python
from acouchbase.cluster import Cluster, get_event_loop
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator


async def write_and_read(key, value):
    cluster = await Cluster.connect('couchbase://localhost',
                      ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    cb = cluster.bucket('default')
    await cb.on_connect()
    cb_coll = cb.default_collection()
    await cb_coll.upsert(key, value)
    result = await cb_coll.get(key)
    return result

loop = get_event_loop()
rv = loop.run_until_complete(write_and_read('foo', 'bar'))
print(rv.content_as[str])
```
### Twisted

To use with Twisted, import ```txcouchbase.cluster``` instead of ```couchbase.cluster```.  The ```txcouchbase``` API offers an API similar to the ```couchbase``` API.

>**NOTE:** The minimum required Twisted version is 21.7.0.

>:exclamation:**WARNING:** The 4.x SDK introduced a breaking change where the txcouchbase package must be imported _prior_ to importing the reactor (see example below).  This is so that the asyncio reactor can be installed.


```python
# IMPORTANT -- the txcouchbase import must occur PRIOR to importing the reactor
import txcouchbase
from twisted.internet import reactor, defer

from txcouchbase.cluster import TxCluster
from couchbase.options import ClusterOptions
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

The documentation is using Sphinx and a number of extensions.  To build the documentation be sure to
``pip install`` the sphinx_requirements.txt.

```console
python3 -m pip install -r sphinx_requirements.txt
```

To build the documentation, go into the docs directory and run:
```console
make html
```
The HTML output can be found in docs/build/html/.

Alternatively, you can also build the documentation from the top-level directory:
```console
python setup.py build_sphinx
```

Once built, the docs will be in in build/sphinx/html.  You can open the index.html file with the following command:
```console
open docs/build/sphinx/html/index.html
```

# Testing<a id="testing"></a>
[Back to Contents](#contents)

For running the tests, be sure to ``pip install`` the dev_requirements.txt.  The Couchbase Python SDK uses pytest for the test suite.

```console
python3 -m pip install -r dev_requirements.txt
```

The tests need a running Couchbase instance. For this, a test_config.ini file must be present, containing various connection parameters. The default test_config.ini file may be found in the tests directory. You may modify the values of the test_config.ini file as needed.

To run the tests for the blocking API (i.e. couchbase API):
```console
python -m pytest -m pycbc_couchbase -p no:asyncio -v -p no:warnings
```

To run the tests for the asyncio API (i.e. acouchbase API):
```console
python -m pytest -m pycbc_acouchbase --asyncio-mode=strict -v -p no:warnings
```

# Contributing<a id="contributing"></a>
[Back to Contents](#contents)

We welcome contributions from the community!  Please see follow the steps outlined [here](https://github.com/couchbase/couchbase-python-client/blob/master/CONTRIBUTING.md) to get started.

The Python SDK uses pre-commit in order to handle linting, formatting and verifying the code base.
pre-commit can be installed either by installing the development requirements:

```
python3 -m pip install -r dev_requirements.txt
```

Or by installing pre-commit separately
```
python3 -m pip install pre-commit
```

To run pre-commit, use the following:
```
pre-commit run --all-files
```

# License
[Back to Contents](#contents)

The Couchbase Python SDK is licensed under the Apache License 2.0.

See [LICENSE](https://github.com/couchbase/couchbase-python-client/blob/master/LICENSE) for further details.

# Support & Additional Resources<a id="support-additional-resources"></a>
[Back to Contents](#contents)

If you found an issue, please file it in our [JIRA](https://issues.couchbase.com/projects/PYCBC/issues/).

The Couchbase Discord server is a place where you can collaborate about all things Couchbase. Connect with others from the community, learn tips and tricks, and ask questions.  [Join Discord and contribute](https://discord.com/invite/sQ5qbPZuTh).

You can ask questions in our [forums](https://forums.couchbase.com/).

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
$ brew install openssl@1.1
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
$ pyenv install 3.9.7
```

Set local shell to installed Python version:
```console
$  pyenv local 3.9.7
```

To use virtualenvwrapper with pyenv, install pyenv-virtualenvwrapper:
```console
$ brew install pyenv-virtualenvwrapper
```

To setup a virtualenvwrapper in your pyenv shell, run either ```pyenv virtualenvwrapper``` or ```pyenv virtualenvwrapper_lazy```

>**NOTE:** If issues with ```pyenv virtualenvwrapper```, using ```python -m pip install virtualenvwrapper``` should accomplish the same goal.

Make a virtualenv:
```console
$ mkvirtualenv python-3.9.7-test
```

Install the SDK:
```console
$ python -m pip install couchbase
```

### Run individual pre-commit commands<a id="appendix-precommit"></a>
To run pre-commit hooks separately, use the following.

#### autopep8
```
pre-commit run autopep8 --all-files
```

#### bandit
```
pre-commit run bandit --all-files
```

#### clang-format
```
pre-commit run clang-format --all-files
```

#### flake8
```
pre-commit run flake8 --all-files
```

#### isort
```
pre-commit run isort --all-files
```

#### trailing whitespace
```
pre-commit run trailing-whitespace --all-files
```
