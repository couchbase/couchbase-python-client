# python_bindings

POC for python bindings using couchbase-cxx-client


# Contents<a id="contents"></a>
- [Prerequisites](#prerequisites)
- [Contributing](#contributing)
- [Testing](#testing)

# Prerequisites<a id="prerequisites"></a>

NOTE: you must install python (if using pyenv) specifying enable-shared,
like so:

```
PYTHON_CONFIGURE_OPTS="enable-shared" pyenv install 3.8.9
```

That's a problem that the existing python client doesn't have, unsure
how to deal with it yet.

To get openssl to be found by cmake on macos, where openssl was
installed via homebrew:

```
brew info openssl
```

This will show you how to get it seen by pkg-config. To check that it
worked, do this:

```
pkg-config --modversion openssl
```

# Contributing<a id="contributing"></a>

The SDK uses pre-commit in order to handle linting, formattinga and verifying the code base.
pre-commit can be installed either by install the dev-requirements:

```
python3 -m pip install -r dev-requirements.txt
```

Or by installing pre-commit separately
```
python3 -m pip install pre-commit
```

To run pre-commit, use the following:
```
pre-commit run --all-files
```

To run hooks separately, use the following.

## trailing whitespace
```
pre-commit run trailing-whitespace --all-files
```

## flake8
```
pre-commit run flake8 --all-files
```

## autopep8
```
pre-commit run autopep8 --all-files
```

## isort
```
pre-commit run isort --all-files
```

## bandit
```
pre-commit run bandit --all-files
```

## clang-format
```
pre-commit run clang-format --all-files
```
