# Python FIT Performer

A gRPC server ("performer") that enables testing using the FIT testing framework.

## Protocol

The `.proto` files in [`proto/`](proto) define the gRPC contract between FIT and the performer. They are not
authored here — they're mirrored from the `operational/` directory of
[`couchbaselabs/fit-protocol`](https://github.com/couchbaselabs/fit-protocol).

To refresh `proto/` from the latest protocol and regenerate the Python code:

```sh
./bin/update_proto.sh
```

Generated Python/gRPC code lives in `generated/`, which is gitignored and rebuilt on demand rather than committed.
To regenerate it from the `.proto` files currently on disk:

```sh
./bin/generate.sh
```

## Building & Running


> [!NOTE]
> The `couchbase-cxx-client` submodule needs to be checked out first (`git  submodule update --init --recursive`).

### With Docker

A Dockerfile is provided to allow building a Docker image for the performer. Its build context is the repository root, as it requires the SDK source, so build from there, not from `tools/fit_performer/`:

```sh
docker build -t fit-performer -f tools/fit_performer/Dockerfile .
```

The image build installs the SDK, installs the performer's dependencies, generates the protobuf code, and runs
the performer, which listens for gRPC requests on port `8060`.

Run the image, publishing that port:

```sh
docker run --rm -p 8060:8060 fit-performer
```

### Without Docker

First ensure that the C++ extension has been built (see [BUILDING.md](../../BUILDING.md)).

From the repository root:

```sh
cd tools/fit_performer
pip install -r requirements.txt
./bin/generate.sh
cd ../..
python -m tools.fit_performer
```
