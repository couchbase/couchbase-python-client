# A Docker image for the Python performer for the FIT testing framework.
# The build context is the repository root, so build from there with:
# docker build -f tools/fit_performer/Dockerfile .
FROM python:3.14-slim-trixie AS build

WORKDIR /app

COPY ./ ./

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake python3-dev git libssl-dev

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip

RUN if [ ! -f deps/couchbase-cxx-client/CMakeLists.txt ]; then \
        echo "WARNING: deps/couchbase-cxx-client is missing or empty.  Run" \
             "'git submodule update --init --recursive' in the repository root;" \
             "the SDK build below will fail without it."; \
    fi

# CI checks out repo with no tags or commit history history, so unshallow the repo here.
# This is to ensure couchbase_version.py is able to determine the version of the SDK.
RUN if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then \
        echo "Shallow checkout detected, fetching history and tags..."; \
        git fetch --unshallow --tags --no-recurse-submodules origin; \
    else \
        echo "Full checkout detected, no need to fetch history or tags."; \
    fi;

RUN PYCBC_USE_CPM_CACHE=false pip install .

WORKDIR /app/tools/fit_performer

RUN pip install -r requirements.txt
RUN ./bin/generate.sh
RUN pip list

FROM python:3.14-slim-trixie AS runtime

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

WORKDIR /app

COPY tools/fit_performer/ ./tools/fit_performer/
COPY --from=build /app/tools/fit_performer/generated ./tools/fit_performer/generated

ENTRYPOINT ["./tools/fit_performer/bin/entrypoint.sh"]
CMD ["python", "-m", "tools.fit_performer"]
