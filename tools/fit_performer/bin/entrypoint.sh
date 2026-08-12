#!/bin/sh
# entrypoint.sh

# Translate LOG_LEVEL → PYCBC_LOG_LEVEL if PYCBC_LOG_LEVEL is not already set
if [ -z "${PYCBC_LOG_LEVEL}" ] && [ -n "${LOG_LEVEL}" ]; then
    export PYCBC_LOG_LEVEL="${LOG_LEVEL}"
fi

exec "$@"
