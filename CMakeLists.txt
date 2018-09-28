cmake_minimum_required(VERSION 3.8)
project(couchbase_python_client_2_3_1)
set(LCB_ROOT ../libcouchbase)
set(LCB_ROOT_SRC ${LCB_ROOT}/src)
set(LCB_ROOT_CONTRIB ${LCB_ROOT}/contrib)
#SET_TARGET_PROPERTIES(couchbase_python_client_2_3_1 PROPERTIES LINKER_LANGUAGE C)
set(PYTHON_INCLUDE_DIR /Users/ellis_breen/root/virtualenvs/3.6/default/include/python3.6m/)
set(PYTHON_INCLUDE ${PYTHON_INCLUDE_DIR})
set(LCB_IO ${LCB_ROOT}/src/lcbio)
set(OT_ROOT thirdparty/opentracing-cpp)
set(LCB_CJSON ${LCB_ROOT}/contrib/cJSON)
set(TP_CJSON thirdparty/cJSON)
set(LCB_TRACING_DIR ${LCB_ROOT}/src/tracing)
set(LCB_MCSERVER_DIR ${LCB_ROOT}/src/mcserver)
set(LCB_BUCKETCONFIG_DIR ${LCB_ROOT}/src/bucketconfig)
set(LCB_OPERATIONS ${LCB_ROOT}/src/operations)
aux_source_directory(${LCB_ROOT}/src/operations LCB_OPS)
aux_source_directory(${LCB_ROOT}/src LCB_CORE)
aux_source_directory(${LCB_ROOT}/src/http HTTP)
aux_source_directory(${LCB_ROOT}/include/memcached/ MCD_INC)
aux_source_directory(${LCB_ROOT}/include/libcouchbase/ LCB_INC)
aux_source_directory(${LCB_ROOT}/contrib/lcb-jsoncpp LCB_JSON)
aux_source_directory(${LCB_CJSON} LCB_CJSON_SRC )
aux_source_directory(${LCB_ROOT}/src/ssl LCB_SSL)
aux_source_directory(${LCB_MCSERVER_DIR} LCB_MCSERVER)
aux_source_directory(${LCB_IO} LCB_IO_A)
aux_source_directory(${LCB_ROOT_SRC}/sasl LCB_SASL)
aux_source_directory(${LCB_ROOT_SRC}/tracing LCB_TRACING_DIR)
aux_source_directory(${LCB_BUCKETCONFIG_DIR} BUCKETCONFIG)
aux_source_directory(LCB_VBUCKET ${LCB_ROOT}/src/vbucket)
aux_source_directory(LCB_SASL ${LCB_ROOT}/contrib/cbsasl)
aux_source_directory(LCB_CLI ${LCB_ROOT}/contrib/cliopts)
aux_source_directory(LCB_ROOT_A ${LCB_ROOT_SRC})

include(ExternalProject)
add_definitions(-DLCB_TRACING=TRUE
        -DPYCBC_TRACING_ENABLE=TRUE
        )

ExternalProject_Add (cJSON
PREFIX ${OT_ROOT}
GIT_REPOSITORY "https://github.com/DaveGamble/cJSON.git"

BINARY_DIR ${TP_CJSON}
SOURCE_DIR ${TP_CJSON}
INSTALL_DIR ${TP_CJSON}/build
)

ExternalProject_Add (opentracing-cpp
        PREFIX ${OT_ROOT}
        GIT_REPOSITORY "https://github.com/griels/opentracing-cpp.git"

        GIT_TAG "07779476cd362629fabce02a6a952d0fc478b5c1"
        BINARY_DIR ${OT_ROOT}
        SOURCE_DIR ${OT_ROOT}
        INSTALL_DIR ${OT_ROOT}/build
        )

include(FindPythonLibs)

include_directories( ${LCB_ROOT_SRC}
        ${LCB_ROOT}/include
        ${MCD_INC}
        ${HTTP}
        ${PYTHON_INCLUDE_DIR}
        ${LCB_JSON}
        ${LCB_SSL}
        ${LCB_MCSERVER}
        ${LCB_MCSERVER_DIR}
        ${LCB_IO}
        ${LCB_VBUCKET}
        ${LCB_SASL}
        ${LCB_CLI}
        ${LCB_ROOT_SRC}
        ${LCB_SASL}
        ${LCB_SASL}/src
        ${LCB_ROOT_CONTRIB}
        ${LCB_TRACING_DIR}
        ${LCB_CJSON}
        ${PYTHON_INCLUDE_DIRS}
        ${LCB_OPS}
        ${LCB_OPERATIONS}
        )

ExternalProject_Add(libcouchbase
        DOWNLOAD_COMMAND ""
        SOURCE_DIR ${LCB_ROOT}
        )
set(CMAKE_CXX_STANDARD 11)
set(EXTRA_SOURCE_DIRS
        ${LCB_ROOT}/src
        ${LCB_ROOT}/include/memcached
        ${LCB_ROOT}/include/libcouchbase
        ${LCB_ROOT}/src/http
        ${LCB_ROOT}/src/ssl
        ${LCB_MCSERVER_DIR}

        ${LCB_ROOT}/src/mcserver
        ${LCB_IO}

        ${LCB_VBUCKET}
        ${LCB_SASL}
        ${LCB_CLI}
        ${LCB_ROOT_SRC}
        ${LCB_ROOT_A}

        ${LCB_BUCKETCONFIG}
        ${LCB_BUCKETCONFIG_DIR}
        ${LCB_OPERATIONS}
        ${LCB_OPS}
        )

set(SOURCE
        ${SOURCE}
        ${LCB_OPERATIONS}

        ${LCB_OPS}
        ${LCB_CORE}
        ${LCB_INC}
        ${MCD_INC}
        ${HTTP}
        ${LCB_SSL}
        ${LCB_ROOT}/src/ssl
        ${LCB_TRACING_DIR}
        ${LCB_JSON}
        ${LCB_MCSERVER}
        ${LCB_IO}
        ${LCB_MCSERVER_DIR}

        ${LCB_VBUCKET}
        ${LCB_SASL}
        ${LCB_CLI}
        ${LCB_ROOT_SRC}
        ${LCB_BUCKETCONFIG}
        ${LCB_BUCKETCONFIG_DIR}
        )
link_directories(${LCB_ROOT}/lib)
link_libraries(${LCB_ROOT}/../lib/
        )
add_executable(couchbase_python_client_2_3_1
        ${LCB_BUCKETCONFIG}
        ${LCB_BUCKETCONFIG_DIR}
        ${LCB_TRACING_DIR}
        ${OT_ROOT}/src
        ${LCB_CORE}
        ${LCB_INC}
        ${MCD_INC}
        ${LCB_JSON}
        ${LCB_SSL}
        ${LCB_CJSON_SRC}
        ${LCB_VBUCKET}
        ${LCB_SASL}
        ${LCB_CLI}
        ${LCB_MCSERVER}
        ${LCB_MCSERVER_DIR}
        ${LCB_JSON}
        ${LCB_OPERATIONS}
        ${LCB_OPS}
        acouchbase/tests/asyncio_tests.py
        acouchbase/tests/fixtures.py
        acouchbase/tests/py34only.py
        acouchbase/tests/py35only.py
        acouchbase/__init__.py
        acouchbase/asyncio_iops.py
        acouchbase/bucket.py
        acouchbase/py34only/iterator.py
        couchbase/asynchronous/__init__.py
        couchbase/asynchronous/bucket.py
        couchbase/asynchronous/events.py
        couchbase/asynchronous/n1ql.py
        couchbase/asynchronous/rowsbase.py
        couchbase/asynchronous/view.py
        couchbase/iops/__init__.py
        couchbase/iops/base.py
        couchbase/iops/select.py
        couchbase/tests/admin/__init__.py
        couchbase/tests/cases/__init__.py
        couchbase/tests/cases/admin_t.py
        couchbase/tests/cases/append_t.py
        couchbase/tests/cases/arithmetic_t.py
        couchbase/tests/cases/badargs_t.py
        couchbase/tests/cases/cbftstrings_t.py
        couchbase/tests/cases/cluster_t.py
        couchbase/tests/cases/connection_t.py
        couchbase/tests/cases/connstr_t.py
        couchbase/tests/cases/datastructures_t.py
        couchbase/tests/cases/delete_t.py
        couchbase/tests/cases/design_t.py
        couchbase/tests/cases/dupkeys_t.py
        couchbase/tests/cases/empty_key_t.py
        couchbase/tests/cases/encodings_t.py
        couchbase/tests/cases/endure_t.py
        couchbase/tests/cases/enh_err_t.py
        couchbase/tests/cases/excextra_t.py
        couchbase/tests/cases/flush_t.py
        couchbase/tests/cases/format_t.py
        couchbase/tests/cases/get_t.py
        couchbase/tests/cases/diag_t.py
        couchbase/tests/cases/iops_t.py
        couchbase/tests/cases/itertypes_t.py
        couchbase/tests/cases/itmops_t.py
        couchbase/tests/cases/ixmgmt_t.py
        couchbase/tests/cases/lock_t.py
        couchbase/tests/cases/lockmode_t.py
        couchbase/tests/cases/misc_t.py
        couchbase/tests/cases/mutationtokens_t.py
        couchbase/tests/cases/n1ql_t.py
        couchbase/tests/cases/n1qlstrings_t.py
        couchbase/tests/cases/observe_t.py
        couchbase/tests/cases/pipeline_t.py
        couchbase/tests/cases/results_t.py
        couchbase/tests/cases/rget_t.py
        couchbase/tests/cases/set_converters_t.py
        couchbase/tests/cases/set_t.py
        couchbase/tests/cases/spatial_t.py
        couchbase/tests/cases/stats_t.py
        couchbase/tests/cases/subdoc_t.py
        couchbase/tests/cases/touch_t.py
        couchbase/tests/cases/transcoder_t.py
        couchbase/tests/cases/verinfo_t.py
        couchbase/tests/cases/view_iterator_t.py
        couchbase/tests/cases/view_t.py
        couchbase/tests/cases/viewstrings_t.py
        couchbase/tests/cases/xattr_t.py
        couchbase/tests/__init__.py
        couchbase/tests/base.py
        couchbase/tests/importer.py
        couchbase/tests/test_sync.py
        couchbase/views/__init__.py
        couchbase/views/iterator.py
        couchbase/views/params.py
        couchbase/__init__.py
        couchbase/_bootstrap.py
        couchbase/_ixmgmt.py
        couchbase/_logutil.py
        couchbase/_pyport.py
        couchbase/_version.py
        couchbase/admin.py
        couchbase/auth_domain.py
        couchbase/bucket.py
        couchbase/bucketmanager.py
        couchbase/analytics.py
        couchbase/cluster.py
        couchbase/connection.py
        couchbase/connstr.py
        couchbase/exceptions.py
        couchbase/experimental.py
        couchbase/fulltext.py
        couchbase/items.py
        couchbase/mockserver.py
        couchbase/mutation_state.py
        couchbase/n1ql.py
        couchbase/priv_constants.py
        couchbase/result.py
        couchbase/subdocument.py
        couchbase/transcoder.py
        couchbase/user_constants.py
        examples/abench.py
        examples/basic.py
        examples/bench.py
        examples/connection-pool.py
        examples/docloader.py
        examples/gbench.py
        examples/iops_demo.py
        examples/item.py
        examples/reversed_keys.py
        examples/search_keywords.py
        examples/twist-sample.py
        examples/txbasic.py
        examples/txbench.py
        examples/txview.py
        gcouchbase/tests/__init__.py
        gcouchbase/tests/test_api.py
        gcouchbase/tests/test_gevent.py
        gcouchbase/__init__.py
        gcouchbase/bucket.py
        gcouchbase/connection.py
        gcouchbase/iops_gevent0x.py
        gcouchbase/iops_gevent10.py
        src/bucket.c
        src/callbacks.c
        src/cntl.c
        src/connevents.c
        src/constants.c
        src/convert.c
        src/crypto.c
        src/counter.c
        src/ctranscoder.c
        src/exceptions.c
        src/ext.c
        src/fts.c
        src/get.c
        src/htresult.c
        src/http.c
        src/iops.c
        src/iops.h
        src/ixmgmt.c
        src/miscops.c
        src/mresdict.h
        src/multiresult.c
        src/n1ql.c
        src/observe.c
        src/opresult.c
        src/oputil.c
        src/oputil.h
        src/pipeline.c
        src/pycbc.h
        src/result.c
        src/store.c
        src/typeutil.c
        src/views.c
        txcouchbase/tests/__init__.py
        txcouchbase/tests/base.py
        txcouchbase/tests/test_n1ql.py
        txcouchbase/tests/test_ops.py
        txcouchbase/tests/test_txconn.py
        txcouchbase/tests/test_views.py
        txcouchbase/__init__.py
        txcouchbase/bucket.py
        txcouchbase/connection.py
        txcouchbase/iops.py
        couchbase_version.py
        setup.py)

