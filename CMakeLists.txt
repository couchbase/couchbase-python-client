cmake_minimum_required(VERSION 3.8)
project(couchbase_python_client_2_3_1)
set(THIRDPARTY_LCB_ROOT libcouchbase-prefix)
set(LCBCXX_ROOT libcouchbase-cxx-prefix/src/libcouchbase-cxx)
set(LCB_ROOT ${THIRDPARTY_LCB_ROOT}/src/libcouchbase)
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
set(PYCBC_CMAKE_CPYTHON_WRAPPER 1)
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
include(FindPythonLibs)

cmake_minimum_required(VERSION 2.8)
#project(myproject CXX)

# Download automatically, you can also just copy the conan.cmake file
if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
   message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
   file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/master/conan.cmake"
                  "${CMAKE_BINARY_DIR}/conan.cmake")
endif()

include(${CMAKE_BINARY_DIR}/conan.cmake)

#conan_cmake_run(REQUIRES OpenSSL/1.0.2n@conan/stable
#                BASIC_SETUP
#                BUILD missing)

include(ExternalProject)
add_definitions(-DLCB_TRACING=TRUE
        -DPYCBC_TRACING_ENABLE=TRUE
        )

ExternalProject_Add(cJSON
        PREFIX ${OT_ROOT}
        GIT_REPOSITORY "https://github.com/DaveGamble/cJSON.git"

        BINARY_DIR ${TP_CJSON}
        SOURCE_DIR ${TP_CJSON}
        INSTALL_DIR ${TP_CJSON}/build
        )
#ExternalProject_Add(libcouchbase-cxx
#        GIT_REPOSITORY https://github.com/couchbaselabs/libcouchbase-cxx.git
#        GIT_TAG master
#        )
set(LCB_CMAKE_ARGS -DLCB_NO_TESTS=1)
if (WIN32)
    ExternalProject_Add(libcouchbase
            #PREFIX thirdparty/libcouchbase
            GIT_REPOSITORY git://github.com/couchbase/libcouchbase.git
            GIT_TAG ${_git_rev}

            CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory <SOURCE_DIR>/build
            COMMAND cd <SOURCE_DIR>/build && cmake -DLCB_NO_PLUGINS=1 .. -G "Visual Studio 14 2015 Win64" ${LCB_CMAKE_ARGS}

            BUILD_COMMAND cd <SOURCE_DIR>/build && cmake --build . --config Release ${LCB_CMAKE_ARGS}

            INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/install
            INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/include <INSTALL_DIR>/include
            COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/build/generated <INSTALL_DIR>/include
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/archive/
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/Release
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/build/bin/Release/libcouchbase.dll <INSTALL_DIR>/bin/Release/
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/archive/Release
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/build/lib/Release/libcouchbase.lib <INSTALL_DIR>/archive/Release/
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/build/lib/Release/libcouchbase.exp <INSTALL_DIR>/archive/Release/

            COMMAND ${CMAKE_COMMAND} -E make_directory <SOURCE_DIR>/dbgbuild
            COMMAND cd <SOURCE_DIR>/dbgbuild && cmake .. -DLCB_NO_PLUGINS=1 ${LCB_CMAKE_ARGS}

            COMMAND cd <SOURCE_DIR>/dbgbuild && cmake --build . --config Debug ${LCB_CMAKE_ARGS}

            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/Debug
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/dbgbuild/bin/Debug/libcouchbase_d.dll <INSTALL_DIR>/bin/Debug/
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/archive/Debug
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/dbgbuild/lib/Debug/libcouchbase_d.lib <INSTALL_DIR>/archive/Debug/
            COMMAND ${CMAKE_COMMAND} -E copy <SOURCE_DIR>/dbgbuild/lib/Debug/libcouchbase_d.exp <INSTALL_DIR>/archive/Debug/


            #COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists_package.txt <INSTALL_DIR>/CMakeLists.txt
            )


else (WIN32)
    if (APPLE)
        SET(LIBCOUCHBASE_LIBRARY_FILE "<SOURCE_DIR>/build/lib/libcouchbase.dylib")
        SET(LIBCOUCHBASE_DBG_LIBRARY_FILE "<SOURCE_DIR>/dbgbuild/lib/libcouchbase.dylib")
    else (APPLE)
        SET(LIBCOUCHBASE_LIBRARY_FILE "<SOURCE_DIR>/build/lib/libcouchbase.so")
        SET(LIBCOUCHBASE_DBG_LIBRARY_FILE "<SOURCE_DIR>/dbgbuild/lib/libcouchbase.so")
    endif (APPLE)


    SET(_parallelism 4)
    ExternalProject_Add(libcouchbase
            #PREFIX thirdparty/libcouchbase
            GIT_REPOSITORY git://github.com/couchbase/libcouchbase.git
            GIT_TAG ${_git_rev}

            CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory <SOURCE_DIR>/build
            COMMAND ${CMAKE_COMMAND} -E make_directory <SOURCE_DIR>/dbgbuild
            COMMAND cd <SOURCE_DIR>/build && cmake .. -DLCB_NO_PLUGINS=1 -DLCB_BUILD_DTRACE=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>/Release ${LCB_CMAKE_ARGS}
            COMMAND cd <SOURCE_DIR>/dbgbuild && cmake .. -DLCB_NO_PLUGINS=1 -DLCB_BUILD_DTRACE=OFF -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>/Debug ${LCB_CMAKE_ARGS}

            BUILD_IN_SOURCE 1
            BUILD_COMMAND $(MAKE) -j${_parallelism} all install -C build
            COMMAND $(MAKE) -j${_parallelism} all install -C dbgbuild

            INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/install
            INSTALL_COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/Release/include <INSTALL_DIR>/include
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib
            COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/Release/lib <INSTALL_DIR>/lib/Release
            COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/Debug/lib <INSTALL_DIR>/lib/Debug
            COMMAND ${CMAKE_COMMAND} -E remove_directory <INSTALL_DIR>/Debug
            COMMAND ${CMAKE_COMMAND} -E remove_directory <INSTALL_DIR>/Release
            #COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists_package.txt <INSTALL_DIR>/CMakeLists.txt
            )

    # OS X-only: Custom post-build step to set the shared library install name.
    if (APPLE)
        ExternalProject_Add_Step(libcouchbase install_name
                COMMAND install_name_tool -id @rpath/libcouchbase.dylib ${LIBCOUCHBASE_LIBRARY_FILE}
                COMMAND install_name_tool -id @rpath/libcouchbase.dylib ${LIBCOUCHBASE_DBG_LIBRARY_FILE}
                DEPENDEES build
                DEPENDERS install
                )
    endif (APPLE)
endif (WIN32)
#add_subdirectory(${THIRDPARTY_LCB_ROOT}/src/libcouchbase)
ExternalProject_Get_Property(libcouchbase install_dir)
ExternalProject_Get_Property(libcouchbase source_dir)
#find_package(Cppyy REQUIRED)
#cppyy_add_bindings(
#        "Stuff" "0" "Enrico" "enrico.guiraud@cern.ch"
#        LANGUAGE_STANDARD "11"
#H_FILES "couchbase.h")

include_directories(${install_dir}/include/libcouchbase
        ${source_dir} ${source_dir}/src/*
        ${source_dir}/src/views
        install/include/libcouchbase
        ${LCBCXX_ROOT}/include
        ${LCBCXX_ROOT}/include/libcouchbase
        ${LCBCXX_ROOT}/include/libcouchbase/couchbase++
        ${LCB_ROOT}/src/views
        ${THIRDPARTY_LCB_ROOT}/src/libcouchbase/include
        ${THIRDPARTY_LCB_ROOT}/src/libcouchbase
        ${source_dir}
        ${PYTHON_INCLUDE_DIRS}
        ${PYTHON_INCLUDE_DIR}
        )
link_libraries(${PYTHON_LIBRARIES} libcouchbase.dylib)
#add_subdirectory(${source_dir})
#add_library(libcouchbase STATIC IMPORTED)
add_dependencies(libcouchbase project_libcouchbase)

add_library(couchbase_python_client_2_3_1 SHARED
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
        setup.py

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
        )
