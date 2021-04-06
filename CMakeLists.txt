cmake_minimum_required(VERSION 3.1)
project(couchbase_python_client)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(THIRDPARTY_LCB_ROOT libcouchbase_src-prefix)
set(LCBCXX_ROOT libcouchbase-cxx-prefix/src/libcouchbase-cxx)
set(LCB_ROOT ${THIRDPARTY_LCB_ROOT}/src/libcouchbase_src)
set(PYCBC_CMAKE_CPYTHON_WRAPPER 1)
cmake_policy(SET CMP0054 NEW)
include(cmake/json-cmake/JSONParser.cmake)
include(FindPythonInterp)
include(ExternalProject)

if(HYBRID_BUILD)
    set(PYCBC_C_MOD_NAME "_dummy")
    set(PYCBC_C_MOD_SUFFIX ".so")
else()
    include(FindPythonLibs)

    if (PYTHON_LIBDIR)
        set(PYTHON_LIBDIR_HINT HINTS ${PYTHON_LIBDIR})
    else(PYTHON_LIBDIR)
        set(PYTHON_LIBDIR_HINT "")
    endif()

    if (PYTHON_VERSION_EXACT)
        find_package(PythonInterp ${PYTHON_VERSION_EXACT} REQUIRED)
        find_package(PythonLibs ${PYTHON_VERSION_EXACT} REQUIRED)
    else(PYTHON_VERSION_EXACT)
        find_package(PythonInterp REQUIRED)
        find_package(PythonLibs ${PYTHON_LIBDIR_HINT} REQUIRED)
    endif()

    if(WIN32)
        set(PYCBC_C_MOD_NAME "_libcouchbase.${PYTHONLIBS_VERSION_STRING}")
        set(PYCBC_C_MOD_SUFFIX ".pyd")
    else()
        set(PYCBC_C_MOD_NAME "_libcouchbase")
        set(PYCBC_C_MOD_SUFFIX ".so")
    endif()
endif()
if (FORCE_DEBUG OR (CMAKE_BUILD_TYPE MATCHES DEBUG))
    set(RELEASE_TYPE Debug)
else()
    set(RELEASE_TYPE Release)
endif()

macro(use_cxx11)
if (CMAKE_VERSION VERSION_LESS "3.1")
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11")
endif ()
else ()
set (CMAKE_CXX_STANDARD 11)
endif ()
endmacro(use_cxx11)

set (LCB_CFLAGS ${CFLAGS} -fPIC )
set (LCB_CXXFLAGS ${CXXFLAGS} -fPIC)
if (WIN32)
else(WIN32)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -fPIC -fno-strict-aliasing -Wno-missing-braces -Wstrict-prototypes")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-common -dynamic -DNDEBUG -g -fwrapv -Wswitch")
set (CFLAGS "${CFLAGS} -Wno-missing-braces")
set (CXXFLAGS "${CXXFLAGS} -Wno-missing-braces")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -fPIC -fno-strict-aliasing -Wno-missing-braces -Wstrict-prototypes")
endif()

function(get_conan)
    if(NOT CONAN_INCLUDED)
        # Download automatically, you can also just copy the conan.cmake file
        if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
            message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
            file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/master/conan.cmake"
                    "${CMAKE_BINARY_DIR}/conan.cmake")
        endif()
        include(${CMAKE_BINARY_DIR}/conan.cmake)
        conan_add_remote(NAME bintray
        URL https://api.bintray.com/conan/conan-community/conan )
        set(CONAN_INCLUDED TRUE)
    endif()
endfunction()

if("${CMAKE_SIZEOF_VOID_P}" EQUAL "8")
    MESSAGE("++ 64 bit architecture")
    set(PKGARCH "amd64")
else()
    MESSAGE("++ 32 bit architecture")
    set(PKGARCH "win32")
endif()

if (NOT (OPENSSL_ROOT_DIR AND EXISTS ${OPENSSL_ROOT_DIR}))
    set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/OpenSSL-prefix/src/OpenSSL/${PKGARCH})
endif()

function(get_ssl)
    execute_process(
        COMMAND     ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/gen_config.py --temp_build_dir ${CMAKE_CURRENT_BINARY_DIR}
    )
    set (OPENSSLCFG_FILE ${CMAKE_CURRENT_BINARY_DIR}/openssl_version.json)
    file(READ ${OPENSSLCFG_FILE} OPENSSLCFG)
    sbeParseJson(openssl_cfg OPENSSLCFG)
    message("Need to find OpenSSL ${openssl_cfg.major} from\n${OPENSSLCFG}")

    if (OPENSSL_ROOT_DIR AND EXISTS ${OPENSSL_ROOT_DIR})
        message("Found relevant OpenSSL ${OPENSSL_ROOT_DIR}")
        ExternalProject_Add(OpenSSL
                SOURCE_DIR ${OpenSSL_DIR}
                CONFIGURE_COMMAND ""
                BUILD_COMMAND ""
                INSTALL_COMMAND ""
                TEST_COMMAND ""
                )
    endif()
    if (PYCBC_SSL_FETCH MATCHES ".*(CONAN|ALL).*")
        get_conan()
        conan_cmake_run(REQUIRES openssl/${openssl_cfg.major}
                BASIC_SETUP
                BUILD missing
                INSTALL_FOLDER ${openssl_cfg.ssl_root_dir}
                OPTIONS Pkg:shared=True)

        include(${CMAKE_CURRENT_BINARY_DIR}/openssl/conanbuildinfo.cmake)
        conan_basic_setup()
        file(GLOB OPENSSL_FILES ${CONAN_OPENSSL_ROOT}/*)
        file(COPY ${OPENSSL_FILES}
                DESTINATION ${OPENSSL_ROOT_DIR})
        set(OPENSSL_LIB CONAN_PKG::openssl)
    endif()
    if (WIN32)
        # on Windows, we can download from cpython-bin-deps
        message("Can't find relevant OpenSSL, downloading instead")

        if (PYCBC_SSL_FETCH MATCHES ".*(ALL|GITHUB).*")
            ExternalProject_Add(OpenSSL
                URL "https://github.com/python/cpython-bin-deps/archive/openssl-bin-${openssl_cfg.major}.zip"
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND ""
                    INSTALL_COMMAND ""
                    TEST_COMMAND ""

                    )
            ExternalProject_Get_Property(OpenSSL source_dir)
            set(GITHUB_OPENSSL_ROOT_DIR ${source_dir}/${PKGARCH})
            message("Downloaded from Github zip: Set OPENSSL_ROOT_DIR to ${OPENSSL_ROOT_DIR}")
            file(GLOB OPENSSL_FILES ${GITHUB_OPENSSL_ROOT_DIR}/*)
            file(COPY ${OPENSSL_FILES}
                    DESTINATION ${OPENSSL_ROOT_DIR})
            message("Copied files from ${GITHUB_OPENSSL_ROOT_DIR} OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR}: [${OPENSSL_FILES}]")
        endif()
    endif()
endfunction()
message("Got OpenSSL_DIR ${OpenSSL_DIR}")
message("Got OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR}")




set(CMAKE_MACOSX_RPATH 1)

set(BUILD_CFG_FILE cbuild_cfg.json)
file(READ ${BUILD_CFG_FILE} BUILDCFG)
sbeParseJson(build_cfg BUILDCFG)
message("environment LCB_TAG: $ENV{LCB_TAG}")

set(PYCBC_SRC )
if($ENV{HYBRID_BUILD})
    set(HYBRID_BUILD $ENV{HYBRID_BUILD})
endif()
message("got HYBRID_BUILD=${HYBRID_BUILD}")
if("${HYBRID_BUILD}" MATCHES "TRUE")
    message("enabling hybrid build")
    set(ENABLE_HYBRID_BUILD TRUE)
endif()
if(ENABLE_HYBRID_BUILD)
    message("Doing hybrid build")
else()
    message("Scanning ${BUILD_CFG_FILE} for bindings/config")
    foreach(var ${build_cfg})
        message("${var} = ${${var}}")
        if ("${${var}}" MATCHES ".*\\.(cxx|pp|c|h|hpp|hxx)$")
            message("Adding code item ${var} = ${${var}}")
            set(PYCBC_SRC ${PYCBC_SRC} ${${var}})
        endif()
    endforeach()
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DPYCBC_LCB_API=${PYCBC_LCB_API}")
set(LCB_REPO ${build_cfg.comp_options.lcb_archive_repo})
message("repo: ${LCB_REPO}")
if (NOT LCB_REPO)
    message("lcb_archive_repo not found in config, using default")
    set(LCB_REPO "https://github.com/couchbase/libcouchbase" )
endif()
message("config lcb tag: ${build_cfg.comp_options.lcb_tag}")
set(LCB_TAG $ENV{LCB_TAG})
if (LCB_TAG)
    message("using LCB_TAG (${LCB_TAG})from environment")
else()
    set(LCB_TAG ${build_cfg.comp_options.lcb_tag})
    message("using lcb tag (${LCB_TAG}) from config")
endif()
if (NOT LCB_TAG)
    set(LCB_TAG "master")
    message("using default tag(${LCB_TAG})")
endif()
message("lcb_tag: ${LCB_TAG}")

function(make_dir path)
    file(MAKE_DIRECTORY ${path})
endfunction(make_dir)

if (MAKE)
else()
set(MAKE make)
endif()

set(LCB_SRC TRUE)
if (LCB_SRC)
    if (NOT (OPENSSL_ROOT_DIR AND EXISTS ${OPENSSL_ROOT_DIR}))
        find_package(
                OpenSSL
                PATHS ${OPENSSL_ROOT_DIR}
        )
        if (OpenSSL_DIR)
            set(OPENSSL_ROOT_DIR OpenSSL_DIR)
        elseif(PYCBC_SSL_FETCH)
            get_ssl()
        endif()
    endif()
    if (OPENSSL_ROOT_DIR)
        message("Found OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR}")
        set (LCB_SSL_OPTS -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR})
    else()
        message("Found no existent OPENSSL_ROOT_DIR: ${OPENSSL_ROOT_DIR}")
    endif()

    SET(LCB_BUILD_DIR "${CMAKE_BINARY_DIR}/Release/lcb_build")
    SET(LCB_DBGBUILD_DIR "${CMAKE_BINARY_DIR}/Release/lcb_dbgbuild")
    if (CMAKE_BUILD_TYPE MATCHES DEBUG)
        set(LCB_SELECTED_BUILD_DIR ${LCB_DBGBUILD_DIR})
    else()
        set(LCB_SELECTED_BUILD_DIR ${LCB_BUILD_DIR})
    endif()
    SET(LIBCOUCHBASE_LIBRARY_NAME "libcouchbase.lib")
    SET(LIBCOUCHBASE_DBG_LIBRARY_NAME "libcouchbase_d.lib")
    SET(LIBCOUCHBASE_STATIC_LIBRARY_NAME "libcouchbase_d.lib")
    SET(LCB_CMAKE_ARGS -DLCB_NO_TOOLS=1 -DLCB_NO_TESTS=1)
    if (WIN32)
        if (CMAKE_GENERATOR_PLATFORM MATCHES x64)
            set(WIN_PLATFORM "Visual Studio 14 2015 Win64")
        else()
            set(WIN_PLATFORM "Visual Studio 14 2015")
        endif()
        set(LCB_CMAKE_ARGS ${LCB_CMAKE_ARGS} -DLCB_NO_MOCK=1 ${LCB_SSL_OPTS} -DCMAKE_BUILD_PARALLEL_LEVEL=1)
        SET(LIBCOUCHBASE_LIBRARY_FILE "${LCB_BUILD_DIR}/lib/Release/${LIBCOUCHBASE_LIBRARY_NAME}")
        SET(LIBCOUCHBASE_DBG_LIBRARY_FILE "${LCB_BUILD_DIR}/lib/Debug/${LIBCOUCHBASE_DBG_LIBRARY_NAME}")
        ExternalProject_Add(libcouchbase_src
            URL "${LCB_REPO}/archive/${LCB_TAG}.zip"
            CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory ${LCB_BUILD_DIR}
            COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_CURRENT_BINARY_DIR}/install

            COMMAND cd ${LCB_BUILD_DIR} && echo "in ${LCB_BUILD_DIR}" && cmake -DLCB_NO_PLUGINS=1  -B . -G ${WIN_PLATFORM} ${LCB_CMAKE_ARGS} -S <SOURCE_DIR>
            BUILD_COMMAND cd ${LCB_BUILD_DIR} && cmake --build ${LCB_BUILD_DIR} --config Release

            INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/install
            INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/include <INSTALL_DIR>/include
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib/

            COMMAND ${CMAKE_COMMAND} -E copy_directory ${LCB_BUILD_DIR}/generated <INSTALL_DIR>/include
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/Release
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib/Release
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/bin/Release/libcouchbase.dll <INSTALL_DIR>/lib/Release/
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/lib/Release/libcouchbase.lib <INSTALL_DIR>/lib/Release/
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/lib/Release/libcouchbase.exp <INSTALL_DIR>/lib/Release/

            COMMAND cd ${LCB_BUILD_DIR} && cmake --build ${LCB_BUILD_DIR} --config Debug
            COMMAND ${CMAKE_COMMAND} -E copy_directory ${LCB_BUILD_DIR}/generated <INSTALL_DIR>/include

            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/bin/Debug
            COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib/Debug
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/bin/Debug/libcouchbase_d.dll <INSTALL_DIR>/lib/Debug/
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/lib/Debug/libcouchbase_d.lib <INSTALL_DIR>/lib/Debug/
            COMMAND ${CMAKE_COMMAND} -E copy ${LCB_BUILD_DIR}/lib/Debug/libcouchbase_d.exp <INSTALL_DIR>/lib/Debug/
            )


    else (WIN32)
        if (APPLE)
            SET(LIBCOUCHBASE_LIBRARY_NAME "libcouchbase.dylib")
            SET(LIBCOUCHBASE_DBG_LIBRARY_NAME "libcouchbase.dylib")
            SET(LIBCOUCHBASE_STATIC_LIBRARY_NAME "libcouchbaseS.a")

        else (APPLE)
            SET(LIBCOUCHBASE_LIBRARY_NAME "libcouchbase.so")
            SET(LIBCOUCHBASE_DBG_LIBRARY_NAME "libcouchbase.so")
        endif (APPLE)

        set(LCB_CMAKE_ARGS ${LCB_CMAKE_ARGS} -DLCB_NO_MOCK=1 -DLCB_NO_PLUGINS=1 -DLCB_BUILD_DTRACE=OFF)
        set(LCB_CMAKE_CONFIG_CMD cmake -E env CXXFLAGS=${LCB_CXXFLAGS} env CFLAGS=${LCB_CFLAGS}
                cmake <SOURCE_DIR> ${LCB_CMAKE_ARGS})
        SET(_parallelism 4)

        INCLUDE(GNUInstallDirs)

        ExternalProject_Add(libcouchbase_src
                URL "${LCB_REPO}/archive/${LCB_TAG}.zip"
                CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory "${LCB_BUILD_DIR}"
                make_dir ${LCB_SELECTED_BUILD_DIR}

                COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_CURRENT_BINARY_DIR}/install

                COMMAND cd ${LCB_SELECTED_BUILD_DIR} && ${LCB_CMAKE_CONFIG_CMD}
                        -DCMAKE_BUILD_TYPE=${RELEASE_TYPE} -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>/${RELEASE_TYPE}

                BUILD_IN_SOURCE 1
                BUILD_COMMAND ${MAKE} -j${_parallelism} all install -C ${LCB_SELECTED_BUILD_DIR}

                INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/install
                INSTALL_COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/${RELEASE_TYPE}/include <INSTALL_DIR>/include
                COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib
                COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/${RELEASE_TYPE}/${CMAKE_INSTALL_LIBDIR} <INSTALL_DIR>/lib/${RELEASE_TYPE}
                COMMAND ${CMAKE_COMMAND} -E remove_directory <INSTALL_DIR>/${RELEASE_TYPE}
                )
        SET(LIBCOUCHBASE_LIBRARY_FILE "${LCB_BUILD_DIR}/lib/${LIBCOUCHBASE_LIBRARY_NAME}")
        SET(LIBCOUCHBASE_DBG_LIBRARY_FILE "${LCB_DBGBUILD_DIR}/lib/${LIBCOUCHBASE_DBG_LIBRARY_NAME}")
        if (CMAKE_BUILD_TYPE MATCHES DEBUG)
            SET(LIBCOUCHBASE_SELECTED_LIBRARY_FILE ${LIBCOUCHBASE_DBG_LIBRARY_FILE})
        else()
            SET(LIBCOUCHBASE_SELECTED_LIBRARY_FILE ${LIBCOUCHBASE_LIBRARY_FILE})
        endif()
        SET(LIBCOUCHBASE)
        # OS X-only: Custom post-build step to set the shared library install name.
        if (APPLE)
            ExternalProject_Add_Step(libcouchbase_src install_name
                    COMMAND install_name_tool -id @rpath/libcouchbase.dylib ${LIBCOUCHBASE_SELECTED_LIBRARY_FILE}
                    COMMAND install_name_tool -id @rpath/libcouchbase.2.dylib ${LCB_SELECTED_BUILD_DIR}/lib/libcouchbase.2.dylib
                    DEPENDEES build
                    DEPENDERS install
                    )
        endif (APPLE)
    endif (WIN32)
endif (LCB_SRC)
set(LCBCXX_ROOT_PATH ${CMAKE_BINARY_DIR}/${LCBCXX_ROOT})

set(libcouchbase_src ${CMAKE_BINARY_DIR}/${LCB_ROOT})
set(install_dir ${CMAKE_BINARY_DIR}/install)
if (LCB_SRC)
    ExternalProject_Get_Property(libcouchbase_src install_dir)
    ExternalProject_Get_Property(libcouchbase_src source_dir)
    set(CPP_DEPS ${CPP_DEPS} libcouchbase_src)
else (LCB_SRC)
    find_library(libcouchbase_src libcouchbase)
endif(LCB_SRC)

if("${PYCBC_ADD_INLINE}")
    add_subdirectory(${libcouchbase_src})
    if (PYCBC_CXX)
        add_subdirectory(${CMAKE_BINARY_DIR}/${LCBCXX_ROOT})
    endif()
endif()

cmake_policy(SET CMP0015 NEW)

if ("${PYCBC_CPPYY}")
    find_package(Cppyy REQUIRED)
    cppyy_add_bindings(
            "Stuff" "0" "Enrico" "enrico.guiraud@cern.ch"
            LANGUAGE_STANDARD "11"
            H_FILES "couchbase.h")
endif()

if(USE_BOOST)
    find_package(Boost COMPONENTS python27 REQUIRED)
    set(BOOST_SOURCES
            /usr/local/include/boost/python/module.hpp
            src/bindings.cpp)
else()
    set(Boost_LIBRARIES )
    set(Boost_INCLUDE_DIR )
    set(BOOST_SOURCES )
endif(USE_BOOST)
include_directories(
        ${PYTHON_INCLUDE_DIRS}
        ${PYTHON_INCLUDE_PATH}
        /usr/local/include
        ${LCBCXX_ROOT_PATH}/include
        ${CMAKE_BINARY_DIR}/${install_dir}/include
        ${install_dir}/include
        ${source_dir} ${source_dir}/src/*
        ${source_dir}
        ${CMAKE_BINARY_DIR}/Release/lcb_dbgbuild/generated/
        ${libcouchbase_src}/src
        ${libcouchbase_src}/include
)

file(GLOB_RECURSE PY_SOURCES_FOLDERS acouchbase/*.py txcouchbase/*.py  couchbase/*.py couchbase_core/*.py couchbase_tests/*.py couchbase_v2/*.py)
file(GLOB PY_SOURCES_FILES *.py)
set(PY_SOURCES ${PY_SOURCES_FOLDERS} ${PY_SOURCES_FILES})
message("Got PY_SOURCES ${PY_SOURCES}")
add_library(couchbase_python_client SHARED
        ${PY_SOURCES_FOLDERS}
        ${PY_SOURCES}
        ${PYCBC_SRC}
)

if (PYTHON_LIBDIR)
else()
    set(PYTHON_LIBDIR ${PYTHON_LIBRARIES})
endif()
link_directories(${CMAKE_BINARY_DIR}/${install}/lib/${RELEASE_TYPE} ${PYTHON_LIBDIR})
add_dependencies(couchbase_python_client ${CPP_DEPS})
target_include_directories(couchbase_python_client PUBLIC  ${install_dir}/include)

if (PYTHON_LIBFILE)
else()
    set(PYTHON_LIBFILE python)
endif()

# required rpaths
# OSX: loader_path
# Linux: $ORIGIN
set(LIBCOUCHBASE_DBG_DIRPATH ${install_dir}/lib/Debug)
set(LIBCOUCHBASE_RELEASE_DIRPATH ${install_dir}/lib/Release)
set(LIBCOUCHBASE_DBG_PATH ${LIBCOUCHBASE_DBG_DIRPATH}/${LIBCOUCHBASE_DBG_LIBRARY_NAME})
set(LIBCOUCHBASE_RELEASE_PATH ${LIBCOUCHBASE_RELEASE_DIRPATH}/${LIBCOUCHBASE_LIBRARY_NAME})

if (FORCE_DEBUG OR CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(LIBCOUCHBASE_FINAL_DIRPATH ${LIBCOUCHBASE_DBG_DIRPATH})
    set(LIBCOUCHBASE_FINAL_PATH ${LIBCOUCHBASE_DBG_PATH})
else()
    set(LIBCOUCHBASE_FINAL_DIRPATH ${LIBCOUCHBASE_RELEASE_DIRPATH})
    set(LIBCOUCHBASE_FINAL_PATH ${LIBCOUCHBASE_RELEASE_PATH})
endif()



set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
target_link_libraries(couchbase_python_client ${Boost_LIBRARIES}   ${LIBCOUCHBASE_FINAL_PATH}  ${PYTHON_LIBRARIES} -Wl,-rpath,${LIBCOUCHBASE_FINAL_DIRPATH})
link_libraries(libcouchbase_src
        ${OPENSSL_LIB})
set_target_properties(
        couchbase_python_client
        PROPERTIES COMPILE_FLAGS "${CMAKE_C_FLAGS}"
        INSTALL_RPATH "${LIBCOUCHBASE_DBG_DIRPATH};${INSTALL_RPATH}"
        PREFIX ""
        OUTPUT_NAME ${PYCBC_C_MOD_NAME}
        LINKER_LANGUAGE C
        SUFFIX ${PYCBC_C_MOD_SUFFIX}
)

