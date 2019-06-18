cmake_minimum_required(VERSION 3.1)
project(couchbase_python_client)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(THIRDPARTY_LCB_ROOT libcouchbase_src-prefix)
set(LCBCXX_ROOT libcouchbase-cxx-prefix/src/libcouchbase-cxx)
set(LCB_ROOT ${THIRDPARTY_LCB_ROOT}/src/libcouchbase_src)
set(PYCBC_CMAKE_CPYTHON_WRAPPER 1)
include(FindPythonInterp)
include(FindPythonLibs)

if (PYTHON_LIBDIR)
    set(PYTHON_LIBDIR_HINT HINTS ${PYTHON_LIBDIR})
else(PYTHON_LIBDIR)
    set(PYTHON_LIBDIR_HINT "")
endif()

if (PYTHON_VERSION_EXACT)
    find_package(PythonInterp ${PYTHON_VERSION_EXACT} EXACT REQUIRED)
    find_package(PythonLibs ${PYTHON_VERSION_EXACT} EXACT  REQUIRED)
else(PYTHON_VERSION_EXACT)
    find_package(PythonInterp REQUIRED)
    find_package(PythonLibs ${PYTHON_LIBDIR_HINT} REQUIRED)
endif()

cmake_policy(SET CMP0054 NEW)
if(WIN32)
    set(PYCBC_C_MOD_NAME "_libcouchbase.${PYTHONLIBS_VERSION_STRING}")
    set(PYCBC_C_MOD_SUFFIX ".pyd")
else()
    set(PYCBC_C_MOD_NAME "_libcouchbase")
    set(PYCBC_C_MOD_SUFFIX ".so")
endif()
if (FORCE_DEBUG OR (CMAKE_BUILD_TYPE MATCHES DEBUG))
    set(RELEASE_TYPE Debug)
else()
    set(RELEASE_TYPE Release)
endif()

if(HYBRID_BUILD)
set(PYCBC_C_MOD_NAME "_dummy")
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

set (LCB_CFLAGS ${CFLAGS} -fPIC)
set (LCB_CXXFLAGS ${CFLAGS} -fPIC)
if (WIN32)
else(WIN32)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -fPIC -fno-strict-aliasing -Wall -Wstrict-prototypes -pthread")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-common -dynamic -DNDEBUG -g -fwrapv")
endif()

if ("$PYCBC_USE_CONAN")
    # Download automatically, you can also just copy the conan.cmake file
    include(${CMAKE_BINARY_DIR}/conan.cmake)
    if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
        message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
        file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/master/conan.cmake"
                "${CMAKE_BINARY_DIR}/conan.cmake")
    endif()
    conan_cmake_run(REQUIRES OpenSSL/1.0.2n@conan/stable
            BASIC_SETUP
            BUILD missing)
endif()

include(ExternalProject)


set(CMAKE_MACOSX_RPATH 1)
set(LCB_REPO_PROTOCOL http)
if (LCB_REPO_USERNAME)
    set(LCB_REPO_AT "@")
else(LCB_REPO_USERNAME)
    set(LCB_REPO_AT "")
endif()

set(BUILD_CFG_FILE cbuild_cfg.json)
include(cmake/json-cmake/JSONParser.cmake)
file(READ ${BUILD_CFG_FILE} BUILDCFG)
sbeParseJson(build_cfg BUILDCFG)

if (PYCBC_LCB_API)
    message("Picked up PYCBC_LCB_API=[${PYCBC_LCB_API}] from calling environment")
else()
    message("got build_cfg ${build_cfg}")
    set (PYCBC_LCB_API ${build_cfg.comp_options.PYCBC_LCB_API})
    message("Picked up PYCBC_LCB_API=[${PYCBC_LCB_API}] from config")
endif()
message("got PYCBC_LCB_API... ${PYCBC_LCB_API}")

set(PYCBC_SRC )
message("got HYBRID_BUILD=${HYBRID_BUILD}")
if("${HYBRID_BUILD}")
    message("enabling hybrid build")
    set(ENABLE_HYBRID_BUILD TRUE)
endif()
if(ENABLE_HYBRID_BUILD)
    message("Doing hybrid build")
else()
    message("Scanning ${BUILD_CFG_FILE} for bindings/config, final PYCBC_LCB_API==[${PYCBC_LCB_API}]")
    foreach(var ${build_cfg})
        message("${var} = ${${var}}")
        if ("${${var}}" MATCHES ".*\\.(cxx|pp|c|h|hpp|hxx)$")
            # only add applicable API-specific code
            if (("${var}" MATCHES "build_cfg.apis..*") AND (NOT ("${var}" MATCHES ".*${PYCBC_LCB_API}.*")))
                continue()
            endif()
            message("Adding code item ${var} = ${${var}}")
            set(PYCBC_SRC ${PYCBC_SRC} ${${var}})
        endif()
    endforeach()
endif()

set(LCB_COLLECTIONS_REF ${build_cfg.apis.${PYCBC_LCB_API}.git.ref})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DPYCBC_LCB_API=${PYCBC_LCB_API}")
set(LCB_REPO ${build_cfg.apis.${PYCBC_LCB_API}.git.repo})
if (NOT LCB_REPO)
    set(LCB_REPO ${LCB_REPO_PROTOCOL}://${LCB_REPO_AT}${LCB_REPO_USERNAME}review.couchbase.org/libcouchbase)
endif()

set(DEBUG_TAGSEARCH  TRUE)
set(LCB_TAG_CANDIDATES ${build_cfg.apis.${PYCBC_LCB_API}.git.tag})
if (LCB_TAG)
    message("Got tag from environemtn: ${LCB_TAG}")
elseif(NOT LCB_TAG_CANDIDATES)
    message("Got no tag, defaulting to master")
    set(LCB_TAG master)
else()
    message("Got tag candidates in order of preference: ${LCB_TAG_CANDIDATES}")
    find_package (Git)
    if (GIT_FOUND)
        message("git found: ${GIT_EXECUTABLE} in version ${GIT_VERSION_STRING}")
    endif (GIT_FOUND)

    execute_process(
            COMMAND ${GIT_EXECUTABLE} ls-remote ${LCB_REPO} tags/*
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE OUTPUT)

    set (DEFAULT_TAG "master")
    set (REPO_TAG ${DEFAULT_TAG} CACHE STRING "Select a repo tag")

    string (REGEX MATCHALL "([^/]+)[\^{}]\n" TAGS ${OUTPUT})
    string (REGEX REPLACE "\n" ";"  TAGS ${TAGS})
    set_property (CACHE REPO_TAG PROPERTY STRINGS ${TAGS})

    message("got REPO_TAG ${TAGS}")
    foreach(CAND_NUM IN LISTS LCB_TAG_CANDIDATES)
        set(CANDIDATE ${build_cfg.apis.${PYCBC_LCB_API}.git.tag_${CAND_NUM}})
        set(BROADEST_TAG ${CANDIDATE})
        message("looking for ${CANDIDATE}")
        foreach(TAG ${TAGS})
            string(REGEX MATCH "${CANDIDATE}" MATCHRES "${TAG}")
            if (DEBUG_TAGSEARCH)
                message("comparing tag [${TAG}] against candidate [${CANDIDATE}], got ${MATCHRES}")
            endif()
            if (MATCHRES)
                message("Got matching tag ${TAG}")
                set(LCB_TAG ${TAG})
                break()
            else()
                if (DEBUG_TAGSEARCH)
                    message("[${CANDIDATE}] doesn't match [${TAG}], trying semver")
                endif()
                string(REGEX MATCHALL "[<>]((.+,)|(.+$))" TERMS ${CANDIDATE})
                if (DEBUG_TAGSEARCH)
                    message("Looking for semver spec in ${CANDIDATE}: [${TERMS}]")
                endif()

                string(REGEX REPLACE "-alpha." ".0." DEALPHAD_TAG ${TAG})
                string(REGEX REPLACE "-beta." ".1." CANONICAL_TAG ${DEALPHAD_TAG})

                if (((NOT BEST_CANONICAL_SEMVER) OR (CANONICAL_TAG VERSION_GREATER BEST_CANONICAL_SEMVER)) AND TERMS)
                    if (DEBUG_TAGSEARCH)
                        message("Appears to be a semver termlist, trying ${TERMS}")
                    endif()
                    set(TERMFAIL FALSE)
                    foreach(RAW_TERM IN LISTS TERMS)
                        string(REGEX REPLACE "(.+)," "\\1" TERM ${RAW_TERM})


                        if (DEBUG_TAGSEARCH)
                            message("Testing ${TERM}")
                        endif()
                        if (${TERM} MATCHES ">.*")
                            string(REGEX REPLACE ">(.*)" "\\1" GT_TERM ${TERM})
                            string(REGEX REPLACE "-alpha." ".0." DEALPHAD_TERM ${GT_TERM})
                            string(REGEX REPLACE "-beta." ".1." GT_TERM_CANONICAL ${DEALPHAD_TERM})
                            if (NOT (${CANONICAL_TAG} VERSION_GREATER ${GT_TERM_CANONICAL}))
                                if (DEBUG_TAGSEARCH)
                                    message("${TAG} not greater than ${GT_TERM}")
                                endif()
                                set(TERMFAIL TRUE)
                                break()
                            endif()
                        elseif (${TERM} MATCHES "<.*")
                            string(REGEX REPLACE "<(.*)" "\\1" LT_TERM ${TERM})
                            string(REGEX REPLACE "-alpha." ".0." DEALPHAD_TERM ${LT_TERM})
                            string(REGEX REPLACE "-beta." ".1." LT_TERM_CANONICAL ${DEALPHAD_TERM})
                            if (NOT (${CANONICAL_TAG} VERSION_LESS ${LT_TERM_CANONICAL}))
                                if (DEBUG_TAGSEARCH)
                                    message("${TAG} not less than ${LT_TERM}")
                                endif()
                                set(TERMFAIL TRUE)
                                break()
                            endif()
                        else()
                            message("Unrecognised semver term, bailing out")
                            set(TERMFAIL TRUE)
                        endif()
                    endforeach()
                    if (NOT TERMFAIL)
                        message("All terms satisfied, ${TAG} best so far, previous was [${BEST_SEMVER}]")
                        set(BEST_SEMVER ${TAG})
                        set(BEST_CANONICAL_SEMVER ${CANONICAL_TAG})
                    endif()
                endif()
            endif()
        endforeach()
        if (LCB_TAG)
            break()
        endif()
    endforeach()
    if (NOT LCB_TAG)
        message("Didn't find any of the tags listed, trying semver")
        if (BEST_SEMVER)
            message("Using best semver match: ${BEST_SEMVER}")
            set(LCB_TAG ${BEST_SEMVER})
        else()
            message("using last entry ${BROADEST_TAG}")
            set(LCB_TAG ${BROADEST_TAG})
        endif()
    endif()
endif()

function(make_dir path)
    file(MAKE_DIRECTORY ${path})
endfunction(make_dir)

if (MAKE)
else()
set(MAKE make)
endif()

set(LCB_SRC TRUE)
if (LCB_SRC)
	if (LCB_COLLECTIONS_REF)
			set (REF_CMDS git fetch ${LCB_REPO} ${LCB_COLLECTIONS_REF}
	        		&& git reset --hard
	       		 	&& git clean -f
	        		&& git checkout FETCH_HEAD)
    else()
        set (REF_CMDS echo "no ref required")
	endif()
    message("got build_cfg ${build_cfg}")
    message("got build_cfg.patches.git ${build_cfg.patches.git.repo} ${build_cfg.patches.git.ref}")
    set(PATCH_REPO ${build_cfg.apis.${PYCBC_LCB_API}.patches.git.repo})
    if (PATCH_REPO)
        set(PATCH_REF ${build_cfg.apis.${PYCBC_LCB_API}.patches.git.ref})
    else()
        set(PATCH_REPO ${build_cfg.patches.git.repo})
        set(PATCH_REF ${build_cfg.patches.git.ref})
    endif()

    if (PATCH_REPO)
        set (PATCH_COMMAND git config user.email \"buildbot@couchbase.com\"
                && git config user.name \"Couchbase Buildbot\"
                && git fetch ${PATCH_REPO} ${PATCH_REF}
                && git reset --hard
                && git clean -f
                && git cherry-pick FETCH_HEAD)
    else()
        set (PATCH_COMMAND echo "no patch required")
    endif()
    message("Got PATCH_COMMAND ${PATCH_COMMAND}")
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
        set(LCB_CMAKE_ARGS ${LCB_CMAKE_ARGS} -DLCB_NO_SSL=1 -DLCB_NO_MOCK=1)
        SET(LIBCOUCHBASE_LIBRARY_FILE "${LCB_BUILD_DIR}/lib/Release/${LIBCOUCHBASE_LIBRARY_NAME}")
        SET(LIBCOUCHBASE_DBG_LIBRARY_FILE "${LCB_BUILD_DIR}/lib/Debug/${LIBCOUCHBASE_DBG_LIBRARY_NAME}")
        ExternalProject_Add(libcouchbase_src
            GIT_REPOSITORY ${LCB_REPO}
            GIT_TAG ${LCB_TAG}
            PATCH_COMMAND ${PATCH_COMMAND}
            CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory ${LCB_BUILD_DIR}
            COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_CURRENT_BINARY_DIR}/install
            COMMAND cd <SOURCE_DIR> && ${REF_CMDS}

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

        ExternalProject_Add(libcouchbase_src
                GIT_REPOSITORY ${LCB_REPO}
                GIT_TAG ${LCB_TAG}
                UPDATE_COMMAND ""
                PATCH_COMMAND ${PATCH_COMMAND}
                CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory "${LCB_BUILD_DIR}"
                make_dir ${LCB_SELECTED_BUILD_DIR}

                COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_CURRENT_BINARY_DIR}/install

                COMMAND cd <SOURCE_DIR> && ${REF_CMDS}
                COMMAND cd ${LCB_SELECTED_BUILD_DIR} && ${LCB_CMAKE_CONFIG_CMD}
                        -DCMAKE_BUILD_TYPE=${RELEASE_TYPE} -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>/${RELEASE_TYPE}

                BUILD_IN_SOURCE 1
                BUILD_COMMAND ${MAKE} -j${_parallelism} all install -C ${LCB_SELECTED_BUILD_DIR}

                INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/install
                INSTALL_COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/${RELEASE_TYPE}/include <INSTALL_DIR>/include
                COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib
                COMMAND ${CMAKE_COMMAND} -E rename <INSTALL_DIR>/${RELEASE_TYPE}/lib <INSTALL_DIR>/lib/${RELEASE_TYPE}
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

file(GLOB_RECURSE PY_SOURCES_FOLDERS acouchbase/*.py gcouchbase/*.py txcouchbase/*.py  couchbase/*.py couchbase_core/*.py couchbase_tests/*.py couchbase_v2/*.py)
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
set_target_properties(
        couchbase_python_client
        PROPERTIES COMPILE_FLAGS "${CMAKE_C_FLAGS}"
        INSTALL_RPATH "${LIBCOUCHBASE_DBG_DIRPATH};${INSTALL_RPATH}"
        PREFIX ""
        OUTPUT_NAME ${PYCBC_C_MOD_NAME}
        LINKER_LANGUAGE C
        SUFFIX ${PYCBC_C_MOD_SUFFIX}
)

