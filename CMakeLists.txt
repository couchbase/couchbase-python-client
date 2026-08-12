#  Copyright 2016-2023. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

cmake_minimum_required(VERSION 3.19)
# needed for CMAKE_MSVC_RUNTIME_LIBRARY
cmake_policy(SET CMP0091 NEW)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)

function(get_python_version)
  if(DEFINED ENV{PYCBC_PYTHON3_EXECUTABLE})
    message(STATUS "Using python3 executable from environment: $ENV{PYCBC_PYTHON3_EXECUTABLE}")
    set(PYTHON3_EXE $ENV{PYCBC_PYTHON3_EXECUTABLE})
  else()
    message(STATUS "Using default python3 executable")
    set(PYTHON3_EXE "python3")
  endif()

  # Be careful with whitespace
  execute_process(COMMAND ${PYTHON3_EXE} -c
                  "import platform
import re
version = platform.python_version()
match = re.search(r'\\d+\\.\\d+\\.\\d+', version)
print(match.group())"
                  OUTPUT_VARIABLE local_python_version
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(LOCAL_PYTHON_VERSION "${local_python_version}" PARENT_SCOPE)
endfunction()

if(WIN32)
  # cmake-format: off
  # MultiThreaded$<$<CONFIG:Debug>:Debug>DLL for /MD compile flag
  # MultiThreaded$<$<CONFIG:Debug>:Debug> for /MT compile flag
  # cmake-format: on
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
  add_definitions(/bigobj)
  add_definitions(-D_WIN32_WINNT=0x0601)
endif()

project(couchbase_client)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

get_python_version()
message(STATUS "LOCAL_PYTHON_VERSION=${LOCAL_PYTHON_VERSION}")

if(LOCAL_PYTHON_VERSION)
  set(Python3_FIND_VIRTUALENV FIRST)
  message(STATUS "Attempting to find python version: ${LOCAL_PYTHON_VERSION}")
  find_package(Python3 ${LOCAL_PYTHON_VERSION} EXACT COMPONENTS Interpreter Development.Module)
else()
  find_package(Python3 COMPONENTS Interpreter Development.Module)
endif()


if(WIN32)
  set(PYCBC_C_MOD_SUFFIX_DEFAULT ".pyd")
else()
  set(PYCBC_C_MOD_SUFFIX_DEFAULT ".so")
endif()
# Normally set by pycbc_build_setup.py to the building interpreter's own
# sysconfig EXT_SUFFIX (e.g. .cpython-310-darwin.so). This is what makes a
# version-specific (non-abi3) build loadable only by a matching interpreter;
# the plain fallback below is for a raw `cmake` invocation outside setup.py.
set(PYCBC_C_MOD_SUFFIX "${PYCBC_C_MOD_SUFFIX_DEFAULT}" CACHE STRING "Compiled extension module suffix, including any interpreter tag")

file(READ "${PROJECT_SOURCE_DIR}/couchbase/_version.py" PYCBC_VERSION_CONTENTS)
string(REGEX MATCH "__version__.*([0-9]+\\.[0-9]+\\.[0-9]+)(\\.?[+a-z0-9]*)" PYCBC_FOUND_VERSION ${PYCBC_VERSION_CONTENTS})
set(PYCBC_VERSION "${CMAKE_MATCH_1}")
if(NOT CMAKE_MATCH_2 STREQUAL "")
  set(PYCBC_VERSION "${PYCBC_VERSION}${CMAKE_MATCH_2}")
endif()
message(STATUS "PYCBC_VERSION=${PYCBC_VERSION}")

option(USE_STATIC_BORINGSSL "Statically link BoringSSL instead of dynamically linking OpenSSL" FALSE)
message(STATUS "USE_STATIC_BORINGSSL=${USE_STATIC_BORINGSSL}")
if(NOT USE_STATIC_BORINGSSL)
  set(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL
      ON
      CACHE BOOL "" FORCE)
  if(OPENSSL_ROOT_DIR)
    message(STATUS "OPENSSL_ROOT_DIR set to ${OPENSSL_ROOT_DIR}, calling finder...")
    find_package(OpenSSL REQUIRED)
  endif()

  if(OPENSSL_FOUND)
    message(STATUS "OpenSSL found, OPENSSL_ROOT_DIR set to ${OPENSSL_ROOT_DIR}")
  else()
    if(WIN32)
      if("${CMAKE_SIZEOF_VOID_P}" EQUAL "8")
        message(STATUS "++ 64 bit architecture")
        set(PKGARCH "amd64")
      else()
        message(STATUS "++ 32 bit architecture")
        set(PKGARCH "win32")
      endif()
      if(NOT OPENSSL_VERSION)
        message(STATUS "No OpenSSL version set...cannot attempt to download.")
      else()
        # see pycbc_build_setup.py for default OpenSSL versions
        FetchContent_Declare(openssl
                             URL https://github.com/python/cpython-bin-deps/archive/openssl-bin-${OPENSSL_VERSION}.zip)
        message(STATUS "fetching OpenSSL version: ${OPENSSL_VERSION}")
        FetchContent_Populate(openssl)
        message(STATUS "Downloaded OpenSSL: ${openssl_SOURCE_DIR}/${PKGARCH}")
        set(OPENSSL_ROOT_DIR ${openssl_SOURCE_DIR}/${PKGARCH})
      endif()
    elseif(APPLE)
      # we were not supplied an OPENSSL_ROOT_DIR, so for macos assume brew is how it is installed, if it is...
      find_program(BREW_COMMAND brew)
      if(BREW_COMMAND)
        message(STATUS "brew command: ${BREW_COMMAND}")
        set(BREW_OPENSSL "openssl@3")
        if(USE_OPENSSLV1_1)
          set(BREW_OPENSSL "openssl@1.1")
          message(STATUS "Using OpenSSL v1.1 from homebrew")
        else()
          message(STATUS "Using OpenSSL v3 from homebrew")
        endif()
        execute_process(
          COMMAND ${BREW_COMMAND} --prefix ${BREW_OPENSSL}
          OUTPUT_VARIABLE BREW_OPENSSL_PREFIX
          RESULT_VARIABLE BREW_RESULT
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message(STATUS "brew result: ${BREW_RESULT}, prefix: ${BREW_OPENSSL_PREFIX}")
        if(BREW_RESULT EQUAL 0)
          set(OPENSSL_ROOT_DIR
              ${BREW_OPENSSL_PREFIX}
              CACHE INTERNAL "" FORCE)
          message(STATUS "brew set OPENSSL_ROOT_DIR to ${OPENSSL_ROOT_DIR}, finding OpenSSL...")
        endif()
      endif()
    else()
      message(STATUS "Not mac or windows, so assuming OpenSSL v1.1 is installed and findable...")
    endif()
    find_package(OpenSSL REQUIRED)
  endif()

  message(STATUS "Adding ${OPENSSL_INCLUDE_DIR} to include dirs...")
  include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
else()
  set(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL
      OFF
      CACHE BOOL "" FORCE)
  set(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL
      ON
      CACHE BOOL "" FORCE)
endif()

set(COUCHBASE_CXX_CLIENT_WRAPPER_UNIFIED_ID
    "python/${PYCBC_VERSION}"
    CACHE STRING "" FORCE)
set(COUCHBASE_CXX_CLIENT_PYTHON_WARNINGS
    ON
    CACHE INTERNAL "")
set(COUCHBASE_CXX_CLIENT_BUILD_STATIC
    ON
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_SHARED
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_INSTALL
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_DOCS
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_TOOLS
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
    OFF
    CACHE BOOL "" FORCE)

# cmake-format: off
# PYCBC-1374 + PYCBC-1495: Move to dynamically link against static stdlib to avoid issues with:
#   - other packages that also link against stdlibs (grpc)
#   - building SDK on RHEL >= RHEL8 as static stdlibs are not available.
# cmake-format: on
option(USE_STATIC_STDLIB "Statically link C++ standard library" FALSE)
if(USE_STATIC_STDLIB)
  set(COUCHBASE_CXX_CLIENT_STATIC_STDLIB
      ON
      CACHE BOOL "" FORCE)
else()
  set(COUCHBASE_CXX_CLIENT_STATIC_STDLIB
      OFF
      CACHE BOOL "" FORCE)
endif()
message(STATUS "USE_STATIC_STDLIB=${USE_STATIC_STDLIB}")

option(DOWNLOAD_MOZILLA_CA_BUNDLE "Allow C++ client to download and embed Mozilla certificates" TRUE)
if(DOWNLOAD_MOZILLA_CA_BUNDLE)
  set(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE
      ON
      CACHE BOOL "" FORCE)
else()
  set(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE
      OFF
      CACHE BOOL "" FORCE)
endif()
message(STATUS "DOWNLOAD_MOZILLA_CA_BUNDLE=${DOWNLOAD_MOZILLA_CA_BUNDLE}")

# handle CPM cache dir
if(DEFINED COUCHBASE_CXX_CPM_CACHE_DIR AND NOT COUCHBASE_CXX_CPM_CACHE_DIR STREQUAL "")
  set(CPM_SOURCE_CACHE "${COUCHBASE_CXX_CPM_CACHE_DIR}")
endif()

if(DEFINED CPM_SOURCE_CACHE)
  message(STATUS "CPM_SOURCE_CACHE=${CPM_SOURCE_CACHE}")
endif()

add_subdirectory(deps/couchbase-cxx-client)

set(COUCHBASE_CXX_BINARY_DIR "${CMAKE_BINARY_DIR}/deps/couchbase-cxx-client")
set(COUCHBASE_CXX_SOURCE_DIR "${PROJECT_SOURCE_DIR}/deps/couchbase-cxx-client")
message(STATUS "COUCHBASE_CXX_BINARY_DIR=${COUCHBASE_CXX_BINARY_DIR}")
message(STATUS "COUCHBASE_CXX_SOURCE_DIR=${COUCHBASE_CXX_SOURCE_DIR}")
if(DEFINED COUCHBASE_CXX_CPM_CACHE_DIR AND NOT COUCHBASE_CXX_CPM_CACHE_DIR STREQUAL "")
  file(COPY "${COUCHBASE_CXX_BINARY_DIR}/mozilla-ca-bundle.crt" "${COUCHBASE_CXX_BINARY_DIR}/mozilla-ca-bundle.sha256"
    DESTINATION "${COUCHBASE_CXX_CPM_CACHE_DIR}")
  message(STATUS "Copied Mozilla cert bundle to cache.  COUCHBASE_CXX_CPM_CACHE_DIR=${COUCHBASE_CXX_CPM_CACHE_DIR}")
endif()

if(Python3_FOUND)
  message(STATUS "Python executable: ${Python3_EXECUTABLE}")
  message(STATUS "Python include dir: ${Python3_INCLUDE_DIRS}")
  message(STATUS "Python libs: ${Python3_LIBRARIES}")
else()
  message(FATAL_ERROR "Python3 not found.")
endif()

include_directories(SYSTEM ${Python3_INCLUDE_DIRS})
file(
  GLOB
  SOURCE_FILES
  "src/*.cxx"
  "src/transactions/*.cxx")
add_library(pycbc_core SHARED ${SOURCE_FILES})

target_compile_definitions(pycbc_core PRIVATE COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS)

# PYCBC-1854: Build against the Python Stable ABI (abi3) by default so a single
# wheel can target every supported CPython. Power users can opt out via the
# PYCBC_PY_LIMITED_API env var (handled in pycbc_build_setup.py) which passes
# -DPYCBC_PY_LIMITED_API:BOOL=OFF here, producing a version-specific build.
option(PYCBC_PY_LIMITED_API "Build the C extension against Py_LIMITED_API (abi3)" ON)
# PYCBC_PY_LIMITED_API_HEX is the lowest CPython release this binary will load on.
# Driven by PYCBC_PY_LIMITED_API_VERSION in pycbc_build_setup.py so it stays in
# lockstep with the bdist_wheel --py-limited-api tag. 0x030A0000 == CPython 3.10,
# the floor required by the limited-API symbols pycbc uses.
set(PYCBC_PY_LIMITED_API_HEX "0x030A0000" CACHE STRING "Py_LIMITED_API hex (e.g. 0x030A0000 for CPython 3.10)")
message(STATUS "PYCBC_PY_LIMITED_API=${PYCBC_PY_LIMITED_API}")
set(PYCBC_C_MOD_OUTPUT_NAME "_core")
if(PYCBC_PY_LIMITED_API)
  message(STATUS "PYCBC_PY_LIMITED_API_HEX=${PYCBC_PY_LIMITED_API_HEX}")
  target_compile_definitions(pycbc_core PRIVATE Py_LIMITED_API=${PYCBC_PY_LIMITED_API_HEX})
  # abi3 tag goes on the suffix (_core.abi3.so), not the output name. The
  # interpreter tag in PYCBC_C_MOD_SUFFIX is dropped here so the filename matches
  # CMakeBuildExt.get_ext_filename(), which pairs ".abi3" with the bare .so.
  string(REGEX REPLACE "^.*(\\.[^.]+)$" "\\1" PYCBC_C_MOD_SHARED_LIB_EXT "${PYCBC_C_MOD_SUFFIX}")
  if(WIN32)
    # Windows has no ".abi3" import suffix: importlib only recognizes
    # ".cp3XY-win_amd64.pyd" and a bare ".pyd", so the stable-ABI module is _core.pyd.
    set(PYCBC_C_MOD_FULL_SUFFIX "${PYCBC_C_MOD_SHARED_LIB_EXT}")
  else()
    set(PYCBC_C_MOD_FULL_SUFFIX ".abi3${PYCBC_C_MOD_SHARED_LIB_EXT}")
  endif()
else()
  set(PYCBC_C_MOD_FULL_SUFFIX "${PYCBC_C_MOD_SUFFIX}")
endif()

target_include_directories(
  pycbc_core PRIVATE SYSTEM
  "${COUCHBASE_CXX_BINARY_DIR}/generated"
  "${COUCHBASE_CXX_SOURCE_DIR}"
  "${COUCHBASE_CXX_SOURCE_DIR}/third_party/cxx_function"
  "${COUCHBASE_CXX_SOURCE_DIR}/third_party/expected/include")

set(COUCHBASE_CXX_CLIENT_TARGET couchbase_cxx_client_static_intermediate)

if(WIN32)
  # Python3_LIBRARIES is the version-specific python3XY.lib. Linking it in a stable-ABI
  # build takes precedence over the python3.lib that pyconfig.h's MSVC auto-link pragma
  # selects under Py_LIMITED_API, which would bind an abi3-tagged .pyd to one interpreter
  # version. Prefer python3.lib explicitly, and fall back to the pragma if it isn't found.
  if(PYCBC_PY_LIMITED_API)
    find_library(
      PYCBC_PYTHON3_SABI_LIBRARY
      NAMES python3
      HINTS ${Python3_LIBRARY_DIRS}
      NO_DEFAULT_PATH)
    set(PYCBC_PYTHON_LINK_LIBRARIES "")
    if(PYCBC_PYTHON3_SABI_LIBRARY)
      set(PYCBC_PYTHON_LINK_LIBRARIES ${PYCBC_PYTHON3_SABI_LIBRARY})
    endif()
    message(STATUS "PYCBC_PYTHON3_SABI_LIBRARY=${PYCBC_PYTHON3_SABI_LIBRARY}")
  else()
    set(PYCBC_PYTHON_LINK_LIBRARIES ${Python3_LIBRARIES})
  endif()
  target_link_libraries(
    pycbc_core PRIVATE
    ${COUCHBASE_CXX_CLIENT_TARGET}
    ${PYCBC_PYTHON_LINK_LIBRARIES}
    asio
    Microsoft.GSL::GSL
    taocpp::json
    spdlog::spdlog
    hdr_histogram_static)
else()
  target_link_libraries(
    pycbc_core PRIVATE
    ${COUCHBASE_CXX_CLIENT_TARGET}
    asio
    Microsoft.GSL::GSL
    taocpp::json
    spdlog::spdlog
    hdr_histogram_static)
  if(APPLE)
    target_link_options(
      pycbc_core
      PRIVATE
      -undefined
      dynamic_lookup)
  endif()
endif()

if(CMAKE_VERBOSE_MAKEFILE)
  target_link_options(pycbc_core PRIVATE -v)
endif()

if(NOT USE_STATIC_BORINGSSL)
  target_link_libraries(pycbc_core PUBLIC ${OPENSSL_LIBRARIES})
endif()

# PyInit__core carries its own visibility("default") via PyMODINIT_FUNC, so hiding
# everything else keeps the module's exported surface to the one symbol CPython needs.
# No-op on MSVC, which exports nothing without __declspec(dllexport).
set_target_properties(
  pycbc_core
  PROPERTIES PREFIX ""
             OUTPUT_NAME ${PYCBC_C_MOD_OUTPUT_NAME}
             SUFFIX ${PYCBC_C_MOD_FULL_SUFFIX}
             C_VISIBILITY_PRESET hidden
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN ON)

# The extension module is the only build artifact that belongs in the Python package
# directory, so the directory is set on the target rather than through a global
# CMAKE_<category>_OUTPUT_DIRECTORY, which applies to every target in the build tree
# (PYCBC-1878).  The category differs by platform: on Windows a DLL is RUNTIME, which also
# covers executables, and everywhere else the module is LIBRARY.  PDB_OUTPUT_DIRECTORY
# otherwise follows the target's own output directory, which would put the symbols back into
# the package directory and therefore into the wheel.
#
# The path is used as passed.  These properties accept native separators, and
# file(TO_CMAKE_PATH) splits on the host's path-list separator, so it is not a safe
# normalizer for a single path.
if(PYCBC_MODULE_OUTPUT_DIRECTORY)
  message(STATUS "PYCBC_MODULE_OUTPUT_DIRECTORY=${PYCBC_MODULE_OUTPUT_DIRECTORY}")
  if(WIN32)
    set(PYCBC_MOD_DIR_CATEGORY RUNTIME)
  else()
    set(PYCBC_MOD_DIR_CATEGORY LIBRARY)
  endif()
  set_target_properties(
    pycbc_core
    PROPERTIES ${PYCBC_MOD_DIR_CATEGORY}_OUTPUT_DIRECTORY "${PYCBC_MODULE_OUTPUT_DIRECTORY}"
               PDB_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  # Multi-config generators (Visual Studio, Xcode) append the configuration name unless the
  # per-config property is set.  Single-config generators leave CMAKE_CONFIGURATION_TYPES
  # empty, so this loop is a no-op there and the plain property above applies.
  foreach(pycbc_config IN LISTS CMAKE_CONFIGURATION_TYPES)
    string(TOUPPER "${pycbc_config}" pycbc_config_uc)
    set_target_properties(
      pycbc_core
      PROPERTIES ${PYCBC_MOD_DIR_CATEGORY}_OUTPUT_DIRECTORY_${pycbc_config_uc} "${PYCBC_MODULE_OUTPUT_DIRECTORY}"
                 PDB_OUTPUT_DIRECTORY_${pycbc_config_uc} "${CMAKE_BINARY_DIR}")
  endforeach()
else()
  # Nothing else places the module, and the CMake-driven build_ext does not check, so the
  # wheel would be packed without an extension and fail at import instead of here.
  message(
    WARNING
      "PYCBC_MODULE_OUTPUT_DIRECTORY is not set: ${PYCBC_C_MOD_OUTPUT_NAME}${PYCBC_C_MOD_FULL_SUFFIX} will be left in the build tree, not the package directory"
  )
endif()
