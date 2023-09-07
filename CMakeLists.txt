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

cmake_minimum_required(VERSION 3.18)
# needed for CMAKE_MSVC_RUNTIME_LIBRARY
cmake_policy(SET CMP0091 NEW)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)

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

if(PYTHON_VERSION_EXACT)
  set(Python_FIND_VIRTUALENV FIRST)
  message("finding python version ${PYTHON_VERSION_EXACT}")
else()
  set(PYTHON_VERSION_EXACT 3.6)
endif()
find_package(Python3 ${PYTHON_VERSION_EXACT} COMPONENTS Interpreter Development.Module)

if(WIN32)
  set(PYCBC_C_MOD_SUFFIX ".pyd")
else()
  set(PYCBC_C_MOD_SUFFIX ".so")
endif()

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
        # default version is currently 1.1.1g (see setup.py)
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
        execute_process(
          COMMAND ${BREW_COMMAND} --prefix openssl@1.1
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
  include_directories(${OPENSSL_INCLUDE_DIR})
else()
  set(COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL
      OFF
      CACHE BOOL "" FORCE)
  set(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL
      ON
      CACHE BOOL "" FORCE)
endif()

set(COUCHBASE_CXX_CLIENT_PYTHON_WARNINGS
    ON
    CACHE INTERNAL "")
set(COUCHBASE_CXX_CLIENT_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES
    OFF
    CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_TOOLS
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

# handle CPM cache dir
if(DEFINED COUCHBASE_CXX_CPM_CACHE_DIR AND NOT COUCHBASE_CXX_CPM_CACHE_DIR STREQUAL "")
  set(CPM_SOURCE_CACHE "${COUCHBASE_CXX_CPM_CACHE_DIR}")
endif()

if(DEFINED CPM_SOURCE_CACHE)
  message(STATUS "CPM_SOURCE_CACHE=${CPM_SOURCE_CACHE}")
endif()

add_subdirectory(deps/couchbase-cxx-client)

set(COUCHBASE_CXX_BINARY_DIR "${CMAKE_BINARY_DIR}/deps/couchbase-cxx-client")
message(STATUS "COUCHBASE_CXX_BINARY_DIR=${COUCHBASE_CXX_BINARY_DIR}")
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
  "src/management/*.cxx"
  "src/transactions/*.cxx")
add_library(pycbc_core SHARED ${SOURCE_FILES})

target_include_directories(pycbc_core PRIVATE "${CB_CXX_DIR}/include" "${CB_CXX_DIR}/third_party/asio/asio/include")

if(WIN32)
  target_link_libraries(
    pycbc_core
    PRIVATE couchbase_cxx_client
            ${Python3_LIBRARIES}
            asio
            Microsoft.GSL::GSL
            taocpp::json)
else()
  target_link_libraries(pycbc_core PRIVATE couchbase_cxx_client asio Microsoft.GSL::GSL taocpp::json)
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

set_target_properties(
  pycbc_core
  PROPERTIES PREFIX ""
             OUTPUT_NAME pycbc_core
             SUFFIX ${PYCBC_C_MOD_SUFFIX})
