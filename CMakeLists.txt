# Copyright 2020 The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.10.2)

# Build protoc with a host configuration. We need to run it on the host to
# create our proto files.
set(CMAKE_HOST_ARGS
  -DBUILD_SHARED_LIBS:BOOL=${BUILD_SHARED_LIBS}
  -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
  -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
  -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
  -DCMAKE_GENERATOR:STRING=${CMAKE_GENERATOR}
  -DCMAKE_MAKE_PROGRAM:FILEPATH=${CMAKE_MAKE_PROGRAM})
set(Protobuf_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../protobuf")
set(Protobuf_HOST_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf-host")
set(Protobuf_TARGET_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf-target")
add_custom_command(
    OUTPUT "${Protobuf_HOST_BINARY_DIR}/protoc"

    # Run another cmake invocation to configure the protobuf project
    COMMAND "${CMAKE_COMMAND}"
      ${CMAKE_HOST_ARGS}
      -H${Protobuf_SOURCE_DIR}/cmake
      -B${Protobuf_HOST_BINARY_DIR}
      -Dprotobuf_BUILD_TESTS:BOOL=OFF

    # Run the actual build tool (ninja) to compile protoc for the host
    COMMAND "${CMAKE_MAKE_PROGRAM}" protoc
    WORKING_DIRECTORY ${Protobuf_HOST_BINARY_DIR}
)
message(STATUS "Building protoc at: ${Protobuf_HOST_BINARY_DIR}/protoc")

# Glob Icing proto sources. Results look like this: icing/proto/document.proto
file(
    GLOB_RECURSE
    Icing_PROTO_FILES
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    "icing/*.proto")
message(STATUS "Icing_PROTO_FILES=${Icing_PROTO_FILES}")

# Run protoc on Icing_PROTO_FILES to generate pb.cc and pb.h files
set(Icing_PROTO_GEN_DIR "${PROTO_GENERATED_FILES_BASE_DIR}/cpp")
file(MAKE_DIRECTORY ${Icing_PROTO_GEN_DIR})
foreach(FILE ${Icing_PROTO_FILES})
    # Find the name of the proto file without the .proto extension
    string(REGEX REPLACE "\.proto$" "" FILE_NOEXT ${FILE})
    list(APPEND Icing_PROTO_SOURCES
      "${Icing_PROTO_GEN_DIR}/${FILE_NOEXT}.pb.cc"
      "${Icing_PROTO_GEN_DIR}/${FILE_NOEXT}.pb.h")
    add_custom_command(
        OUTPUT "${Icing_PROTO_GEN_DIR}/${FILE_NOEXT}.pb.cc"
          "${Icing_PROTO_GEN_DIR}/${FILE_NOEXT}.pb.h"
        COMMAND "${Protobuf_HOST_BINARY_DIR}/protoc"
          --proto_path ${CMAKE_CURRENT_SOURCE_DIR}
          --cpp_out ${Icing_PROTO_GEN_DIR}
          ${FILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS "${Protobuf_HOST_BINARY_DIR}/protoc"
    )
endforeach()
message(STATUS "Icing_PROTO_SOURCES=${Icing_PROTO_SOURCES}")

# Compile libprotobuf
set(protobuf_BUILD_TESTS OFF CACHE BOOL "")
add_subdirectory("${Protobuf_SOURCE_DIR}/cmake" ${Protobuf_TARGET_BINARY_DIR})

# Compile libandroidicu
set(ICU_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../icu/libandroidicu")
set(ICU_TARGET_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/icu-target")
add_subdirectory(${ICU_SOURCE_DIR} ${ICU_TARGET_BINARY_DIR})

# Glob Icing C++ sources
# TODO: When supporting cmake v3.12 or higher, use CONFIGURE_DEPENDS in the glob
# below so that cmake knows when to re-generate the makefiles.
file(
    # List files recursively
    GLOB_RECURSE
    # Store into a variable of this name
    Icing_CC_SOURCES
    # Return paths that are relative to the project root
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    # Glob expressions
    icing/*.cc icing/*.h
)
# Exclude the same types of files as Android.bp. See the comments there.
list(FILTER Icing_CC_SOURCES EXCLUDE REGEX "^icing/.*_test\.cc$")
list(FILTER Icing_CC_SOURCES EXCLUDE REGEX "^icing/.*_benchmark\.cc$")
list(FILTER Icing_CC_SOURCES EXCLUDE REGEX "^icing/testing/.*$")
list(FILTER Icing_CC_SOURCES EXCLUDE REGEX "^icing/tools/.*$")
message(STATUS "Icing_CC_SOURCES=${Icing_CC_SOURCES}")

add_library(
    # .so name
    icing

    # Shared or static
    SHARED

    # Provides a relative path to your source file(s).
    ${Icing_CC_SOURCES}
    ${Icing_PROTO_SOURCES}
)
target_include_directories(icing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(icing PRIVATE ${Icing_PROTO_GEN_DIR})
target_include_directories(icing PRIVATE "${Protobuf_SOURCE_DIR}/src")
target_include_directories(icing PRIVATE "${ICU_SOURCE_DIR}/include")
target_link_libraries(icing protobuf::libprotobuf libandroidicu log)
