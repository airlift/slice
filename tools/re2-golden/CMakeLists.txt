cmake_minimum_required(VERSION 3.20)
project(re2_golden LANGUAGES CXX)

if(NOT DEFINED RE2_DIR)
  message(FATAL_ERROR "RE2_DIR is required")
endif()
if(NOT DEFINED ABSL_DIR)
  message(FATAL_ERROR "ABSL_DIR is required")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(ABSL_PROPAGATE_CXX_STD ON)
set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(RE2_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory("${ABSL_DIR}" absl EXCLUDE_FROM_ALL)
add_subdirectory("${RE2_DIR}" re2 EXCLUDE_FROM_ALL)

add_executable(re2_golden
  re2_golden.cc
  random_corpus.cc
  "${RE2_DIR}/re2/testing/regexp_generator.cc"
  "${RE2_DIR}/re2/testing/string_generator.cc")
target_link_libraries(re2_golden PRIVATE re2::re2)

add_executable(re2_count_diagnostics re2_count_diagnostics.cc)
target_link_libraries(re2_count_diagnostics PRIVATE re2::re2)

add_executable(re2_capture_diagnostics re2_capture_diagnostics.cc)
target_link_libraries(re2_capture_diagnostics PRIVATE re2::re2)
