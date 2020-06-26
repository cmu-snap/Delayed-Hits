cmake_minimum_required(VERSION 3.10)
project(DelayedHits)

# Output targets
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Common includes
include_directories(common/include)
include_directories(SYSTEM libs/cereal-1.3.0/include)

# Boost
find_package(Boost REQUIRED COMPONENTS program_options)
link_libraries(Boost::program_options)

# C++ STD version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Standard libs, CXX flags
link_libraries(m pthread)
add_compile_options(-m64 -O3 -g -pedantic-errors -Wall
                    -Wextra -Werror -Wno-implicit-fallthrough)
# Submodules
add_subdirectory(belatedly)
add_subdirectory(common)
add_subdirectory(caching)
