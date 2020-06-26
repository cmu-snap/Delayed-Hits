cmake_minimum_required(VERSION 3.10)
project(Caching)

# Library
link_libraries(hashing)

# Sources
include_directories(include)
##### Offline Policies #####
add_executable(cache_belady src/cache_belady.cpp)
add_executable(cache_belady_aggdelay src/cache_belady_aggdelay.cpp)
##### Online Policies #####
add_executable(cache_lru src/cache_lru.cpp)
add_executable(cache_arc src/cache_arc.cpp)
add_executable(cache_lhd src/cache_lhd.cpp)
add_executable(cache_lru_aggdelay src/cache_lru_aggdelay.cpp)
add_executable(cache_arc_aggdelay src/cache_arc_aggdelay.cpp)
add_executable(cache_lhd_aggdelay src/cache_lhd_aggdelay.cpp)
