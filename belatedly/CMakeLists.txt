cmake_minimum_required(VERSION 3.10)
project(Belatedly)

# Source
include_directories(include)
add_executable(belatedly src/belatedly.cpp)

##### Gurobi #####

# Check environment variables
if(NOT DEFINED ENV{GUROBI_HOME})
    message(FATAL_ERROR "GUROBI_HOME is not set.")
else()
    message(STATUS "GUROBI_HOME points to " $ENV{GUROBI_HOME})
endif()

if(NOT DEFINED ENV{GRB_LICENSE_FILE})
    message(FATAL_ERROR "GRB_LICENSE_FILE is not set")
else()
    message(STATUS "GRB_LICENSE_FILE points to " $ENV{GRB_LICENSE_FILE})
endif()

find_path(Gurobi_Inc NAMES gurobi_c++.h
                     PATHS "$ENV{GUROBI_HOME}/include/")
if(NOT Gurobi_Inc)
    message(FATAL_ERROR "Could not find gurobi_c++.h")
else()
    message(STATUS "  Found gurobi_c++.h in " ${Gurobi_Inc})
endif()

find_library(Gurobi_Lib NAMES gurobi81
                        PATHS "$ENV{GUROBI_HOME}/lib/")
if(NOT Gurobi_Lib)
    message(FATAL_ERROR "Could not find libgurobi81.so")
else()
    message(STATUS "  Found gurobi81 at " ${Gurobi_Lib})
endif()

find_library(Gurobi_CXX_Lib NAMES gurobi_c++
                            PATHS "$ENV{GUROBI_HOME}/lib/")
if(NOT Gurobi_CXX_Lib)
    message(FATAL_ERROR "Could not find libgurobi_c++.a")
else()
    message(STATUS "  Found gurobi_c++ at " ${Gurobi_CXX_Lib})
endif()

include_directories(SYSTEM ${Gurobi_Inc})
target_link_libraries(belatedly ${Gurobi_CXX_Lib} ${Gurobi_Lib})
