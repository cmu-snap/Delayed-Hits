cmake_minimum_required(VERSION 3.10)
project(Common)

# Create a shared library for the hashing code
add_library(hashing STATIC src/MurmurHash3.cpp)
