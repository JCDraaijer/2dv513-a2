cmake_minimum_required(VERSION 3.14)
project(sqltest)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_STANDARD 99)

find_package(Threads REQUIRED)
add_executable(sqltest main.c jsmn.h jsmn.c timer.c timer.h sql.c sql.h)
target_link_libraries(sqltest Threads::Threads mysqlclient sqlite3)
