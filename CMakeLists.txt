cmake_minimum_required(VERSION 3.14)
project(sqltest)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_C_STANDARD 99)

find_package(Threads REQUIRED)
add_executable(sqltest main.c jsmn.h jsmn.c timer.c timer.h redo.c redo.h common.h)
target_link_libraries(sqltest Threads::Threads mysqlclient)
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/cmake-build-debug/${CMAKE_PROJECT_NAME} DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/build)