//
// Created by jona on 2019-11-28.
//

#ifndef SQLTEST_COMMON_H
#define SQLTEST_COMMON_H

#include <time.h>
#include <sqlite3.h>

struct parseresult {
    long lines;
    long tokens;
    int invalid;
    struct timespec time;
};

struct job {
    union {
        long start;
        char *startP;
    };
    union {
        long end;
        char *endP;
    };
    int jobId;
    char *filename;
    int bufferSize;
    struct parseresult result;
    sqlite3 *db;
};

int sqlite_insert(char *buffer, const char *endP, long *totalTokens, long *totalLines, sqlite3 *db);
#endif //SQLTEST_COMMON_H
