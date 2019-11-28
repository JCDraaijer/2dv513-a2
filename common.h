//
// Created by jona on 2019-11-28.
//

#ifndef SQLTEST_COMMON_H
#define SQLTEST_COMMON_H

#include <time.h>

struct parseResult {
    long lines;
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
    struct parseResult result;
    int bufferSize;
};

#endif //SQLTEST_COMMON_H
