//
// Created by jona on 2019-12-08.
//

#ifndef SQLTEST_COMMON_H
#define SQLTEST_COMMON_H

#include "timer.h"

typedef struct {
    long lines;
    long tokens;
    long characters;
    int invalid;
    struct timespec time;
} parseresult_t;
#endif //SQLTEST_COMMON_H
