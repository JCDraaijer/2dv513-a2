//
// Created by jona on 2019-12-08.
//

#ifndef SQLTEST_REDO_H
#define SQLTEST_REDO_H

#include "common.h"

typedef struct {
    char *buffer;
    unsigned int bufferSize;
    unsigned int length;
} string_t;

int redo(char *buffer, const char *endP, long *totalTokens, long *totalLines, MYSQL *db, int queryLines);

void stringinit(string_t *string, unsigned int size);

void stringreset(string_t *string);

void stringfree(string_t *string);

void stringfit(string_t *string, unsigned int length);

void stringsetsize(string_t *string, unsigned int size);

void stringcat(string_t *dst, const string_t *src);

#endif //SQLTEST_REDO_H
