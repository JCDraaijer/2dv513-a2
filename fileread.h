//
// Created by jona on 2019-11-28.
//

#ifndef SQLTEST_FILEREAD_H
#define SQLTEST_FILEREAD_H

#include "common.h"

void parseTokensFromFile(char *, char *, int, int, long, struct job *, int queryLines);

void *parseTokens(void *);

long findNextNewline(int, long);

#endif //SQLTEST_FILEREAD_H
