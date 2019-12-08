//
// Created by jona on 2019-12-08.
//

#ifndef SQLTEST_SQL_H
#define SQLTEST_SQL_H

#define SQLITE_MODE 1
#define MYSQL_MODE 2

typedef struct {
    long lines;
    long tokens;
    long characters;
    int invalid;
    struct timespec time;
} parseresult_t;

typedef struct {
    long start;
    long end;
    int jobId;
    char *filename;
    char *username;
    char *password;
    char *database;
    int bufferSize;
    parseresult_t result;
    int queryLines;
    int mode;
} job_t;

typedef struct {
    char *buffer;
    unsigned int bufferSize;
    unsigned int length;
} string_t;

int sqlinsert(int mode, void *db, char *buffer, int queryLines, const char *endP, string_t *subreddits,
              long *totalTokens,
              long *totalLines);

void stringinit(string_t *string, unsigned int size);

void stringreset(string_t *string);

void stringfree(string_t *string);

void stringfit(string_t *string, unsigned int length);

void stringsetsize(string_t *string, unsigned int size);

void stringcat(string_t *dst, const string_t *src);

int stringContainsWhole(const string_t *string, const char *buffer, jsmntok_t *token);

parseresult_t
constructQueries(int queryLines, string_t *commentQuery, string_t *valuesString, string_t *insertedSubreddits,
                 string_t *subredditQuery, char *buffer, const char *endP);

#endif //SQLTEST_SQL_H
