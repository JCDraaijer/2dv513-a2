//
// Created by jona on 2019-11-28.
//

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sqlite3.h>

#include "fileread.h"
#include "timer.h"
#include "jsmn.h"

void *parseTokens(void *collection) {
    int exit = 1;
    struct job *job = collection;
    long totalTokenCount = 0;

    int fd = open(job->filename, O_RDONLY);
    lseek(fd, job->start, SEEK_CUR);
    int bufferSize = 1024 * job->bufferSize + 2048;

    char *buffer = malloc(sizeof(char) * bufferSize + 2048);
    char *query = malloc(sizeof(char) * bufferSize + 2048);

    long bufferIndex = 0;
    int totalLines = 0;
//  const char *queryStart = "INSERT INTO entries(id, parent_id, link_id, author, body, subreddit_id, subreddit, score, created_utc) VALUES ";
    const char *queryStart = "INSERT INTO entries VALUES ";

    timekeeper_t timer;
    starttimer(&timer);
    sqlite3 *db;
    int connRes;
    connRes = sqlite3_open("/home/jona/data/2dv513/sqlite.db", &db);
    if (connRes != SQLITE_OK) {
        printf("Couldn't open database\n");
        pthread_exit(&exit);
    }
    do {
        long toRead = bufferSize - 2048;
        long maxRead = job->end - lseek(fd, 0, SEEK_CUR);
        if (toRead > maxRead) {
            toRead = maxRead;
        }
        int bytesRead = read(fd, buffer, toRead);
        bufferIndex = bytesRead - 1;
        if (bytesRead == toRead) {
            while (buffer[bufferIndex] != '\n') {
                read(fd, buffer + ++bufferIndex, 1);
                if (bufferIndex >= bufferSize - 512) {
                    bufferSize += 2048;
                    buffer = realloc(buffer, sizeof(char) * bufferSize);
                    query = realloc(query, sizeof(char) * bufferSize);
                }
            }
        }
        sprintf(query, "%s", queryStart);

        char *currentBuffer = buffer;
        int valuesStringSize = 1024;
        char *valuesString = malloc(sizeof(char) * valuesStringSize);
        int tokenCount = 0;
        int lines = 0;
        do {
            int end = 0;

            while (currentBuffer[end++] != '\n');

            jsmntok_t tokens[43];
            jsmn_parser parser;
            jsmn_init(&parser);
            tokenCount = jsmn_parse(&parser, currentBuffer, end - 1, tokens, 43);
            if (tokenCount != 43) {
                printf("Error! Invalid entry at line %d. %d chars. Errorcode=%d.\n", totalLines + 1, end, tokenCount);
                continue;
            }

            for (int i = 1; i < tokenCount; i++) {
                jsmntok_t token = tokens[i];
                for (int x = token.start; x < token.end - 1; x++) {
                    if (currentBuffer[x] == '\\' && currentBuffer[x + 1] == '\"'){
                        currentBuffer[x] = '\"';
                    }
                }
            }

            totalTokenCount += tokenCount - 1;
            while (end + 128 >= valuesStringSize) {
                valuesStringSize += 1024;
                valuesString = realloc(valuesString, sizeof(char) * valuesStringSize);
            }


            jsmntok_t *id = getbykey("id", currentBuffer, tokens, tokenCount);
            jsmntok_t *parent_id = getbykey("parent_id", currentBuffer, tokens, tokenCount);
            jsmntok_t *linkid = getbykey("link_id", currentBuffer, tokens, tokenCount);
            jsmntok_t *author = getbykey("author", currentBuffer, tokens, tokenCount);
            jsmntok_t *body = getbykey("body", currentBuffer, tokens, tokenCount);
            jsmntok_t *subreddit_id = getbykey("subreddit_id", currentBuffer, tokens, tokenCount);
            jsmntok_t *subreddit = getbykey("subreddit", currentBuffer, tokens, tokenCount);
            jsmntok_t *score = getbykey("score", currentBuffer, tokens, tokenCount);
            jsmntok_t *created_utc = getbykey("created_utc", currentBuffer, tokens, tokenCount);
            int strLen = sprintf(valuesString,
                                 "(\"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", %.*s, %.*s), ",
                                 id->end - id->start,
                                 currentBuffer + id->start,
                                 parent_id->end - parent_id->start,
                                 currentBuffer + parent_id->start,
                                 linkid->end - linkid->start,
                                 currentBuffer + linkid->start,
                                 author->end - author->start,
                                 currentBuffer + author->start,
                                 body->end - body->start,
                                 currentBuffer + body->start,
                                 subreddit_id->end - subreddit_id->start,
                                 currentBuffer + subreddit_id->start,
                                 subreddit->end - subreddit->start,
                                 currentBuffer + subreddit->start,
                                 score->end - score->start,
                                 currentBuffer + score->start,
                                 created_utc->end - created_utc->start,
                                 currentBuffer + created_utc->start);
            strcat(query, valuesString);
            totalLines++;
            lines++;
            currentBuffer += end;
        } while (currentBuffer < (buffer + bufferIndex));
        query[strlen(query) - 2] = ';';
        char *errormsg = malloc(sizeof(char) * 1024);
        int result;
        while ((result = sqlite3_exec(db, query, NULL, 0, &errormsg)) == SQLITE_BUSY);
        if (result != SQLITE_OK) {
            printf("%d, %s\n", result, errormsg);
            printf("%s\n", query);
            totalLines -= lines;
            job->result.invalid += lines;
        }
        printf("Sent a query %d\n", totalLines);
    } while (lseek(fd, 0, SEEK_CUR) < job->end);
    sqlite3_close(db);
    stoptimer(&timer);

    free(buffer);
    close(fd);

    job->result.tokens = totalTokenCount;
    job->result.lines = totalLines;
    job->result.time.tv_sec = timer.seconds;
    job->result.time.tv_nsec = timer.nanos;
    pthread_exit(&exit);
}

long findNextNewline(int fd, long min) {
    long currentIndex = lseek(fd, 0, SEEK_CUR);
    lseek(fd, min, SEEK_CUR);
    long index = 0;
    char character;
    do {
        read(fd, &character, 1);
        index++;
    } while (character != '\n');
    lseek(fd, currentIndex, SEEK_SET);
    return min + index;
}

void parseTokensFromFile(char *filename, int threadCount, int bufferSize, long maxSize, struct job *jobs) {
    int fd = open(filename, O_RDONLY);

    long proxSize = maxSize / threadCount;
    long start = 0;
    long end = proxSize;

    pthread_t threads[threadCount];
    for (int i = 0; i < threadCount; i++) {
        jobs[i].bufferSize = bufferSize;
        jobs[i].filename = filename;
        jobs[i].jobId = i;
        if (i == threadCount - 1) {
            jobs[i].start = start;
            jobs[i].end = maxSize;
        } else {
            jobs[i].start = start;
            jobs[i].end = findNextNewline(fd, end);
            start = jobs[i].end;
            end = jobs[i].end + proxSize;
        }
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_create(&threads[i], NULL, &parseTokens, &jobs[i]);
    }

    close(fd);
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }
}
