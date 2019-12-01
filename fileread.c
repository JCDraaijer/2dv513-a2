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
    int bufferSize = (1024 * job->bufferSize) + 4096;

    //TODO figure out why larger buffer sizes make the program grind to a halt
    char *buffer = malloc(sizeof(char) * bufferSize + 2048);

    long bufferIndex = 0;
    long *totalLines = malloc(sizeof(long));

    timekeeper_t timer;
    starttimer(&timer);
    sqlite3 *db;
    int connRes;
    connRes = sqlite3_open(job->database, &db);
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
                }
            }
        }
        sqlite_insert(buffer, buffer + bufferIndex + 1, &totalTokenCount, totalLines, db, job->queryLines);
    } while (lseek(fd, 0, SEEK_CUR) < job->end);
    sqlite3_close(db);
    stoptimer(&timer);

    free(buffer);
    close(fd);

    job->result.tokens = totalTokenCount;
    job->result.lines = *totalLines;
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

void
parseTokensFromFile(char *filename, char *database, int threadCount, int bufferSize, long maxSize, struct job *jobs,
                    int queryLines) {
    int fd = open(filename, O_RDONLY);

    long proxSize = maxSize / threadCount;
    long start = 0;
    long end = proxSize;

    pthread_t threads[threadCount];
    for (int i = 0; i < threadCount; i++) {
        jobs[i].bufferSize = bufferSize;
        jobs[i].filename = filename;
        jobs[i].database = database;
        jobs[i].jobId = i;
        jobs[i].result.invalid = 0;
        jobs[i].queryLines = queryLines;
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
