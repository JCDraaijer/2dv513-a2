//
// Created by jona on 2019-11-28.
//

#include "buffered.h"
#include "timer.h"
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include "jsmn.h"

void *parseTokensBuffered(void *collection) {
    struct job *job = collection;
    printf("Parsing tokens from %li characters in job %d.\n", job->endP - job->startP, job->jobId);

    long totalTokenCount = 0;

    char *buffer = job->startP;
    long tokenCount = 0;
    int lines = 0;
    timekeeper_t timer;
    starttimer(&timer);
    do {
        int start = 0;
        int end = 0;

        while (buffer[end++] != '\n');

        jsmntok_t tokens[50];
        jsmn_parser parser;
        jsmn_init(&parser);

        tokenCount = jsmn_parse(&parser, buffer + start, end - 1 - start, tokens, 50);
        totalTokenCount += tokenCount - 1;
        lines++;
        buffer += end;
    } while (tokenCount > 0 && *buffer != EOF && *buffer != 0 && buffer < job->endP);
    stoptimer(&timer);
    job->result.tokens = totalTokenCount;
    job->result.lines = lines;
    job->result.time.tv_sec = timer.seconds;
    job->result.time.tv_nsec = timer.nanos;
    int exit = 1;
    pthread_exit(&exit);
}

void parseFromBuffered(char *filename, long size, int threadCount, struct job *jobs) {

    int file = open(filename, O_RDONLY);

    timekeeper_t readTimer;
    starttimer(&readTimer);

    char *buffer = malloc(sizeof(char) * size);

    long bytesRead;
    long totalRead = 0;
    while ((bytesRead = read(file, buffer + totalRead, size)) != 0) {
        totalRead += bytesRead;
    }

    close(file);

    stoptimer(&readTimer);
    double mbs = ((double) totalRead / (double) (readTimer.seconds * 1000000000 + readTimer.nanos)) * 1000;
    printf("Read %li bytes in %li.%03li seconds, at ~%0.2lfMb/s\n", totalRead, readTimer.seconds,
           readTimer.nanos / 1000000,
           mbs);

    printf("Parsing buffer using %d threads.\n", threadCount);

    pthread_t threads[threadCount];

    long proxSize = size / threadCount;
    char *currentPointer = buffer;
    for (int i = 0; i < threadCount; i++) {
        jobs[i].jobId = i;
        if (i == threadCount - 1) {
            jobs[i].startP = currentPointer;
            jobs[i].endP = buffer + size;
        } else {
            jobs[i].startP = currentPointer;
            jobs[i].endP = jobs[i].startP + proxSize;
            while (*jobs[i].endP++ != '\n');
            currentPointer = jobs[i].endP;
        }
    }

    for (int i = 0; i < threadCount; i++) {
        pthread_create(&threads[i], NULL, &parseTokensBuffered, &jobs[i]);
    }
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }
    free(buffer);
}