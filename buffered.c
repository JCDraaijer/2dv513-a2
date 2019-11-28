//
// Created by jona on 2019-11-28.
//

#include "buffered.h"
#include "timer.h"
#include "jsmn.h"
#include <stdio.h>
#include <pthread.h>

void *parseTokens(void *collection) {
    struct job *pointers = collection;
    //printf("Parsing tokens from %li characters in job %d.\n", pointers->endP - pointers->startP, pointers->jobId);

    long totalTokenCount = 0;

    char *buffer = pointers->startP;
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
    } while (tokenCount > 0 && *buffer != EOF && *buffer != 0 && buffer < pointers->endP);
    stoptimer(&timer);
    if (totalTokenCount > 0) {
        //printf("Parsed %li tokens and %d lines in job %d.\n", totalTokenCount / 2, lines, pointers->jobId);
    } else {
        printf("Failed to parse tokens in job %d.\n", pointers->jobId);
    }
    pointers->result.lines = lines;
    pointers->result.time.tv_sec = timer.seconds;
    pointers->result.time.tv_nsec = timer.nanos;

    int exit = 1;
    pthread_exit(&exit);
}

void parseFromBuffered(char *buffer, long size, int threadCount, struct job *jobs) {
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
        pthread_create(&threads[i], NULL, &parseTokens, &jobs[i]);
    }
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }
}