//
// Created by jona on 2019-11-28.
//

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#include "fileread.h"
#include "timer.h"
#include "jsmn.h"

void *parseTokens(void *collection) {
    struct job *job = collection;
    long totalTokenCount = 0;

    int fd = open(job->filename, O_RDONLY);
    lseek(fd, job->start, SEEK_CUR);
    int bufferSize = 1024 * 1024 * job->bufferSize;

    char *buffer = malloc(sizeof(char) * bufferSize);

    long bufferIndex = 0;
    int lines = 0;
    long tokenCount = 0;

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
                if (bufferIndex >= bufferSize) {
                    buffer = realloc(buffer, sizeof(char) * (bufferSize + 2048));
                    bufferSize += 2048;
                }
            }
        }
        char *currentBuffer = buffer;
        do {
            int start = 0;
            int end = 0;

            while (currentBuffer[end++] != '\n');

            jsmntok_t tokens[50];
            jsmn_parser parser;
            jsmn_init(&parser);

            tokenCount = jsmn_parse(&parser, currentBuffer + start, end - 1 - start, tokens, 50);
            totalTokenCount += tokenCount - 1;
            lines++;
            currentBuffer += end;
        } while (currentBuffer < (buffer + bufferIndex));
    } while (lseek(fd, 0, SEEK_CUR) < job->end);
    free(buffer);
    close(fd);

    timekeeper_t timer;
    starttimer(&timer);
    stoptimer(&timer);
    if (totalTokenCount > 0) {
        //printf("Parsed %li tokens and %d lines in job %d.\n", totalTokenCount / 2, lines, job->jobId);
    } else {
        printf("Failed to parse tokens in job %d.\n", job->jobId);
    }
    job->result.lines = lines;
    job->result.time.tv_sec = timer.seconds;
    job->result.time.tv_nsec = timer.nanos;

    int exit = 1;
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
    // printf("Parsing file using %d threads.\n", threadCount);

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
