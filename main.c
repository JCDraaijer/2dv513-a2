#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>

#include "timer.h"
#include "buffered.h"
#include "common.h"

int main(int argc, char **argv) {
    char *filename = NULL;
    int parse = 1;
    int threadCount = 8;
    int c;
    while ((c = getopt(argc, argv, "f:nj:")) != -1) {
        switch (c) {
            case 'f':
                filename = optarg;
                break;
            case 'n':
                parse = 0;
                break;
            case 'j': {
                char *end;
                threadCount = (int) strtol(optarg, &end, 10);
                if (end != optarg + strlen(optarg)) {
                    threadCount = 4;
                    printf("Invalid thread count specified. Defaulting to 4\n");
                }
                break;
            }
            default:
                break;
        }
    }

    if (filename == NULL) {
        printf("The -f argument is required\n");
        return 1;
    }

    struct stat buffer;

    int file = open(filename, O_RDONLY);

    if (file < 0) {
        printf("Could not open file.\n");
        return 1;
    }


    printf("Reading file %s\n", filename);

    timekeeper_t timer;
    starttimer(&timer);

    fstat(file, &buffer);
    long size = buffer.st_size;

    char *strBuf = (char *) malloc(sizeof(char) * size);

    long bytesRead;
    long totalRead = 0;
    while ((bytesRead = read(file, strBuf + totalRead, size)) != 0) {
        totalRead += bytesRead;
    }
    stoptimer(&timer);
    double mbs = ((double) totalRead / (double) (timer.seconds * 1000000000 + timer.nanos)) * 1000;
    printf("Read %li bytes in %li.%03li seconds, at ~%0.2lfMb/s\n", totalRead, timer.seconds, timer.nanos / 1000000,
           mbs);


    timekeeper_t totalTimer;
    starttimer(&totalTimer);
    long totalLines = 0;

    struct job *jobs = malloc(sizeof(struct job) * threadCount);
    if (parse) {
        parseFromBuffered(strBuf, size, threadCount, jobs);
    }

    stoptimer(&totalTimer);

    for (int i = 0; i < threadCount; i++) {
        totalLines += jobs[i].result.lines;
    }

    printf("Parsed a total of %li lines in %li.%03li seconds.\n", totalLines, totalTimer.seconds,
           totalTimer.nanos / 1000000);

    free(strBuf);
    return 0;
}

