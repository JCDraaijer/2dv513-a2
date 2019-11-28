#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>

#include "common.h"
#include "timer.h"
#include "buffered.h"
#include "fileread.h"

int main(int argc, char **argv) {
    char *filename = NULL;
    int parse = 1;
    int threadCount = 8;
    int fullBuffer = 0;
    int c;
    while ((c = getopt(argc, argv, "f:nj:b")) != -1) {
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
            case 'b':
                fullBuffer = 1;
                break;
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

    fstat(file, &buffer);
    long size = buffer.st_size;
    close(file);

    printf("Reading file %s\n", filename);

    timekeeper_t totalTimer;
    starttimer(&totalTimer);

    struct job *jobs = malloc(sizeof(struct job) * threadCount);
    if (parse) {
        if (fullBuffer) {
            parseFromBuffered(filename, size, threadCount, jobs);
        } else {
            parseTokensFromFile(filename, threadCount, size, jobs);
        }
    }

    stoptimer(&totalTimer);

    long totalLines = 0;
    for (int i = 0; i < threadCount; i++) {
        totalLines += jobs[i].result.lines;
    }
    free(jobs);
    printf("Parsed a total of %li lines in %li.%03li seconds.\n", totalLines, totalTimer.seconds,
           totalTimer.nanos / 1000000);
    return 0;
}

