#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>

#include "common.h"
#include "timer.h"
#include "fileread.h"

int main(int argc, char **argv) {
    char *filename = NULL;
    int threadCount = 4;
    int bufferSize = 50;
    int verbosity = 0;
    int c;
    while ((c = getopt(argc, argv, "f:j:s:v")) != -1) {
        switch (c) {
            case 'f':
                filename = optarg;
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
            case 'v':
                verbosity++;
                break;
            case 's': {
                char *end;
                bufferSize = (int) strtol(optarg, &end, 10);
                if (end != optarg + strlen(optarg)) {
                    bufferSize = 50;
                    printf("Invalid buffer size specified. Defaulting to 50\n");
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

    fstat(file, &buffer);
    long size = buffer.st_size;
    close(file);
    if (verbosity > 1) {
        printf("Parsing %s file of size %li using %d threads.\n", filename, size, threadCount);

    } else if (verbosity > 0) {
        printf("Reading file %s\n", filename);
    }

    timekeeper_t totalTimer;
    starttimer(&totalTimer);

    struct job jobs[threadCount];
    parseTokensFromFile(filename, threadCount, bufferSize, size, jobs);

    stoptimer(&totalTimer);

    long totalLines = 0;
    for (int i = 0; i < threadCount; i++) {
        struct parseresult result = jobs[i].result;
        if (verbosity > 1) {
            printf("Parsed %li tokens and %li lines in job %d, skipped %d lines due to errors\n", result.tokens / 2,
                   result.lines, jobs[i].jobId, jobs[i].result.invalid);
        }
        totalLines += result.lines;
    }
    if (verbosity > 0) {
        printf("Parsed a total of %li lines in %li.%03li seconds.\n", totalLines, totalTimer.seconds,
               totalTimer.nanos / 1000000);
    }
    printf("%li, %li.%03li\n", totalLines, totalTimer.seconds, totalTimer.nanos / 1000000);
    return 0;
}

