#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>

#include "common.h"
#include "timer.h"
#include "fileread.h"

static struct option long_options[] =
        {"filename", required_argument, NULL, 'f',
         "database", required_argument, NULL, 'd',
         "jobs", required_argument, NULL, 'j',
         "buffer-size", required_argument, NULL, 's',
         "query-lines", required_argument, NULL, 'q',
         "verbose", no_argument, NULL, 'v',
         "help", no_argument, NULL, 'h',

         0, 0, 0, 0};

int main(int argc, char **argv) {
    char *filename = NULL;
    int threadCount = 4;
    int bufferSize = 50;
    int verbosity = 0;
    int queryLines = 200;
    char *database = NULL;
    int c;
    int option_index;
    opterr = 0;
    while ((c = getopt_long(argc, argv, "f:d:j:s:q:vh", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h': {
                printf("Available options:\n");
                printf("--filename, -f [filename]     Specify the filename to read the Reddit comments in JSON format from. (REQUIRED)\n");
                printf("--database, -d [filename]     The filename of the sqlite database to write the values to. (REQUIRED)\n");
                printf("--jobs, -j                    The amount of threads to be used for parsing. (Default: 4)\n");
                printf("--buffer-size, -s             Specify the size of the file buffer used by each thread in KB. (Default: 50)\n");
                printf("--query-lines, -q             Specify how many lines of each file should be sent per query. Big values can impact performance (Default: 200)\n");
                printf("                              Increase in case of database bottleneck. Decrease in case of CPU bottleneck.\n");
                printf("--verbose, -v                 Specify the verbosity of the program. Use multiple times for more verbosity\n");
                printf("--help, -h                    Print this menu.\n");
                return EXIT_SUCCESS;
            }
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
            case 'q': {
                char *end;
                queryLines = (int) strtol(optarg, &end, 10);
                if (end != optarg + strlen(optarg)) {
                    bufferSize = 200;
                    printf("Invalid buffer size specified. Defaulting to 50\n");
                }
                break;
            }
            case 'd':
                database = optarg;
                break;
            case '?':
                fprintf(stderr, "Invalid argument \"%s\". Use --help for help.\n", argv[optind - 1]);
                return EXIT_FAILURE;
            default:
                break;
        }
    }

    if (filename == NULL) {
        printf("The filename argument is required. (See --help for help)\n");
        return EXIT_FAILURE;
    }
    if (database == NULL) {
        printf("The database argument is required. (See --help for help)\n");
        return EXIT_FAILURE;
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
        printf("Parsing file %s of size %li using %d threads into database %s\n", filename, size, threadCount, database);

    } else if (verbosity > 0) {
        printf("Parsing file %s into database %s\n", filename, database);
    }

    timekeeper_t totalTimer;
    starttimer(&totalTimer);

    struct job jobs[threadCount];
    parseTokensFromFile(filename, database, threadCount, bufferSize, size, jobs, queryLines);

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

