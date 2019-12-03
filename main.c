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

#define DEFAULT_BUFFER_SIZE 1024
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_QUERY_LINES 2000

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
    int threadCount = DEFAULT_THREAD_COUNT;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    int queryLines = DEFAULT_QUERY_LINES;
    int verbosity = 0;
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
                printf("--jobs, -j                    The amount of threads to be used for parsing. (Default: %d)\n",
                       DEFAULT_THREAD_COUNT);
                printf("--buffer-size, -s             The initial size of the file buffer used by each thread in KB (may grow). (Default: %d)\n",
                       DEFAULT_BUFFER_SIZE);
                printf("--query-lines, -q             The maximum amount of tuples that should be inserted per query. (Default: %d)\n",
                       DEFAULT_QUERY_LINES);
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
                    threadCount = DEFAULT_THREAD_COUNT;
                    printf("Invalid thread count specified. Defaulting to %d\n", DEFAULT_THREAD_COUNT);
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
                    bufferSize = DEFAULT_BUFFER_SIZE;
                    printf("Invalid buffer size specified. Defaulting to %d\n", DEFAULT_BUFFER_SIZE);
                }
                break;
            }
            case 'q': {
                char *end;
                queryLines = (int) strtol(optarg, &end, 10);
                if (end != optarg + strlen(optarg)) {
                    bufferSize = DEFAULT_QUERY_LINES;
                    printf("Invalid buffer size specified. Defaulting to %d\n", DEFAULT_QUERY_LINES);
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

    sqlite3 *db;
    int connRes = sqlite3_open_v2(database, &db, SQLITE_OPEN_READONLY, NULL);
    if (connRes != SQLITE_OK) {
        printf("Couldn't open database (err=%d)\n", connRes);
        return EXIT_FAILURE;
    }
    sqlite3_close_v2(db);

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
        printf("Parsing file %s of size %li using %d threads into database %s\n", filename, size, threadCount,
               database);

    } else if (verbosity > 0) {
        printf("Parsing file %s into database %s\n", filename, database);
    }

    timekeeper_t totalTimer;
    starttimer(&totalTimer);

    struct job jobs[threadCount];
    parseTokensFromFile(filename, database, threadCount, bufferSize, size, jobs, queryLines);
    printCallCount();
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

