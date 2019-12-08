#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <pthread.h>
#include <mysql/mysql.h>
#include "timer.h"
#include "jsmn.h"
#include "redo.h"

#define DEFAULT_BUFFER_SIZE 1024
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_QUERY_LINES 2000

const char *queryStart = "INSERT INTO entries(id, parent_id, link_id, author, body, subreddit_id, subreddit, score, created_utc) VALUES ";

struct job {
    long start;
    long end;
    int jobId;
    char *filename;
    char *username;
    char *password;
    char *database;
    int bufferSize;
    parseresult_t result;
    int queryLines;
};

int sqlInsert(char *buffer, const char *endP, long *totalTokens, long *totalLines, MYSQL *db, int queryLines);

void parseTokensFromFile(char *, char *, char *, char *, int, int, long, struct job *, int queryLines);

void *parseTokens(void *);

long findNextNewline(int, long);

static struct option long_options[] =
        {"filename", required_argument, NULL, 'f',
         "username", required_argument, NULL, 'u',
         "password", required_argument, NULL, 'p',
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
    char *username = NULL;
    char *password = NULL;
    int c;
    int option_index;
    opterr = 0;
    while ((c = getopt_long(argc, argv, "f:u:d:j:s:q:vhp:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h': {
                printf("Available options:\n");
                printf("--filename, -f [filename]     Specify the filename to read the Reddit comments in JSON format from. (REQUIRED)\n");
                printf("--username, -u [username]     The MySQL username to use. (REQUIRED)\n");
                printf("--password, -p [password]     The MySQL password to use. (Alternatively, input it after executing the command)\n");
                printf("--database, -d [filename]     The filename of the MySQL database to write the values to. (REQUIRED)\n");
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
            case 'u':
                username = optarg;
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
            case 'p':
                password = optarg;
                break;
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
    if (username == NULL) {
        printf("The username argument is required. (See --help for help)\n");
        return EXIT_FAILURE;
    }
    if (password == NULL) {
        password = getpass("Password: ");
    }

    MYSQL *db = mysql_init(NULL);
    MYSQL *connRes = mysql_real_connect(db, "localhost", username, password, database, 0, NULL, 0);
    if (connRes == NULL) {
        printf("Couldn't open database %s (err=%s)\n", database, mysql_error(db));
        return EXIT_FAILURE;
    }
    mysql_close(db);

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
    parseTokensFromFile(filename, username, password, database, threadCount, bufferSize, size, jobs, queryLines);
    stoptimer(&totalTimer);

    long totalLines = 0;
    for (int i = 0; i < threadCount; i++) {
        parseresult_t result = jobs[i].result;
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
    long totalLines = 0;

    timekeeper_t timer;
    starttimer(&timer);
    MYSQL *db = mysql_init(NULL);
    MYSQL *connRes = mysql_real_connect(db, "localhost", job->username, job->password, job->database, 0, NULL,
                                        0);
    if (connRes == NULL) {
        printf("Couldn't open database\n");
        mysql_close(db);
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
        redo(buffer, buffer + bufferIndex + 1, &totalTokenCount, &totalLines, db, job->queryLines);
    } while (lseek(fd, 0, SEEK_CUR) < job->end);
    mysql_close(db);
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

void
parseTokensFromFile(char *filename, char *username, char *password, char *database, int threadCount, int bufferSize,
                    long maxSize, struct job *jobs,
                    int queryLines) {
    int fd = open(filename, O_RDONLY);

    long proxSize = maxSize / threadCount;
    long start = 0;
    long end = proxSize;

    pthread_t threads[threadCount];
    for (int i = 0; i < threadCount; i++) {
        jobs[i].bufferSize = bufferSize;
        jobs[i].filename = filename;
        jobs[i].username = username;
        jobs[i].password = password;
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

int sqlInsert(char *buffer, const char *endP, long *totalTokens, long *totalLines, MYSQL *db, int queryLines) {
    char *currentBuffer = buffer;

    long querySize = 8192;
    unsigned long queryLength = 0;
    char *query = malloc(sizeof(char) * querySize);

    int valuesStringSize = 1024;
    char *valuesString = malloc(sizeof(char) * valuesStringSize);

    int tokenCount = 0;
    int linesTotal = 0;
    jsmntok_t tokens[50];
    jsmn_parser parser;

    do {
        int currentLines = 0;
        sprintf(query, "%s", queryStart);
        queryLength = strlen(query);
        do {
            int end = 0;

            while (currentBuffer[end++] != '\n' && end != buffer - endP);
            jsmn_init(&parser);
            tokenCount = jsmn_parse(&parser, currentBuffer, end - 1, tokens, 50);
            if (tokenCount < 0) {
                printf("Error! Invalid entry at line %li. %d chars. Errorcode=%d.\n", *totalLines + 1, end, tokenCount);
                continue;
            }

            for (int i = 2; i < tokenCount - 1; i += 2) {
                jsmntok_t token = tokens[i];
                for (int x = token.start; x < token.end - 1; x++) {
                    if (currentBuffer[x] == '\\' && currentBuffer[x + 1] == '\"') {
                        currentBuffer[x] = '\"';
                    }
                }
            }

            *totalTokens += tokenCount - 1;
            while (end + 256 >= valuesStringSize) {
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
            unsigned long originalQueryLength = queryLength;
            queryLength += sprintf(valuesString,
                                   "(\"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", %.*s, %.*s),\n",
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
            while (querySize <= queryLength + 2) {
                querySize += 1024;
                query = realloc(query, sizeof(char) * querySize);
            }
            strncat(query + originalQueryLength, valuesString, queryLength - originalQueryLength);
            query[queryLength] = 0;
            *totalLines += 1;
            linesTotal++;
            currentLines++;
            currentBuffer += end;
        } while (currentLines < queryLines && currentBuffer < endP);
        query[strlen(query) - 2] = ';';

        int result = 0;

        result = mysql_query(db, query);
        if (result) {
            printf("Error: %s\n", mysql_error(db));
        }

    } while (currentBuffer < endP);
    free(valuesString);
    free(query);
    return 0;
}