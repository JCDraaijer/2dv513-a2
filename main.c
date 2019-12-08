#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <pthread.h>
#include <mysql/mysql.h>
#include <sqlite3.h>
#include "timer.h"
#include "jsmn.h"
#include "sql.h"

#define DEFAULT_BUFFER_SIZE 1024
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_QUERY_LINES 2000

#define CREATE_NO_TABLES 0
#define CREATE_TABLES 1
#define CREATE_TABLES_CONST 2
#define CREATE_TABLES_DROP 3
#define CREATE_TABLES_CONST_DROP 4

const char *createSubredditsTable = "CREATE TABLE IF NOT EXISTS subreddits(subreddit_id BIGINT, subreddit_name VARCHAR(50))";
const char *createCommentsTable = "CREATE TABLE IF NOT EXISTS comments(comment_id BIGINT,subreddit_id BIGINT,name VARCHAR(20),"
                                  "link_id VARCHAR(20),author VARCHAR(25),body TEXT,time_created INTEGER,parent_id VARCHAR(20),score INTEGER);";
const char *createSubredditsTableConst = "CREATE TABLE IF NOT EXISTS subreddits(subreddit_id BIGINT PRIMARY KEY UNIQUE NOT NULL,subreddit_name VARCHAR(50) UNIQUE NOT NULL);";
const char *createCommentsTableConst = "CREATE TABLE IF NOT EXISTS comments(comment_id BIGINT PRIMARY KEY UNIQUE NOT NULL,subreddit_id BIGINT NOT NULL,name VARCHAR(20) NOT NULL,"
                                       "link_id VARCHAR(20) NOT NULL, author VARCHAR(25) NOT NULL, body TEXT NOT NULL, "
                                       "time_created INTEGER NOT NULL,parent_id VARCHAR(20) NOT NULL,score INTEGER NOT NULL, "
                                       "FOREIGN KEY (subreddit_id) REFERENCES subreddits(subreddit_id));";

void parseTokensFromFile(char *, char *, char *, char *, int, int, long, job_t *, int queryLines, int mode);

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
         "create-tables", no_argument, NULL, 'c',
         "create-tables-const", no_argument, NULL, 128,
         "mode", required_argument, NULL, 'm',
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
    int mode = MYSQL_MODE;
    int createTables = CREATE_NO_TABLES;
    opterr = 0;
    int c;
    int option_index;
    while ((c = getopt_long(argc, argv, "p:f:u:d:j:s:q:vhcm:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h': {
                printf("Available options:\n");
                printf("--filename, -f [filename]     Specify the filename to read the Reddit comments in JSON format from. (REQUIRED)\n");
                printf("--username, -u [username]     The MySQL username to use. (REQUIRED FOR MYSQL)\n");
                printf("--password, -p [password]     The MySQL password to use. (Alternatively, input it after executing the command)\n");
                printf("--database, -d [filename]     The filename of the database (or filename) to write the values to. (REQUIRED)\n");

                printf("--jobs, -j                    The amount of threads to be used for parsing. (Default: %d)\n",
                       DEFAULT_THREAD_COUNT);
                printf("--buffer-size, -s             The initial size of the file buffer used by each thread in KB (may grow). (Default: %d)\n",
                       DEFAULT_BUFFER_SIZE);
                printf("--query-lines, -q             The maximum amount of tuples that should be inserted per query. (Default: %d)\n",
                       DEFAULT_QUERY_LINES);
                printf("--create-tables, -c           Create the tables without constraints (if they do not exist)\n");
                printf("--create-tables-const, -cc    Create the tables with constraints (if they do not exist)\n");
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
            case 'p': {
                password = optarg;
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
            case 'm':
                mode = (int) strtol(optarg, NULL, 10);
                break;
            case 'c':
                createTables++;
                break;
            case 128:
                createTables = CREATE_TABLES_CONST;
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
    void *db;
    if (mode == MYSQL_MODE) {
        db = mysql_init(NULL);
        MYSQL *connRes = mysql_real_connect(db, "localhost", username, password, database, 0, NULL, 0);
        if (connRes == NULL) {
            printf("Couldn't open database %s (err=%s)\n", database, mysql_error(db));
            return EXIT_FAILURE;
        }
        if (createTables == CREATE_TABLES_DROP || createTables == CREATE_TABLES_CONST_DROP) {
            mysql_query(db, "DROP TABLE subreddits;");
            mysql_query(db, "DROP TABLE comments;");
            createTables -= 2;
        }
        if (createTables == CREATE_TABLES) {
            mysql_query(db, createSubredditsTable);
            mysql_query(db, createCommentsTable);
        } else if (createTables == CREATE_TABLES_CONST) {
            mysql_query(db, createSubredditsTableConst);
            mysql_query(db, createCommentsTableConst);
        }
        mysql_close(db);
    } else if (mode == SQLITE_MODE) {
        int connRes = sqlite3_open_v2(database, (sqlite3 **) &db, 0, NULL);
        if (connRes != SQLITE_OK) {
            printf("Couldn't open database (err=%d)\n", connRes);
            return EXIT_FAILURE;
        }
        if (createTables == CREATE_TABLES_DROP || createTables == CREATE_TABLES_CONST_DROP) {
            sqlite3_exec(db, "DROP TABLE subreddits;", NULL, 0, NULL);
            sqlite3_exec(db, "DROP TABLE comments;", NULL, 0, NULL);
            createTables -= 2;
        }
        if (createTables == CREATE_TABLES) {
            sqlite3_exec(db, createSubredditsTable, NULL, 0, NULL);
            sqlite3_exec(db, createCommentsTable, NULL, 0, NULL);
        } else if (createTables == CREATE_TABLES_CONST) {
            sqlite3_exec(db, createSubredditsTableConst, NULL, 0, NULL);
            sqlite3_exec(db, createCommentsTableConst, NULL, 0, NULL);
        }
        sqlite3_close_v2(db);
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
        printf("Parsing file %s of size %li using %d threads into database %s\n", filename, size, threadCount,
               database);

    } else if (verbosity > 0) {
        printf("Parsing file %s into database %s\n", filename, database);
    }

    timekeeper_t totalTimer;
    starttimer(&totalTimer);

    job_t jobs[threadCount];
    parseTokensFromFile(filename, username, password, database, threadCount, bufferSize, size, jobs, queryLines, mode);
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

void *parseTokens(void *collection) {
    int exit = 1;
    job_t *job = collection;
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
    string_t subreddits;
    stringinit(&subreddits, 1024);
    printf("Init subreddits\n");
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
        sqlinsert(job->mode, db, buffer, job->queryLines, buffer + bufferIndex + 1, &subreddits, &totalTokenCount,
                  &totalLines);
    } while (lseek(fd, 0, SEEK_CUR) < job->end);
    mysql_close(db);
    stoptimer(&timer);
    stringfree(&subreddits);

    free(buffer);
    close(fd);

    job->result.tokens = totalTokenCount;
    job->result.lines = totalLines;
    job->result.time.tv_sec = timer.seconds;
    job->result.time.tv_nsec = timer.nanos;
    pthread_exit(&exit);
}

void
parseTokensFromFile(char *filename, char *username, char *password, char *database, int threadCount, int bufferSize,
                    long maxSize, job_t *jobs,
                    int queryLines, int mode) {
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
        jobs[i].mode = mode;
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