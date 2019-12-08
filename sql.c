#include <mysql/mysql.h>
#include <stdio.h>
#include "jsmn.h"
#include "sql.h"

//
// Created by jona on 2019-12-08.
//
#define commentsQueryStart \
"INSERT INTO comments(comment_id, subreddit_id, name, link_id, author, body, time_created, parent_id, score) VALUES "

const string_t commentsInsertString = {commentsQueryStart, strlen(commentsQueryStart) + 1, strlen(commentsQueryStart)};
const char *subredditInsertString = "INSERT INTO subreddits(subreddit_id, subreddit_name) VALUES";

void stringinit(string_t *string, unsigned int size) {
    string->bufferSize = size;
    string->buffer = malloc(sizeof(char) * string->bufferSize);
    string->length = 0;
}

void stringreset(string_t *string) {
    string->length = 0;
    string->buffer[0] = 0;
}

void stringfree(string_t *string) {
    string->length = 0;
    string->bufferSize = 0;
    free(string->buffer);
}

void stringsetsize(string_t *string, unsigned int size) {
    string->bufferSize = size;
    string->buffer = realloc(string->buffer, sizeof(char) * string->bufferSize);
}

void stringfit(string_t *string, unsigned int length) {
    while (length >= string->bufferSize) {
        stringsetsize(string, string->bufferSize * 2);
    }
}

void stringcat(string_t *dst, const string_t *src) {
    stringfit(dst, dst->length + src->length + 1);
    for (unsigned int i = 0; i < src->length; i++) {
        dst->buffer[dst->length + i] = src->buffer[i];
    }
    dst->length = dst->length + src->length;
    dst->buffer[dst->length] = 0;
}

int stringContainsWhole(const string_t *string, const char *buffer, jsmntok_t *token) {
    char str[token->size + 1];
    sprintf(str, "%.*s ", token->end - token->start, buffer + token->start);
    if (strstr(string->buffer, str) != NULL) {
        return 1;
    }
    return 0;
}

int sqlinsert(int mode, void *db, char *buffer, int queryLines, const char *endP, string_t *subreddits,
              long *totalTokens, long *totalLines) {
    char *currentBuffer = buffer;

    string_t commentsQuery;
    stringinit(&commentsQuery, 8192);

    string_t subredditQuery;
    stringinit(&subredditQuery, 1024);

    string_t singleValue;
    stringinit(&singleValue, 1024);

    parseresult_t result;
    do {
        stringreset(&commentsQuery);
        stringreset(&singleValue);
        stringreset(&subredditQuery);
        stringcat(&commentsQuery, &commentsInsertString);
        result = constructQueries(queryLines, &commentsQuery, &singleValue, subreddits, &subredditQuery, currentBuffer,
                                  endP);
        int success = 0;
        int mysqlResult = 0;

        if (subredditQuery.length != 0) {
            int index = 0;
            int end = 0;
            while (index < subredditQuery.length) {
                while (subredditQuery.buffer[end++] != ';');
                if (mode == MYSQL_MODE) {
                    mysqlResult = mysql_real_query(db, subredditQuery.buffer + index, end - index);
                    if (mysqlResult) {
                        printf("Error %d when inserting subreddits: %s\n", mysqlResult, mysql_error(db));
                        printf("%.*s\n", end - index, subredditQuery.buffer + index);
                    }
                } else if (mode == SQLITE_MODE) {

                }
                index = end + 1;
            }
        }

        mysqlResult = mysql_query(db, commentsQuery.buffer);
        if (mysqlResult) {
            printf("Error %d when inserting lines: %s\n", mysqlResult, mysql_error(db));
            success = 0;
        } else {
            success = 1;
        }
        if (success) {
            *totalLines += result.lines;
            *totalTokens += result.tokens;
        }
        currentBuffer += result.characters;
    } while (result.lines > 0 && currentBuffer < endP);
    stringfree(&commentsQuery);
    stringfree(&singleValue);
    return 1;
}

parseresult_t
constructQueries(int queryLines, string_t *commentQuery, string_t *valuesString, string_t *insertedSubreddits,
                 string_t *subredditQuery, char *buffer, const char *endP) {
    parseresult_t result = {0, 0};
    jsmn_parser parser;
    jsmntok_t tokens[50];

    string_t space = {" ", 2, 1};

    while (result.lines < queryLines && buffer < endP) {
        int end = 0;

        while (buffer[end++] != '\n' && end != buffer - endP);
        jsmn_init(&parser);
        int tokenCount = jsmn_parse(&parser, buffer, end - 1, tokens, 50);

        for (int i = 2; i < tokenCount - 1; i += 2) {
            jsmntok_t token = tokens[i];
            for (int x = token.start; x < token.end - 1; x++) {
                if (buffer[x] == '\\' && buffer[x + 1] == '\"') {
                    buffer[x] = '\"';
                }
            }
        }

        stringfit(valuesString, end + 256);

        jsmntok_t *id = getbykey("id", buffer, tokens, tokenCount);
        jsmntok_t *parent_id = getbykey("parent_id", buffer, tokens, tokenCount);
        jsmntok_t *name = getbykey("name", buffer, tokens, tokenCount);
        jsmntok_t *linkid = getbykey("link_id", buffer, tokens, tokenCount);
        jsmntok_t *author = getbykey("author", buffer, tokens, tokenCount);
        jsmntok_t *body = getbykey("body", buffer, tokens, tokenCount);
        jsmntok_t *subreddit_id = getbykey("subreddit_id", buffer, tokens, tokenCount);
        jsmntok_t *subreddit = getbykey("subreddit", buffer, tokens, tokenCount);
        jsmntok_t *score = getbykey("score", buffer, tokens, tokenCount);
        jsmntok_t *created_utc = getbykey("created_utc", buffer, tokens, tokenCount);

        long actualSubredditId = strtol(buffer + subreddit_id->start + 3, NULL, 36);
        long actualId = strtol(buffer + id->start, NULL, 36);

        if (!stringContainsWhole(insertedSubreddits, buffer, subreddit_id)) {
            string_t insertQuery;
            stringinit(&insertQuery, 512);
            insertQuery.length = sprintf(insertQuery.buffer, "%s (%li, \"%.*s\"); ", subredditInsertString,
                                         actualSubredditId, subreddit->end - subreddit->start,
                                         buffer + subreddit->start);
            stringcat(subredditQuery, &insertQuery);

            string_t subredditStr = {buffer + subreddit_id->start, subreddit_id->end - subreddit_id->start,
                                     subreddit_id->end - subreddit_id->start};
            stringcat(insertedSubreddits, &subredditStr);
            stringcat(insertedSubreddits, &space);
        }


        valuesString->length = sprintf(valuesString->buffer,
                                       "(%li, %li, \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", %.*s, \"%.*s\", %.*s),\n",
                                       actualId,
                                       actualSubredditId,
                                       name->end - name->start, buffer + name->start,
                                       linkid->end - linkid->start, buffer + linkid->start,
                                       author->end - author->start, buffer + author->start,
                                       body->end - body->start, buffer + body->start,
                                       created_utc->end - created_utc->start, buffer + created_utc->start,
                                       parent_id->end - parent_id->start, buffer + parent_id->start,
                                       score->end - score->start, buffer + score->start);

        stringcat(commentQuery, valuesString);
        result.lines++;
        result.tokens += tokenCount;
        result.characters += end;
        buffer += end;
    }
    if (subredditQuery->length != 0) {
        subredditQuery->buffer[subredditQuery->length - 2] = ';';
    }
    commentQuery->buffer[commentQuery->length - 2] = ';';
    return result;
}

