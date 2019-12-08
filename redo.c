#include <mysql/mysql.h>
#include <stdio.h>
#include "jsmn.h"
#include "redo.h"

//
// Created by jona on 2019-12-08.
//
#define commentsQueryStart \
"INSERT INTO entries(id, parent_id, link_id, " \
"author, body, subreddit_id, subreddit, score, created_utc) VALUES "

const string_t commentsInsertString = {commentsQueryStart, strlen(commentsQueryStart) + 1, strlen(commentsQueryStart)};

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
    dst->length += src->length;
    dst->buffer[dst->length] = 0;
}

parseresult_t
constructQuery(string_t *commentQuery, string_t *subredditQuery, string_t *valuesString, char *buffer, const char *endP,
               int queryLines);

int redo(char *buffer, const char *endP, long *totalTokens, long *totalLines, MYSQL *db, int queryLines) {
    char *currentBuffer = buffer;

    string_t comments;
    stringinit(&comments, 8192);

    string_t subreddits;
    stringinit(&subreddits, 1024);

    string_t singleValue;
    stringinit(&singleValue, 1024);
    parseresult_t result;
    do {
        stringreset(&comments);
        stringreset(&singleValue);

        stringcat(&comments, &commentsInsertString);
        result = constructQuery(&comments, NULL, &singleValue, currentBuffer, endP, queryLines);

        int mysqlResult = 0;

        mysqlResult = mysql_query(db, comments.buffer);
        if (mysqlResult) {
            printf("Error: %s\n", mysql_error(db));
        }

        currentBuffer += result.characters;
        *totalLines += result.lines;
        *totalTokens += result.tokens;

    } while (result.lines > 0 && currentBuffer < endP);
    stringfree(&comments);
    stringfree(&singleValue);
    return 1;
}

parseresult_t
constructQuery(string_t *commentQuery, string_t *subredditQuery, string_t *valuesString, char *buffer, const char *endP,
               int queryLines) {
    parseresult_t result = {0, 0};
    jsmn_parser parser;
    jsmntok_t tokens[50];

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
        jsmntok_t *linkid = getbykey("link_id", buffer, tokens, tokenCount);
        jsmntok_t *author = getbykey("author", buffer, tokens, tokenCount);
        jsmntok_t *body = getbykey("body", buffer, tokens, tokenCount);
        jsmntok_t *subreddit_id = getbykey("subreddit_id", buffer, tokens, tokenCount);
        jsmntok_t *subreddit = getbykey("subreddit", buffer, tokens, tokenCount);
        jsmntok_t *score = getbykey("score", buffer, tokens, tokenCount);
        jsmntok_t *created_utc = getbykey("created_utc", buffer, tokens, tokenCount);
        valuesString->length = sprintf(valuesString->buffer,
                                       "(\"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", \"%.*s\", %.*s, %.*s),\n",
                                       id->end - id->start,
                                       buffer + id->start,
                                       parent_id->end - parent_id->start,
                                       buffer + parent_id->start,
                                       linkid->end - linkid->start,
                                       buffer + linkid->start,
                                       author->end - author->start,
                                       buffer + author->start,
                                       body->end - body->start,
                                       buffer + body->start,
                                       subreddit_id->end - subreddit_id->start,
                                       buffer + subreddit_id->start,
                                       subreddit->end - subreddit->start,
                                       buffer + subreddit->start,
                                       score->end - score->start,
                                       buffer + score->start,
                                       created_utc->end - created_utc->start,
                                       buffer + created_utc->start);
        stringcat(commentQuery, valuesString);
        result.lines++;
        result.tokens += tokenCount;
        result.characters += end;
        buffer += end;
    }
    commentQuery->buffer[commentQuery->length - 2] = ';';
    return result;
}