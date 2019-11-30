//
// Created by jona on 2019-11-30.
//
#include "common.h"
#include "jsmn.h"
#include <stdio.h>
#include <stdlib.h>

const char *queryStart = "INSERT INTO entries(id, parent_id, link_id, author, body, subreddit_id, subreddit, score, created_utc) VALUES ";


int sqlite_insert(char *buffer, const char *endP, long *totalTokens, long *totalLines, sqlite3 *db, char *query) {
    sprintf(query, "%s", queryStart);
    char *currentBuffer = buffer;
    int valuesStringSize = 1024;
    char *valuesString = malloc(sizeof(char) * valuesStringSize);
    int tokenCount = 0;
    int lines = 0;
    jsmntok_t tokens[50];
    jsmn_parser parser;
    do {
        int end = 0;

        while (currentBuffer[end++] != '\n');

        jsmn_init(&parser);
        tokenCount = jsmn_parse(&parser, currentBuffer, end - 1, tokens, 50);
        if (tokenCount < 0) {
            printf("Error! Invalid entry at line %li. %d chars. Errorcode=%d.\n", *totalLines + 1, end, tokenCount);
            continue;
        }

        for (int i = 1; i < tokenCount; i++) {
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

        sprintf(valuesString,
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
        char scorestart = *(currentBuffer + score->start);
        if ((scorestart < '0' || scorestart > '9') && scorestart != '-'){
            printf("%s", valuesString);
        }
        strcat(query, valuesString);
        *totalLines += 1;
        lines++;
        currentBuffer += end;
    } while (currentBuffer < endP);
    query[strlen(query) - 2] = ';';
    char *errormsg = malloc(sizeof(char) * 1024);
    int result = 0;
    while ((result = sqlite3_exec(db, query, NULL, 0, &errormsg)) == SQLITE_BUSY);
    if (result != SQLITE_OK && result != SQLITE_CONSTRAINT) {
        printf("%d, %s\n", result, errormsg);
        char filename[50] = "";
        sprintf(filename, "%s-%li", "errors", *totalLines);
        FILE *file = fopen(filename, "w");
        fprintf(file, "%s\n\n\n\n%s", query, errormsg);
        fclose(file);
        *totalLines -= lines;
        return lines;
    }
    printf("Sent a query %li\n", *totalLines);
    return 0;
}
