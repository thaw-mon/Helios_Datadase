#ifndef UTILS_H
#define UTILS_H

#include <stdlib.h>
#include "hiredis/hiredis.h"

/*redisContext* _redis_connect(const char* hostname, int port){
	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    	redisContext* c = redisConnectWithTimeout(hostname, port, timeout);
    
    	if (c == NULL || c->err) {
        	if (c) {
            		redisFree(c);
        	} 
		return NULL;
    	}
	return c;
}*/

int helios_split(const char *str, char sep, char ***arr){
    int count = 1;
    int token_len = 1;
    int i = 0;
    char *p;
    char *t;

    p = str;
    while (*p != '\0')
    {
        if (*p == sep)
            count++;
        p++;
    }

    *arr = (char**) malloc(sizeof(char*) * count);
    if (*arr == NULL)
        return -1;

    p = str;
    while (*p != '\0')
    {
        if (*p == sep)
        {
            (*arr)[i] = (char*) malloc( sizeof(char) * token_len );
            if ((*arr)[i] == NULL)
                return -1;

            token_len = 0;
            i++;
        }
        p++;
        token_len++;
    }
    (*arr)[i] = (char*) malloc( sizeof(char) * token_len );
    if ((*arr)[i] == NULL)
        return -1;

    i = 0;
    p = str;
    t = ((*arr)[i]);
    while (*p != '\0')
    {
        if (*p != sep && *p != '\0')
        {
            *t = *p;
            t++;
        }
        else
        {
            *t = '\0';
            i++;
            t = ((*arr)[i]);
        }
        p++;
    }

    return count;
}

#endif
