#ifndef ASYNC_CALLBACK_H
#define ASYNC_CALLBACK_H
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <map>
#include "triple_t.hpp"
extern "C"{
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include "type.h"
}

void connectCallback(const redisAsyncContext *c, int status);

void disconnectCallback(const redisAsyncContext *c, int status);


void rediscommand_callback(redisAsyncContext *c, void *r, void *privdata);

void rediswrite_callback(redisAsyncContext *c, void *r, void *privdata);

void read_hashset_callback(redisAsyncContext* c, void* r, void* privdata);
void del_hashset_callback(redisAsyncContext* c, void* r, void* privdata);

void read_zset_callback(redisAsyncContext *c, void *r, void *privdata);
void read_zset_len_callback(redisAsyncContext* c, void* r, void* privdata);
void read_strset_callback(redisAsyncContext *c, void *r, void *privdata);

void traverse_callback(redisAsyncContext *c, void *r, void *privdata);

void batch_traverse_callback(redisAsyncContext* c, void* r, void* privdata);

/*
 * getloc_reqlock_callback
 * batch_getloc_callback
 * */
void batch_getloc_callback(redisAsyncContext *c, void *r, void *privdata);

void batch_getloc2_callback(redisAsyncContext* c, void* r, void* privdata);

void batch_getloc3_callback(redisAsyncContext* c, void* r, void* privdata);
#endif
