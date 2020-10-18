#ifndef HELIOS_STRUCTURE_H
#define HELIOS_STRUCTURE_H

#include "redismodule.h"
#include "utils/type.h"
#include "redis-module-helios/rmutil/vector.h"
#include <assert.h>
/******* String *********/
int _StringSet(RedisModuleCtx* ctx, RedisModuleString* key_v, RedisModuleString*  value ){
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_STRING &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	RedisModule_StringSet(key, value);
	RedisModule_FreeString(ctx, key_v);
	RedisModule_FreeString(ctx, value);
	RedisModule_CloseKey(key);
	return REDISMODULE_OK;
}

int helios_StringSetStr(RedisModuleCtx *ctx, sid_t key_t, const char *str){
	RedisModuleString* key = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString* newv = RedisModule_CreateString(ctx, str, strlen(str));
	return _StringSet(ctx, key, newv);
}

int helios_StringSet(RedisModuleCtx *ctx, sid_t key_t, sid_t v){
	RedisModuleString* key = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *newv = RedisModule_CreateStringFromLongLong(ctx, (long long)v);
	return _StringSet(ctx, key, newv);
}

int helios_StringGetSet(RedisModuleCtx *ctx, sid_t key_t, const char* str, int len){
	RedisModuleString* key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, 
			REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_STRING &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, 
				REDISMODULE_ERRORMSG_WRONGTYPE);
	}
	size_t v_len = 0;
	char* value = RedisModule_StringDMA(key, &v_len, 
			REDISMODULE_WRITE|REDISMODULE_READ);
	int exists = 0;
	if(value != NULL){
	for(int i = 0; i < v_len; ){
		int j = 0;
		for(j = 0; j < len; j++){
			if(value[i + j] != str[j])
				break;
		}
		if(j == len){
			exists = 1;
			break;
		}
		i += len;
	}}
	if(exists != 1){
		RedisModule_StringTruncate(key, len + v_len);
		value = RedisModule_StringDMA(key, &v_len, 
			REDISMODULE_WRITE|REDISMODULE_READ);
		for(int i = len; i > 0; i--){
			value[v_len - i] = str[len - i];
		}
		RedisModule_CloseKey(key);
		return 1;
	}
	RedisModule_CloseKey(key);
	return 0;
}

Vector* deserial_vector(char* bytes, int len){
	int v_sz = len / NBYTES_VID;
	Vector* sets = NewVector(sid_t, v_sz);
	int pos = 0;
	for(int i = 0; i < len; ){
		Vector_Put(sets, pos, deserial_vid(bytes, i));
		i += NBYTES_VID;
		pos++;
	}
	assert(pos == v_sz);
	//printf("o_vals size %d\n", sets.size());
	return sets;
}

Vector* helios_StringGet(RedisModuleCtx *ctx, sid_t key_t){
	RedisModuleString* key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, 
			REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_STRING &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return NULL;
		/*return RedisModule_ReplyWithError(ctx, 
				REDISMODULE_ERRORMSG_WRONGTYPE);*/
	}
	size_t v_len = 0;
	char* value = RedisModule_StringDMA(key, &v_len, 
			REDISMODULE_WRITE|REDISMODULE_READ);
	return deserial_vector(value, v_len);
}
/*int helios_incr_String(RedisModuleCtx* ctx, const char* key_t ){
	RedisModule_AutoMemory(ctx);
	RedisModuleString* key_v = RedisModule_CreateString(ctx, key_t, strlen(key_t));
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_STRING &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	RedisModule_FreeString(ctx, key_v);
	RedisModule_CloseKey(key);
	return REDISMODULE_OK;
}*/

/*********** Zset *************/
int helios_ZsetAdd(RedisModuleCtx *ctx, sid_t key_t, sid_t v){
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *val = RedisModule_CreateStringFromLongLong(ctx, (long long)v);
	
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_ZSET &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}
	RedisModule_ZsetAdd(key, (double)v, val, NULL);

	RedisModule_FreeString(ctx, key_v);
	RedisModule_FreeString(ctx, val);
	RedisModule_CloseKey(key);	
	return 1;
}
// Zset Zadd NX
int _ZsetAdd_NX(RedisModuleCtx *ctx, RedisModuleString *key_v, RedisModuleString * val, double v){
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_ZSET &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}
	int flag = REDISMODULE_ZADD_NX;
	RedisModule_ZsetAdd(key, v, val, &flag);
	
	RedisModule_FreeString(ctx, key_v);
	RedisModule_FreeString(ctx, val);
	RedisModule_CloseKey(key);
	if(flag == REDISMODULE_ZADD_ADDED)
		return 1;
	else if(flag == REDISMODULE_ZADD_UPDATED)
		return 0;
	else if (flag == REDISMODULE_ZADD_NOP)
		return 0;
	else return 0;
}

int helios_ZsetAdd_NX(RedisModuleCtx *ctx, sid_t key_t, sid_t v){
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *val = RedisModule_CreateStringFromLongLong(ctx, (long long)v);
	return _ZsetAdd_NX(ctx, key_v, val, (double)v);
}


int helios_ZsetStrAdd_NX(RedisModuleCtx *ctx, const char *key_t, sid_t v){
	RedisModuleString *key_v = RedisModule_CreateString(ctx, key_t, strlen(key_t));
	RedisModuleString *val = RedisModule_CreateStringFromLongLong(ctx, (long long)v);
	return _ZsetAdd_NX(ctx, key_v, val, (double)v);
}

//zadd score
int helios_ZsetAddScore(RedisModuleCtx *ctx, sid_t key_t, sid_t v, double score){
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *val = RedisModule_CreateStringFromLongLong(ctx, (long long)v);
	
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_ZSET &&
		type != REDISMODULE_KEYTYPE_EMPTY){
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}
	RedisModule_ZsetAdd(key, score, val, NULL);

	RedisModule_FreeString(ctx, key_v);
	RedisModule_FreeString(ctx, val);
	RedisModule_CloseKey(key);	
	return 1;
}
//ZRANGE
Vector* _helios_ZsetGet(RedisModuleCtx *ctx, RedisModuleString *key_v){
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);

	int type = RedisModule_KeyType(key);
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_Log(ctx,"warning","SET not exists");
		return NULL;
	}
	if(type != REDISMODULE_KEYTYPE_ZSET ){
		RedisModule_Log(ctx,"warning","SET type is wrong");
		return NULL; 
	}
	RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
			REDISMODULE_POSITIVE_INFINITE, 0, 0);
	Vector* result = NewVector(sid_t,0);
	int len = 0;
	while(!RedisModule_ZsetRangeEndReached(key)){
		double score;
		RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key, &score);
		sid_t e;
		RedisModule_StringToLongLong(ele, (long long *)&e);
		Vector_Put(result,len,  e);
		RedisModule_FreeString(ctx, ele);
		RedisModule_ZsetRangeNext(key);
		len++;
	}
	Vector_Resize(result,len);
	RedisModule_ZsetRangeStop(key);
	RedisModule_CloseKey(key);
	return result;
}

Vector* helios_ZsetGet(RedisModuleCtx *ctx, sid_t key_t){
	RedisModule_AutoMemory(ctx);
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	
	return _helios_ZsetGet(ctx, key_v);
}

Vector* helios_ZsetGetStr(RedisModuleCtx *ctx, const char* key_t){
	RedisModule_AutoMemory(ctx);
	RedisModuleString *key_v = RedisModule_CreateString(ctx, key_t, strlen(key_t));
	
	return _helios_ZsetGet(ctx, key_v);
}

int helios_ZsetGetUnion(RedisModuleCtx *ctx, sid_t key_t, Vector* union_set){
	RedisModule_AutoMemory(ctx);
	RedisModule_Log(ctx, "warning", "ZsetGetUnion: %ld", key_t);
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);

	int type = RedisModule_KeyType(key);
	if(type == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_Log(ctx,"warning","SET not exists");
		return 0;
	}
	if(type != REDISMODULE_KEYTYPE_ZSET ){
		RedisModule_Log(ctx,"warning","%s","SET type is wrong");
		return 0; 
	}
	RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
			REDISMODULE_POSITIVE_INFINITE, 0, 0);
	int len = Vector_Cap(union_set);
	while(!RedisModule_ZsetRangeEndReached(key)){
		double score;
		RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key, &score);
		sid_t e;
		RedisModule_StringToLongLong(ele, (long long *)&e);
		Vector_Put(union_set, len,  e);
		RedisModule_FreeString(ctx, ele);
		RedisModule_ZsetRangeNext(key);
		len++;
	}
	Vector_Resize(union_set,len);
	RedisModule_ZsetRangeStop(key);
	RedisModule_CloseKey(key);
	return 1;
}

int helios_ZsetExists(RedisModuleCtx *ctx, sid_t key_t){
	RedisModule_AutoMemory(ctx);
	RedisModule_Log(ctx, "warning", "ZsetGetUnion: %ld", key_t);
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);

	int type = RedisModule_KeyType(key);
	if(type == REDISMODULE_KEYTYPE_ZSET){
		RedisModule_CloseKey(key);
		return 1; 
	}else return 0;
}

int helios_delKey(RedisModuleCtx *ctx, const char* key_t){
	RedisModule_AutoMemory(ctx);
	RedisModuleString *key_v = RedisModule_CreateString(ctx, key_t, strlen(key_t));
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ|REDISMODULE_WRITE);
	return RedisModule_DeleteKey(key);
}
int helios_ZsetUnion(RedisModuleCtx *ctx, const char* desti, Vector* sets){
 	int size = Vector_Cap(sets);
	int i = 0;
	if(size >= 2){
		char* v1; Vector_Get(sets, i, &v1);
		i++;
		char* v2; Vector_Get(sets, i, &v2);
		if(size >= 3){
		i++;
		char* v3; Vector_Get(sets, i, &v3);
		if(size >= 4){
		i++;
		char *v4; Vector_Get(sets, i, &v4);
		if(size >= 5){
		i++;
		char *v5; Vector_Get(sets, i, &v5);
		if(size >= 6){
		i++;
		char *v6; Vector_Get(sets, i, &v6);
		if(size >= 7){
			i++;
			char *v7; Vector_Get(sets, i, &v7);
			if(size >= 8){
			i++;
			char *v8; Vector_Get(sets, i, &v8);
			if(size >= 9){
			i++;
			char *v9; Vector_Get(sets, i, &v9);
			if(size >= 10){
			i++;
			char * v10; Vector_Get(sets, i, &v10);
			if(size >= 11)
				return 0;
			RedisModule_Call(ctx, "ZUNIONSTORE", "clcccccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
			return 1;
			}
			RedisModule_Call(ctx, "ZUNIONSTORE", "clccccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8, v9);
			return 1;
			}
			RedisModule_Call(ctx, "ZUNIONSTORE", "clcccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8);
			return 1;
			}
			RedisModule_Call(ctx, "ZUNIONSTORE", "clccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7);
			return 1;
		}
		RedisModule_Call(ctx, "ZUNIONSTORE", "clcccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6);
		return 1;
		}
		RedisModule_Call(ctx, "ZUNIONSTORE", "clccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5);
		return 1;
		}
		RedisModule_Call(ctx, "ZUNIONSTORE", "clcccc", desti, 
				(long long)size, v1, v2, v3, v4);
		return 1;
		}
		RedisModule_Call(ctx, "ZUNIONSTORE", "clccc", desti, 
				(long long)size, v1, v2, v3);
		return 1;
		}
		RedisModule_Call(ctx, "ZUNIONSTORE", "clcc", desti, 
				(long long)size, v1, v2);
		return 1;
	}
	else return 0;
}
int helios_ZsetInter(RedisModuleCtx *ctx, const char* desti, Vector* sets){
 	int size = Vector_Cap(sets);
	int i = 0;
	if(size >= 2){
		char* v1; Vector_Get(sets, i, &v1);
		i++;
		char* v2; Vector_Get(sets, i, &v2);
		if(size >= 3){
		i++;
		char* v3; Vector_Get(sets, i, &v3);
		if(size >= 4){
		i++;
		char *v4; Vector_Get(sets, i, &v4);
		if(size >= 5){
		i++;
		char *v5; Vector_Get(sets, i, &v5);
		if(size >= 6){
		i++;
		char *v6; Vector_Get(sets, i, &v6);
		if(size >= 7){
			i++;
			char *v7; Vector_Get(sets, i, &v7);
			if(size >= 8){
			i++;
			char *v8; Vector_Get(sets, i, &v8);
			if(size >= 9){
			i++;
			char *v9; Vector_Get(sets, i, &v9);
			if(size >= 10){
			i++;
			char * v10; Vector_Get(sets, i, &v10);
			if(size >= 11)
				return 0;
			RedisModule_Call(ctx, "ZINTERSTORE", "clcccccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
			return 1;
			}
			RedisModule_Call(ctx, "ZINTERSTORE", "clccccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8, v9);
			return 1;
			}
			RedisModule_Call(ctx, "ZINTERSTORE", "clcccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7, v8);
			return 1;
			}
			RedisModule_Call(ctx, "ZINTERSTORE", "clccccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6, v7);
			return 1;
		}
		RedisModule_Call(ctx, "ZINTERSTORE", "clcccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5, v6);
		return 1;
		}
		RedisModule_Call(ctx, "ZINTERSTORE", "clccccc", desti, 
				(long long)size, v1, v2, v3, v4, v5);
		return 1;
		}
		RedisModule_Call(ctx, "ZINTERSTORE", "clcccc", desti, 
				(long long)size, v1, v2, v3, v4);
		return 1;
		}
		RedisModule_Call(ctx, "ZINTERSTORE", "clccc", desti, 
				(long long)size, v1, v2, v3);
		return 1;
		}
		RedisModule_Call(ctx, "ZINTERSTORE", "clcc", desti, 
				(long long)size, v1, v2);
		return 1;
	}
	else return 0;
}
/************ Set *************/
Vector* helios_SetGet(RedisModuleCtx *ctx, sid_t key_t){
	RedisModuleCallReply* reply = 
		RedisModule_Call(ctx, "SMEMBERS", "l", (long long)key_t);
	
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		Vector* results = NewVector(sid_t, 0);
		long long items = RedisModule_CallReplyLength(reply);
		RedisModuleCallReply *item;
		for(int i = 0; i < items; i++){
			item = RedisModule_CallReplyArrayElement(reply, i);
			RedisModuleString* v_str = RedisModule_CreateStringFromCallReply(item);
			long long val;
			RedisModule_StringToLongLong(v_str, &val);
			Vector_Put(results, i, (sid_t)val);
			RedisModule_FreeCallReply(item);
		}
		Vector_Resize(results,items);
		return results;
	}
	return NULL;
}
int helios_SetUnion(RedisModuleCtx *ctx, sid_t key_t, Vector* results){
	RedisModuleCallReply* reply = 
		RedisModule_Call(ctx, "SMEMBERS", "l", (long long)key_t);
	int len = Vector_Cap(results);
	if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
		Vector* results = NewVector(sid_t, 0);
		long long items = RedisModule_CallReplyLength(reply);
		RedisModuleCallReply *item;
		for(int i = 0; i < items; i++){
			item = RedisModule_CallReplyArrayElement(reply, i);
			RedisModuleString* v_str = RedisModule_CreateStringFromCallReply(item);
			long long val;
			RedisModule_StringToLongLong(v_str, &val);
			Vector_Put(results, len, (sid_t)val);
			len++;
			RedisModule_FreeCallReply(item);
		}
		Vector_Resize(results,len);
		return 1;
	}
	return 0;
}

/************ HashSet ****************/
RedisModuleString* _helios_HashGet(RedisModuleCtx * ctx, RedisModuleString *key_v, RedisModuleString *field){
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ);
	int type = RedisModule_KeyType(key);

	if(type != REDISMODULE_KEYTYPE_HASH ){
		goto end;
	}
	RedisModuleString *value;
	RedisModule_HashGet(key, REDISMODULE_HASH_NONE, field, &value, NULL);
	RedisModule_CloseKey(key);
	return value;
end:
	RedisModule_Log(ctx, "warning", "Hash type wrong or not exists");
	RedisModule_CloseKey(key);
	return NULL;
}

RedisModuleString* helios_HashGet(RedisModuleCtx *ctx, sid_t key_t, int field){
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *field_v = RedisModule_CreateStringFromLongLong(ctx, (long long)field);
	return _helios_HashGet(ctx, key_v, field_v);
}

int helios_HashGetOne(RedisModuleCtx *ctx, sid_t key_t, long long field1, long long* v1){
	RedisModuleString* v = helios_HashGet(ctx, key_t, field1);
	if(v != NULL)
		RedisModule_StringToLongLong(v, v1);
	return 1;
}

int _helios_HashGetTwo(RedisModuleCtx * ctx, RedisModuleString *key_v, RedisModuleString *field1, 
		RedisModuleString *field2, long long *v1, long long *v2){
	RedisModule_AutoMemory(ctx);
	RedisModuleKey *key = RedisModule_OpenKey(ctx, key_v, REDISMODULE_READ);
	int type = RedisModule_KeyType(key);
	if(type != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		RedisModule_Log(ctx, "warning", "Hash type wrong or not exists");
		return 1;
	}
	RedisModuleString *value1, *value2;
	RedisModule_HashGet(key, REDISMODULE_HASH_NONE, field1, &value1, field2, &value2, NULL);
	RedisModule_CloseKey(key);
	if(value1 != NULL)
		RedisModule_StringToLongLong(value1, v1);
	if(value2 != NULL)
		RedisModule_StringToLongLong(value2, v2);
	return 1;
}
int helios_HashGetTwo(RedisModuleCtx *ctx, sid_t key_t, long long field1, long long field2, 
		long long* v1, long long * v2){
	RedisModuleString *key_v = RedisModule_CreateStringFromLongLong(ctx, (long long)key_t);
	RedisModuleString *field_v1 = RedisModule_CreateStringFromLongLong(ctx, field1);
	RedisModuleString *field_v2 = RedisModule_CreateStringFromLongLong(ctx, field2);
	return _helios_HashGetTwo(ctx, key_v, field_v1, field_v2, v1, v2);
}

#endif
