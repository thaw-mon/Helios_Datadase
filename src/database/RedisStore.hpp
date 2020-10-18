#ifndef REDISSTORE_HPP_
#define REDISSTORE_HPP_

extern "C"
{
#include "hiredis/hiredis.h"
}
#include <string.h>
#include <iostream>
#include <vector>
#include <unistd.h>
#include "type.h"

using namespace std;
class RedisStore{
	public:
		int pip_command = 0;
		//string hostname;
		//int port;
		redisContext *contxt;

		bool getBool(redisReply* reply){
			bool result = false;
			if(reply->type != REDIS_REPLY_ERROR){
				result = true;	
			}
			freeReplyObject(reply);
			return result;
		}

		template <class Type>
		vector<Type> get_array(redisReply* reply){
			vector<Type> results;
			if(reply->type == REDIS_REPLY_ARRAY){
				int size = reply->elements;
				results.resize(size);
				for(int i = 0; i < size; i++){
					if(reply->element[i] ->type == REDIS_REPLY_STRING)
						results[i] = stringToNum<Type>(reply->element[i]->str);
				}
			}
			if(reply->type == REDIS_REPLY_ERROR){
				printf("REDIS ERROR: get_array, %s", reply->str);
			}
			freeReplyObject(reply);
			return results;
		}

		void flush_command(){
			redisReply* reply;
			for(int i = 0; i< pip_command; i++){
				reply = redis_get_reply(contxt);
				if(reply != NULL)
					freeReplyObject(reply);
			}
			pip_command = 0;
		}

		//vector<int> get_array_int(redisReply* reply);

		int getInt(redisReply* reply){
			int result = -1;
			if(reply->type == REDIS_REPLY_INTEGER)
				result = reply->integer;
			else if (reply->type == REDIS_REPLY_STRING) {
				result = stringToNum<int>(reply->str);
			}
			else {
				printf("getInt reply->type = %d\n", reply->type);
			}
			freeReplyObject(reply);
			return result;
		}

		string getStr(redisReply* reply){
			string str = "";
			if(reply->type == REDIS_REPLY_STRING)
				str = reply->str;
			freeReplyObject(reply);
			return str;
		}

	public:
		RedisStore(const char* hostname, int port){
			struct timeval timeout = {1, 500000};
			contxt = redisConnectWithTimeout(hostname, port, timeout);

			while(contxt == NULL || contxt->err){
				if(contxt){
					printf("RedisStore MY ERROR: %s \n", contxt->errstr);
					cout << "hostname = " << hostname << "; port = " << port << endl;
					redisFree(contxt);
				}
				else printf("RedisStore MY ERROR: cannot allocate redis context!\n");
				contxt = redisConnectWithTimeout(hostname, port, timeout);
			}
		}
		virtual ~RedisStore(){
			if(contxt != NULL && !contxt->err)
				redisFree(contxt);
		}

		bool del_key(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "DEL %ld", key);
			if(reply == NULL){
				printf("ERROR: Redis DEL. \n");
				return false;
			}
			int r = getInt(reply);
			return r != -1;
		}

		/*Structure: STRING*/
		string get_string_by_key(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "GET %ld", key);
			if(reply == NULL){
				printf("ERROR: Redis GET. \n");
				return "";
			}
			return getStr(reply);
		}
		string get_string_by_key(const char *key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "GET %s", key);
			if(reply == NULL){
				printf("ERROR: Redis GET. \n");
				return "";
			}
			return getStr(reply);
		}
		bool set_key_string(sid_t key, const char* value){
			redisReply* reply = (redisReply*)redisCommand(contxt, "SET %ld %s", key, value);
			if(reply == NULL){
				printf("ERROR: Redis SET. \n");
				return false;
			}
			return getBool(reply);
		}
		
		bool set_key_string(const char* key, sid_t value){
			redisReply* reply = (redisReply*)redisCommand(contxt, "SET %s %ld", key, value);
			if(reply == NULL){
				printf("ERROR: Redis SET. \n");
				return false;
			}
			return getBool(reply);
		}
		
		bool set_key_string(sid_t key, int value){
			redisReply* reply = (redisReply*)redisCommand(contxt, "SET %ld %ld", key, value);
			if(reply == NULL){
				printf("ERROR: Redis SET. \n");
				return false;
			}
			return getBool(reply);
		}

		
		
		int incr_key(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "INCR %ld", key);
			if(reply == NULL){
				printf("ERROR: Redis INCR. \n");
				return -1;
			}
			return getInt(reply);
		}

		int incrby_key(sid_t key, int incr){
			redisReply* reply = (redisReply*)redisCommand(contxt, "INCRBY %ld %d", key, incr);
			if(reply == NULL){
				printf("ERROR: Redis INCRBY. \n");
				return -1;
			}
			return getInt(reply);
		}

		int decr_key(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "DECR %ld", key);
			if(reply == NULL){
				printf("ERROR: Redis DECR. \n");
				return -1;
			}
			return getInt(reply);
		}
		
		int decr_key(sid_t key, int decr){
			redisReply* reply = (redisReply*)redisCommand(contxt, "DECRBY %ld %d", key, decr);
			if(reply == NULL){
				printf("ERROR: Redis DECRBY. \n");
				return -1;
			}
			return getInt(reply);
		}

		/*Structure: SET 
		 *	TripleStore
		 * */
		bool add_set(sid_t key, sid_t value){
			redisReply* reply = (redisReply*)redisCommand(contxt, "SADD %ld %ld",key, value);
			if(reply == NULL){
				printf("ERROR: Redis SADD. \n");
				return false;
			}
			int res = getInt(reply);
			if(res == 0 || res == 1)
				return true;
			else return false;
		}

		int get_zset_len(sid_t key) {
			redisReply* reply = (redisReply*)redisCommand(contxt, "ZCARD %ld", key);
			if (reply == NULL) {
				printf("ERROR: Redis ZCARD. \n");
				return -1;
			}
			int res = getInt(reply);
			return res;
		}
		
		vector<sid_t> get_set_member(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "SMEMBERS %ld", key);
			if(reply == NULL){
				printf("ERROR: Redis SMEMBERs. \n");
				return vector<sid_t>();
			}
			return get_array<sid_t>(reply);
		}


		/*Structure: HASHMAP*/
		bool set_hash_map(sid_t key, int hash, int value){
			redisReply* reply = (redisReply*)redisCommand(contxt, "HSET %ld %d %d", key, hash, value);
			if(reply == NULL){
				printf("ERROR: REDIS HSET. \n");
				return false;
			}
			int res = getInt(reply);
			if(res == 1 || res == 0)
				return true;
			else {
				printf("fail set_hash_map res = %d\n", res);
				return false;
			}
		}

		int get_hash_key(sid_t key, int hash) {
			redisReply* reply = (redisReply*)redisCommand(contxt, "HGET %ld %d", key,hash);
			if (reply == NULL) {
				printf("ERROR: REDIS HSET. \n");
				return -1;
			}
			return getInt(reply);
		}
		vector<int> get_hash_keys(sid_t key){
			redisReply* reply = (redisReply*)redisCommand(contxt, "HKEYS %ld", key);
			if(reply == NULL){
				printf("ERROR: REDIS HKEYS. \n");
				return vector<int>();
			}
			return get_array<int>(reply);
		}

		bool del_hash_key(sid_t key, int hash){
			redisReply* reply = (redisReply*)redisCommand(contxt, "HDEL %ld %d", key, hash);
			if(reply == NULL){
				printf("ERROR: REDIS HDEL. \n");
				return false;
			}
			int res = getInt(reply);
			if(res == 1 || res == 0)
				return true;
			else return false;
		}


		/*Structure: LIST*/
		redisReply* push_to_list(sid_t key, char* value){
			return (redisReply*)redisCommand(contxt, "LPUSH %ld %s", key, value);
		}
		redisReply* set_list_index(sid_t key, int index, char* value){
			return (redisReply*)redisCommand(contxt, "LSET %ld %d %s", key, index, value);
		}
		redisReply* get_list_index(sid_t key, char* index){
			return (redisReply*)redisCommand(contxt, "LINDEX %ld %s", key, index);
		}


		/******** utilities **********/
		redisReply* redis_get_reply(redisContext* c){
			redisReply* reply;
			if(redisGetReply(contxt, (void **)&reply) != REDIS_OK)
				printf("REDIS ERROR: redisGetReply");
			return reply;
		}

		//TODO dump和restore可以使用migrate 替代
		/**************************************/
		char* dump(sid_t key, size_t *len){
			redisReply* reply = (redisReply*)redisCommand(contxt, "DUMP %ld", key);
			char* str = NULL;
			if (reply == NULL) { 
				printf("ERROR : dump repley is NULL key = %ld\n" ,key);
				//return str;
			}
			assert(reply != NULL);
			if(reply->type == REDIS_REPLY_ERROR)
				printf("DUMP error %s!\n", reply->str);
			if (reply->type == REDIS_REPLY_STRING) {
				str = reply->str; 
				*len = reply->len;
				//printf("key = %ld dump :type = %d ,len %d strlen %d reply->str len %d\n", key, reply->type, *len, strlen(str),strlen(reply->str)); //正常情况应该是二者要相等的啊
			}
			else if(reply->type == REDIS_REPLY_NIL){
				//printf("dump : key %ld : not exist!\n", key);
			}
			else {
				printf("dump: reback error type : %d\n", reply->type);
			}
			free(reply);
			return str;
		}

		const char* dump2(sid_t key, size_t* len) {
			redisReply* reply = (redisReply*)redisCommand(contxt, "DUMP %ld", key);
			stringstream ss;
			string strResult;
			if (reply == NULL) {
				printf("ERROR : dump repley is NULL key = %ld\n", key);
				//*len = -1;
				//return NULL;
			}
			assert(reply != NULL);
			if (reply->type == REDIS_REPLY_ERROR)
				printf("DUMP error %s!\n", reply->str);
			if (reply->type == REDIS_REPLY_STRING) {
				ss << reply->str;
				*len = reply->len;
			}
			else if (reply->type == REDIS_REPLY_NIL) {
				printf("dump : key %ld : not exist!\n", key);
			}
			else {
				printf("dump: reback error type : %d\n", reply->type);
			}
			freeReplyObject(reply); 
			return ss.str().c_str();
			//return strResult.empty()? NULL:strResult.c_str();
		}

		bool restore(sid_t key, const char* str, size_t len){

			//printf("restore %ld %d\n", key, len);
			redisReply *reply; 
			reply= (redisReply*)redisCommand(contxt, 
				"RESTORE %ld %d %b %s", key, 0, str, size_t(len), "REPLACE");
			if (reply == NULL) {
				printf("ERROR : restore repley is NULL key = %ld len = %ld\n", key,len);
				//return false;
			}
			assert(reply != NULL);
			bool flag = true;
			if(reply->type == REDIS_REPLY_ERROR){
				flag = false;
				printf("Restore error! key = %ld reply = %s\n",key, reply->str);
			}
			freeReplyObject(reply);
			return flag;
		}


		bool migrate(sid_t key, const char* hostname, int port) {
			redisReply* reply = (redisReply*)redisCommand(contxt, "MIGRATE %s %d %ld %d %d %s", hostname,port,key,0,1000,"REPLACE");
			bool flag = false;
			if (reply == NULL) {
				printf("ERROR : migrate repley is NULL key = %ld\n", key);
				//	freeReplyObject(reply);
				return flag;
			}
			if (reply->type == REDIS_REPLY_ERROR) {
				printf("MIGRATE error %s!\n", reply->str);
			}
			if (reply->type == REDIS_REPLY_STATUS && reply->str == "OK") {
				flag = true;
			}
			else {
				printf("MIGRATE Type not fit %d!\n", reply->type);
			}
			return flag;
		}

};

#endif
