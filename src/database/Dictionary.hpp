#ifndef DICTIONARY_HPP_
#define DICTIONARY_HPP_

#include "RedisStore.hpp"
#include <vector>
#include <string>
#include "type.h"

using namespace std;

class Dictionary : public RedisStore{

	/* Dictionary stores the ID - string pairs
	 * Using the Redis string structure.
	 * */

	public:
		Dictionary(const char* host, int port):RedisStore(host, port){}
		
		virtual ~Dictionary(){}

		
		/*bool load_local_dict(const char* filename, int num_servers, int sid){
			redisReply* reply;
			reply = (redisReply*)redisCommand(contxt, 
					"helios.load_dict %s %d %d", 
					filename, num_servers, sid);
			freeReplyObject(reply);
		}*/	
		
		bool insert_dict(sid_t id, const string &str){
			redisAppendCommand(contxt, 
					"SET %ld %s", id, str.c_str());
			pip_command++;
			
			if(pip_command > 100)
				flush_command();
			return true;
		}

		bool insert_dict(string str, sid_t id){
			redisAppendCommand(contxt, 
					"SET %s %ld", str.c_str(), id);
			pip_command++;
			
			if(pip_command > 100)
				flush_command();
			return true;
		}
		
		string get_dict_by_id (sid_t id){
			return get_string_by_key(id);
		}

		sid_t get_id_by_dict(const string &text){
			string str = get_string_by_key(text.c_str());
			if(str == ""){
				printf("%s ERROR: lookup DICT", text.c_str());
				return 0;			
			}
			return stringToNum<sid_t>(str);
		}
};

#endif
