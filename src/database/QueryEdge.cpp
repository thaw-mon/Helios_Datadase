#include "QueryEdge.hpp"
#include "QueryNode.hpp"
#include "AsyncRedisStore.hpp"

QueryEdge::~QueryEdge(){
	if(node != NULL){
		delete node;
		node = NULL;
	}
		
}

int QueryEdge::preprocess(int tgt_s, AsyncRedisStore* async_store){
	if(!(exec_flag == 1 || exec_flag == 3))// need preprocess
		return 0;
	if( bind_val.size() == 0){
		printf("ERR- predicate is not bound\n");
		return 0;
	}
	vector<string> int_keys; int_keys.resize(2);
	for(int i = 0; i< bind_val.size(); i++){
		if((node->bind_val).size() >100)
			continue;
		sid_t p = bind_val[i];
		if((node->bind_val).size() == 0){
			string key_t = std::to_string(key_vpid_t(0, p, INDEX, d));
			int_keys.push_back(key_t);
			src_union_key[tgt_s] += key_t;
			continue;
		}

		for(int j = 0; j < (node->bind_val).size(); j++){
			sid_t o = (node->bind_val)[j];
			string key_t;
			if(p == 1 && d == OUT)
				key_t = std::to_string(key_vpid_t(0, o, INDEX, d));
			else 
				key_t = std::to_string(key_vpid_t(o, p, INDEX, inverse_dir(d)));
			//printf("union_key %ld %d %s\n", o, p, key_t.c_str());
			int_keys.push_back(key_t);
			src_union_key[tgt_s] += key_t;
		}
	}
	//cout << "int_keys_size = " << int_keys.size() << endl;
	if(int_keys.size() <= 2)
		return 0;

	//here edge is [?x p ?y]  
	if (int_keys.size() == 3) {
		//cout << "size =3 key_t =" << int_keys[2].c_str() << endl;
		return 1;
	}
	string c = "SUNIONSTORE";
	int_keys[0] = c;
	int_keys[1] = src_union_key[tgt_s];
	async_store->write_command(int_keys);
	//is_key_new = 1;
	
	return 1;
}
int QueryEdge::parse_triples(int *tag, vector<const char*> *args){
	sid_t s,p,o;
	if(d == OUT){
		s = 0;
		p = ++(*tag);
		o = ++(*tag);
	}else{
		o = 0;
		p = ++(*tag);
		s = ++(*tag);
	}
	args->push_back(intToChar(s));
	args->push_back(intToChar(p));
	args->push_back(intToChar(o));
	args->push_back(intToChar(d));
	args->push_back(intToChar(exec_flag));
}

int QueryEdge::parse_binds(vector<const char*> *args){
	args->push_back(intToChar(bind_val.size()));
	for(int i = 0; i < bind_val.size(); i++){
		args->push_back(intToChar(bind_val[i]));
	}

	args->push_back(intToChar((node->bind_val).size()));
	for(int i = 0; i< (node->bind_val).size(); i++){
		args->push_back(intToChar((node->bind_val)[i]));
	}

}

int QueryEdge::print(int indent){
	printf("tag: %d\ttowardsJoint: %d\tdir: %d\t", tag, towardsJoint, d);
	
	printf("binds: {");
	for(int i = 0; i < bind_val.size(); i++){
		printf("%ld ", bind_val[i]);
	}
	printf("}");
	
	printf("\n");
	for(int j = 0; j < indent; j++)
		printf(" ");
	printf("-> ");
	node->print(indent+1);
	
}

