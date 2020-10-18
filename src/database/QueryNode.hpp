#ifndef QUERYNODE_HPP_
#define QUERYNODE_HPP_

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include <vector>
#include "type.h"
#include "QueryEdge.hpp"
#include "AsyncRedisStore.hpp"
#include "TripleDB.hpp"
#include "triple_t.hpp"

using namespace std;
class QueryNode{
	public:
		sid_t root_vertex = 0;
		int tag, was_seen;
		bool pruned_step1, pruned_step2;
		string name;
		vector<sid_t> bind_val;
		vector<QueryEdge*> edges;

		vector<sid_t> bind_to_prune;
		//vector<sid_t> tmp_bind_val;
		vector<string> src_inter_key;
		//int is_key_new = 0;
		//vector<sid_t> candi_src_bind;

		//add couter
		sid_t edge_cut_counter = 0;
		sid_t edge_cut_counter_spend_time = 0;
		//int edge_cut_counter_reassgin = 0; //重分配后的edge_cut计数 移除，动态

		pthread_spinlock_t prune_lock;
		pthread_spinlock_t bind_lock;
		//add lock
		pthread_spinlock_t counter_lock;

	public:
		QueryNode(){
			pthread_spin_init(&prune_lock, 0);
			pthread_spin_init(&bind_lock, 0);
			pthread_spin_init(&counter_lock, 0);
		}
		
		QueryNode(int tag, vector<sid_t> bind_v):
			tag(tag), bind_val(bind_v){
			pthread_spin_init(&prune_lock, 0);
			pthread_spin_init(&bind_lock, 0);
			pthread_spin_init(&counter_lock, 0);
			}

		virtual ~QueryNode(){
			for(int i = 0; i< edges.size(); i++){
				if(edges[i] != NULL)
					delete edges[i];
			}
		}

		void diable_read_index(){
			for(int i = 0; i < edges.size(); i++)
				if(edges[i] -> exec_flag == 1)
					edges[i]->exec_flag = 0;
				else if (edges[i] -> exec_flag == 3)
					edges[i] -> exec_flag = 2;
		}

		void init_interunion_key(int num_server){
			src_inter_key.resize(num_server);
			for(int i = 0; i < edges.size(); i++)
				(edges[i]->src_union_key).resize(num_server);
		}
		//function init src_inter_key
		int preprocess(int tgt_s,AsyncRedisStore* async_store){
			if(edges.size() == 0)
				return 0;
			vector<string> keys;keys.resize(2);
			for(int i = 0; i < edges.size(); i++){
				int flag = edges[i]->preprocess(tgt_s,async_store);
				//cout << "edge src_union_key = " << edges[i]->src_union_key[tgt_s] << endl;
				if(!flag)
					continue;
				keys.push_back(edges[i]->src_union_key[tgt_s]);
				//
				src_inter_key[tgt_s] += edges[i]->src_union_key[tgt_s];
			}
			//cout << "node preprocess : src_inter_key = " << src_inter_key[tgt_s] << endl;
			//cout << "node inter_keys.size() =" << keys.size() << endl;
			
			if(keys.size() <= 2)
				return 0;
			if(keys.size() == 3)
				return 1;
		
			//fuction 获取集合的交集
			string c = "SINTERSTORE";
			keys[0] = c;
			keys[1] = src_inter_key[tgt_s];
			async_store->write_command(keys);
			//is_key_new = 1;
			return 1;
		}
		template <typename Archive> void serialize(Archive &ar, const unsigned int version){
			ar & tag;	
			ar & was_seen;	
			ar & name;
			ar & root_vertex;
			ar & bind_val;
			ar & edges;	
			ar& edge_cut_counter; //add
			ar& edge_cut_counter_spend_time; //add
		}		
		int insert_bind(vector<sid_t> &tgt){
			pthread_spin_lock(&bind_lock);
			bind_val.insert(bind_val.end(), 
					tgt.begin(), tgt.end());
			pthread_spin_unlock(&bind_lock);
			return 1;
		}

		void insert_prune_bind(sid_t vid){
			pthread_spin_lock(&prune_lock);
			bind_to_prune.push_back(vid);
			pthread_spin_unlock(&prune_lock);
		}
		
		void insert_prune_bind(vector<sid_t> &vid){
			pthread_spin_lock(&prune_lock);
			bind_to_prune.insert(bind_to_prune.end(),
					vid.begin(), vid.end());
			pthread_spin_unlock(&prune_lock);
		}

		void add_edge_cut_counter(sid_t num,sid_t time) {
			pthread_spin_lock(&counter_lock);
			edge_cut_counter += num;
			edge_cut_counter_spend_time += time;
			pthread_spin_unlock(&counter_lock);
		}

		void add_edge_cut_counter(sid_t num) {
			pthread_spin_lock(&counter_lock);
			edge_cut_counter += num;
			//edge_cut_counter_spend_time += time;
			pthread_spin_unlock(&counter_lock);
		}

		void set_edge_cut_counter(sid_t counter, sid_t time) {
			pthread_spin_lock(&counter_lock);
			edge_cut_counter = counter;
			edge_cut_counter_spend_time = time;
			pthread_spin_unlock(&counter_lock);
		}
		sid_t get_edge_counter() {
			return edge_cut_counter;
		}
		sid_t get_edge_counter_spend_time() {
			return edge_cut_counter_spend_time;
		}

		int traverse_args(vector<sid_t> &src_bind, vector<const char*> *args){
			args->push_back(intToChar(edges.size()));
			int tag = 0;
			for(int i = 0; i < edges.size(); i++){
				QueryEdge* edge = edges[i];
				edge->parse_triples(&tag, args);
			}

			args->push_back(intToChar(++tag));
			args->push_back(intToChar(src_bind.size()));
			for(int i = 0; i < src_bind.size(); i++){
				args->push_back(intToChar(src_bind[i]));
			}
			for(int i = 0; i < edges.size(); i++){
				QueryEdge* edge = edges[i];
				edge->parse_binds(args);
			}
		}

		int print(int indent){
			printf("tag: %d\t",tag);
			printf("binds: {");
			for(int i = 0; i < bind_val.size(); i++){
				printf("%ld ", bind_val[i]);
			}
			printf("}");

			for(int i = 0; i < edges.size(); i++){
				printf("\n");
				QueryEdge* edge = edges[i];
				for(int j = 0; j < indent; j++)
					printf(" ");
				printf("-- ");
				edge->print(indent+1);
			}
		}
};

#endif
