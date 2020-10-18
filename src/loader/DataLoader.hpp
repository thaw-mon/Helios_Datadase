#ifndef DATALOADER_HPP_
#define DATALOADER_HPP_

#include <string>
#include <iostream>
#include <unistd.h>
#include "Database.hpp"
#include "HeliosConfig.hpp"
#include "omp.h"

using namespace std;
class DataLoader{
	private:
		int32_t server_id;
		int thread_id;
		Database* db;
		HeliosConfig* cfg;
		Memory* shared_mem;

		char* mem; uint64_t mem_sz;
		vector<vector<itriple_t>> triple_spo;
		vector<vector<itriple_t>> triple_ops;

		//static const int nthread_parallel_load  = 1;
	public:
		
		DataLoader(int32_t sid, int tid,  
				HeliosConfig* cfg,Memory* mem)
			:server_id(sid),
			thread_id(tid),
			cfg(cfg), shared_mem(mem)//, memdb(memdb)
	        {
			db = new Database(sid,  cfg, mem);
		};

		void load_data(int load_op){
			if(load_op == 0 || load_op == 2)
				load_local_dict();
			if(load_op == 1 || load_op == 2)
				load_local_data();
				//load_all_data();
		}
		
		void load_data_metis_part(int load_op){
			if(load_op == 0 || load_op == 2)
				load_local_dict();
			if(load_op == 1 || load_op == 2)
				load_local_data_metis();
		}

		void load_local_dict();
		void load_local_data();
		void load_all_data();

		void load_vloc_metis();
		void load_local_data_metis();

		virtual ~DataLoader(){
			delete db;
		}

		void load_mem_data();
		void aggregate_data();
		int load_data_from_allfiles(vector<string> &fnames);

		void load_local_mem_data();

};
#endif
