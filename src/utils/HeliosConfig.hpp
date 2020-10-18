#ifndef LOADERCONFIG_HPP_
#define LOADERCONFIG_HPP_
#include <libconfig.h++>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cassert>

using namespace libconfig;
class HeliosConfig{
	public:
		const char* cfgPath;
		int global_num_servers;
		int num_loader_threads = 1; // on each server
		int num_metadata_threads = 1; //on each server, updating vertex/edge weight
		
		int db_dict_port = 6378;
		int db_counter_port = 6370;
		std::string db_counter_sock;
		
		vector<int> _workerThread_nbr_per_level_; // The number of working threads at each level
		//int _workerThread_nbr_ = 60; // Assuming there are 60 worker threads in all thread pools

		std::string host_file = "machineFile";
		std::string data_dir = "dataDir";
		std::string dict_dir = "dictDir";
		std::string str_test_cfg_path= "";//for workload test
		
		bool initial_metis = false;
		std::string metis_part = "metis-1part";
	
		//	int num_query_threads = 1;
	//	int num_queryplan_threads = 2;

		bool enable_reassign = true;
		int trigger_k = 2;
		bool trigger_double = false;
		double gama = 1.08;
		double beta = 1;
		int edgelog_len = 100000;
		int verticelog_len = 500000;

		int query_proc_method = 1;// 1- gather all data; 2- send sub queries
		
		//main database connect ip & port
		std::vector<std::string> host_names;
		std::vector<int> ports;
		std::vector<std::string> sockfiles;

		std::vector<std::string> dict_files;
		std::vector<std::string> data_files;

		double memdb_size_gb = 1.0;
		double buf_size_mb = 300.0;
	public:
		HeliosConfig(const char* cfgPath, int num_servers)
			:cfgPath(cfgPath), 
			global_num_servers (num_servers){
			load_config();
		}

		bool load_config(){
			Config cfg;
			try{
				cfg.readFile(cfgPath);
			}catch (const FileIOException &fioex) {
				printf("CONFIG %s ERROR: %s\n", cfgPath, fioex.what());
				return false;
			}catch(const ParseException &pex){
				printf("CONFIG PARSE ERROR AT: %s: %d - %s \n", 
						pex.getFile(), pex.getLine(), pex.getError());
				return false;
			}

			//
			printf("heliosConfig point : cfgPath = %s\n", cfgPath);
			cfg.lookupValue("NUM_LOADER_THREADS", num_loader_threads); //
			

			_workerThread_nbr_per_level_.resize(4);
			int num = 0;
			cfg.lookupValue("workerThread_nbr_query_task", num);
			_workerThread_nbr_per_level_[0] = num;
			cfg.lookupValue("workerThread_nbr_queryplan_task", num);
			_workerThread_nbr_per_level_[1] = num;
			cfg.lookupValue("workerThread_nbr_subquery_task", num);
			_workerThread_nbr_per_level_[2] = num;
			cfg.lookupValue("NUM_METADATA_THREADS", num_metadata_threads);
			_workerThread_nbr_per_level_[3] = num_metadata_threads;

			for(int i = 0; i < 4; i++)
			{
				printf("%d, ", _workerThread_nbr_per_level_[i]);
			}
			//printf("iiiiiii\n");
			//cfg.lookupValue("workerThread_nbr", num_loader_threads);

			cfg.lookupValue("DB_DICT_PORT", db_dict_port);
			cfg.lookupValue("DB_COUNTER_PORT", db_counter_port);
			cfg.lookupValue("DB_COUNTER_SOCK", db_counter_sock);

			cfg.lookupValue("HOST_FILE", host_file);
			cfg.lookupValue("DATA_FILE", data_dir);
			cfg.lookupValue("DICT_FILE", dict_dir);
			cfg.lookupValue("INITIAL_METIS", initial_metis);
			cfg.lookupValue("METIS_PART", metis_part);
			
			cfg.lookupValue("WORKLOAD_CONFIG", str_test_cfg_path);
			

			cfg.lookupValue("ENABLE_REASSIGN", enable_reassign);
			cfg.lookupValue("TRIGGER_K", trigger_k);
			cfg.lookupValue("TRIGGER_DOUBLE", trigger_double);
			cfg.lookupValue("GAMA", gama);
			cfg.lookupValue("BETA", beta);
			cfg.lookupValue("EDGELOG_LEN", edgelog_len);
			cfg.lookupValue("VERTICELOG_LEN", verticelog_len);
			cfg.lookupValue("QUERY_PROC_METHOD", query_proc_method);

			cfg.lookupValue("MEMDB_SIZE_GB", memdb_size_gb);
			cfg.lookupValue("BUF_SIZE_MB", buf_size_mb);
			

			std::ifstream hostfile(host_file);
			std::string ip, sockfile; int port;
			if (!hostfile.is_open()){
				printf("error : hostfile = %s can not open \n", host_file.c_str());
				return false;
			}

			for(int i = 0; i< global_num_servers; i++){
				if(hostfile >> ip >> port >> sockfile){
					host_names.push_back(ip);
					ports.push_back(port);
					sockfiles.push_back(sockfile);
				}
				else 
					break;
			}	
			assert(global_num_servers == host_names.size());
			
			//data files
			std::ifstream datafile(data_dir);
			std::string fname;
			while(datafile >> fname)
				data_files.push_back(fname);
			assert(data_files.size() > 0);

			//dict source file names
			std::ifstream dictfile(dict_dir);
			std::string dname;
			while(dictfile >> dname)
				dict_files.push_back(dname);

			assert(dict_files.size() > 0);
			return true;
		} 


};
#endif
