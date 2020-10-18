/*
add in 2020/08/04
功能：重写用户端代码
*/

//////////////////////////////////////////////////////////////////////////////////
#ifndef HELIOS_PART_CLIENT2_H
#define HELIOS_PART_CLIENT2_H

#define _number_of_queries_ 14

//#include <boost/timer.hpp>
#include  <boost/timer/timer.hpp>
#include <libconfig.h>
#include "BindCPU.hpp"
#include "Workload_T2.hpp"
#include "watdiv_workload2.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
//#include <boost/timer/timer.hpp>


template<typename T>
int save_querybank2(T t, std::string str_save_workloads_file_path)
{
	std::cout << "save_querybank begin" << std::endl;
	if (str_save_workloads_file_path != "")
	{
		std::cout << "save workload to file" << std::endl;
		std::vector<std::string> vec_qb_se;
		for (int i = 0; i < t.size(); ++i)
		{
			stringstream ss;
			boost::archive::binary_oarchive oa(ss);
			oa << t[i];
			std::string str = ss.str();
			std::cout << "str_length=" << str.length() << std::endl;
			vec_qb_se.push_back(str);
		}

		WORKLOAD_serialize wk_se(vec_qb_se);
		stringstream ss;
		boost::archive::binary_oarchive oa(ss);
		oa << wk_se;
		std::string str_wk_se = ss.str();
		std::cout << "WORKLOAD_serialize_len=" << str_wk_se.length() << std::endl;
		std::cout << "str_save_workloads_file_path=" << str_save_workloads_file_path << std::endl;
		std::ofstream fd(str_save_workloads_file_path.c_str(), std::ios_base::out | ios::trunc);
		if (!fd.is_open())
		{
			std::cout << "openfile_error" << std::endl;
			fd.close();
			return -1;
		}
		fd << str_wk_se;
		fd.close();
	}
	std::cout << "save_querybank end" << std::endl;
	return 1;
}

template <typename T, typename T2>
int load_query2(T& t1, T2& t2, const std::string str_query_file_path, const std::string& str_data_set_type)
{
	std::cout << "load_query begin" << std::endl;
	WORKLOAD_serialize seri;
	std::fstream in(str_query_file_path.c_str(), std::ios_base::in);
	if (!in.is_open())
	{
		std::cout << "openfile_error   file_path=" << str_query_file_path << std::endl;
		in.close();
		return -1;
	}
	std::string str((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>()); ;
	in.close();
	stringstream ss;
	ss << str;
	boost::archive::binary_iarchive ia(ss);
	ia >> seri;
	std::vector<std::string> vec_qb_se;
	vec_qb_se = seri.get_workload();

	for (int i = 0; i < vec_qb_se.size(); ++i)
	{
		stringstream ss;
		ss << vec_qb_se[i];
		boost::archive::binary_iarchive ia(ss);
		T2 qb;
		ia >> qb;
		t1.push_back(qb);
	}
	std::cout << "load_query end" << std::endl;
	return 1;
}



int test_query2(HeliosConfig* config, vector<QueryBank>& vec_QB_workload, int num_workloads, std::vector<int>& num_query_per_workload,
	const std::string& current_nodeName, int client_nbr, std::string str_save_workloads_file_path)
{
	cout << "client start query" << endl;
	char port[50];
	char portNo[10];
	size_t size = sizeof(port);

	std::vector<double> vec_sum_time_double;
	std::vector<double> vec_sum_time_double_get_usec;
	// Create query_push_socket in order to send queries to query proxies
	zmq::context_t context(1);
	zmq::socket_t query_push_socket(context, ZMQ_PUSH);

	for (auto it_hosts = config->host_names.begin(); it_hosts != config->host_names.end(); ++it_hosts)
	{
		std::cout << "it_hosts=" << *it_hosts << std::endl;
		connect_(query_push_socket, "tcp://" + *it_hosts + ":" + query_proxy_port);
	}

	/////////////////////////
	// Create query_pull_socket in order to receive the results of queries
	zmq::socket_t query_pull_socket(context, ZMQ_PULL);
	bind_(query_pull_socket, "tcp://*:0");

	// Find the port number where query_pull_socket is listening to
	query_pull_socket.getsockopt(ZMQ_LAST_ENDPOINT, &port, &size);

	memcpy(portNo, port + 14 /* Offset */, 5 /* Length */);
	portNo[5] = 0; /* Put end of striong */

	//std::vector<double> vec_time_result_double;

	std::string str_save_query_time = str_save_workloads_file_path + "_query_time";
	std::ofstream fd(str_save_query_time.c_str(), std::ios_base::out | ios::trunc);
	std::cout << "str_save_query_time=" << str_save_query_time << std::endl;
	if (!fd.is_open())
	{
		std::cout << "fd_open_error " << "file_path=" << str_save_query_time << std::endl;
		return -4;
	}
	std::string str_query_begin_time = boost::posix_time::to_iso_string(boost::posix_time::second_clock::local_time());
	std::cout << "query_begin_time=" << str_query_begin_time << std::endl;
	for (int jj = 0; jj < num_workloads; ++jj)
	{
		int kk = jj; //废操作：可以使用jj替换kk
		//	int kk = 0 ;
		std::vector<double> vec_time_result_double_get_usec;
		std::vector<sid_t> vec_edge_cut; 
		//std::vector<double> vec_edge_cut_spend_time; //计算edge_cut_spend_time
		/////////////////////////
		// Step1: Send queries number to query proxies via query_push_socket
		//TODO 修改为不需要vec_QB_workload 只发送 IP 、Port 和 qNumber
		for (int query_nbr = 1; query_nbr <= num_query_per_workload[kk]; query_nbr++)
		{
			ClientData clientData; //定义用户发送数据

			stringstream ss; //废操作
			std::cout << "query_nbr=" << query_nbr << std::endl;
			srand(time(NULL));
			// int random_query_nbr = rand() % _number_of_queries_;
			int random_query_nbr = query_nbr - 1;
			std::cout << "size=" << vec_QB_workload[kk].get_qlist_size() << std::endl;
			if (query_nbr > vec_QB_workload[kk].get_qlist_size())
			{
				std::cout << "query_nbr<vec_QB_workload.get_qlist_size() error" << std::endl;
				break;
			}

			//////////////////////////////
			//push data to query proxy 
			//string clientData = numToString(kk) + "." + numToString(random_query_nbr);
			clientData.setQueryNo(random_query_nbr);
			clientData.setWorkloadNo(kk);
			clientData.setIP(current_nodeName);
			clientData.setPort(portNo);
			clientData.setQuery(vec_QB_workload[kk].getQuery(random_query_nbr)); 
			string clientStr = wrap_clientData(clientData);

			zmq::message_t sendClient(clientStr.length());
			memcpy((void*)sendClient.data(), clientStr.c_str(), clientStr.length());
			//boost::timer t_timer; 
			long send_time = get_usec();
			send_(query_push_socket, &sendClient);
			std::cout << std::endl << BOLDBLUE << "Client#" << client_nbr << " hosted at " << current_nodeName
				<< " sent query#" << query_nbr << RESET << std::endl;

			/////////////////////////////
			//receive query result
			zmq::message_t queryResult;
			receive_(query_pull_socket, &queryResult);

			/////////////////////////////
			Query q = unwrap_query(std::string((char*)queryResult.data(), queryResult.size()));
			std::string str_query_time = "workload_no=" + std::to_string(kk) + " query_no=" + std::to_string(q.queryNo) + " query_pattern=" +
				std::to_string(q.query_pattern) + " time_get_usec=" + std::to_string(((float)(get_usec() - send_time)) / 1000000) + "second \n";
			std::string str_query_edge_cut = "edge_cut=" + std::to_string(q.edge_cut_counter);

			fd << str_query_time << str_query_edge_cut << std::endl;
			std::cout << "str_query_time=" << str_query_time << std::endl;
			//		std::cout << "workload_no=" << kk  << "query_no=" << q.queryNo << "query_pattern=" << q.query_pattern  << "time=" <<t_timer.elapsed() << std::endl; 
			//		std::cout << "workload_no=" << kk << "query_no=" << q.queryNo << "query_pattern=" << q.query_pattern  << "time_get_usec=" <<((float) (get_usec() - send_time))/1000000 << std::endl; 
			//		vec_time_result_double.push_back(t_timer.elapsed());
			vec_time_result_double_get_usec.push_back(((double)(get_usec() - send_time)) / 1000000);
			vec_edge_cut.push_back(q.edge_cut_counter);
			//vec_edge_cut_spend_time.push_back(1.0 * (double)(q.edge_cut_count_spend_time) / 1000000);

			std::cout << "size_q.result" << q.result_map.size() << std::endl;
			for (auto it_r = q.result_map.begin(); it_r != q.result_map.end(); ++it_r)
			{
				std::cout << "size_triple=" << it_r->size() << std::endl;
				for (auto it_triple = it_r->begin(); it_triple != it_r->end(); ++it_triple)
				{
					std::cout << "s=" << it_triple->s << " p=" << it_triple->p << " o" << it_triple->o << std::endl;
				}
			}

			std::cout << std::endl << BOLDBLUE << "Client#" << client_nbr << " hosted at " << current_nodeName
				<< " received the result of query#" << query_nbr << ", as the following:" << RESET << std::endl;

			/////////////////////////////

			cout << std::endl << "--------------------------------------- Timing Metadata ---------------------------------------";
			cout << std::endl << BOLDBLUE << "Response time of the query is "
				<< BOLDRED << ((float)(get_usec() - q.getTime_sent())) / 1000000 << RESET;
			cout << std::endl << BOLDBLUE << "Maximum response time of corresponding query plans is "
				<< ((float)q.time_elapsed_for_queryPlans) / 1000000 << RESET;
			cout << std::endl << BOLDBLUE << "Maximum evaluation time of corresponding query plans is "
				<< ((float)q.time_elapsed_for_evaluation) / 1000000 << RESET;
			cout << std::endl << BOLDBLUE << "Commute time of the query is "
				<< ((float)((get_usec() - q.getTime_sent()) - q.time_elapsed_for_queryPlans)) / 1000000
				<< std::endl << RESET;
			//add Edge Cut info
			cout << std::endl << BOLDBLUE << "Edge Cut Counter of the query is "
				<< q.edge_cut_counter << std::endl << RESET;
			cout << std::endl << BOLDBLUE << "Edge Cut Counter Spend Time of the query is "
				<< q.edge_cut_count_spend_time << std::endl << RESET;
			/////////////////////////////
		
		}
		double time_count = 0.0;
		double time_count_get_usec = 0.0;
		sid_t all_edge_cut = 0;
		//double all_edge_cut_spend_time = 0.0;
		cout << "vec_time_result_double_get_usec.size() = " << vec_time_result_double_get_usec.size() << endl;
		for (int j = 0; j != vec_time_result_double_get_usec.size(); ++j)
		{
			//time_count +=vec_time_result_double[j];
			time_count_get_usec += vec_time_result_double_get_usec[j];
			all_edge_cut += vec_edge_cut[j];
			//all_edge_cut_spend_time += vec_edge_cut_spend_time[j];
		}
		//Error vec_sum_time_double和vec_sum_time_double_get_usec未使用
		vec_sum_time_double.push_back(time_count);
		vec_sum_time_double_get_usec.push_back(time_count_get_usec);
		std::cout << "workloadNumber" << num_workloads << endl;
		std::cout << " cost_time=" << time_count << std::endl;
		std::cout << " cost_time_get_usec=" << time_count_get_usec << std::endl;
		std::cout << "num_workload=" << kk << ": " << std::endl;
		std::string str_cost_time = "cost_time_get_usec=" + std::to_string(time_count_get_usec) + "\n";
		std::string all_edge_cut_str = "all edge cut=" + std::to_string(all_edge_cut) + "\n";

		fd << "workloadNumber" << kk << str_cost_time << all_edge_cut_str;

	}
	fd.close();
	std::string str_query_end_time = boost::posix_time::to_iso_string(boost::posix_time::second_clock::local_time());
	std::cout << "query_begin_time=" << str_query_begin_time << std::endl;
	std::cout << "query_end_time=" << str_query_end_time << std::endl;
	std::cout << "test_query end" << std::endl;
	return 1;
}

//update query to instance
void get_replace_instance(std::vector<int>& vec_pattern, std::vector<std::map<int, std::string>>& vec_replace_instance) {
	std::map<int, std::string> map_rp;
	vec_replace_instance.clear();
	//TODO 增加范围为1-14
	if (std::find(vec_pattern.begin(), vec_pattern.end(), 0) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.Department0.University0.edu/GraduateCourse\t>" });//0
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=1" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 1) != vec_pattern.end())
	{
		map_rp.insert({ 100,"non-select" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=2" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 2) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.Department0.University0.edu/AssistantProfessor\t>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=3" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 3) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.Department0.University\t.edu>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=4" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 4) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.Department0.University\t.edu>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=5" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 5) != vec_pattern.end())
	{
		map_rp.insert({ 100,"non-select" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=6" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 6) != vec_pattern.end())
	{
		map_rp.insert({ 3,"s^<http://www.Department0.University0.edu/AssociateProfessor\t>" });//6
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=7" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 7) != vec_pattern.end())
	{
		map_rp.insert({ 3,"o^<http://www.University\t.edu>" });//7
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=8" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 8) != vec_pattern.end())
	{
		map_rp.insert({ 100,"non-select" });
		vec_replace_instance.push_back(map_rp);//8
		map_rp.clear();
		std::cout << "replace_pattern=9" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 9) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.Department0.University0.edu/GraduateCourse\t>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=10" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 10) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.University\t.edu>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=11" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 11) != vec_pattern.end())
	{
		map_rp.insert({ 2,"o^<http://www.University\t.edu>" });//11
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=12" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 12) != vec_pattern.end())
	{
		map_rp.insert({ 1,"o^<http://www.University\t.edu>" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=13" << std::endl;
	}

	if (std::find(vec_pattern.begin(), vec_pattern.end(), 13) != vec_pattern.end())
	{
		map_rp.insert({ 100,"non-select" });
		vec_replace_instance.push_back(map_rp);
		map_rp.clear();
		std::cout << "replace_pattern=14" << std::endl;
	}


	if (vec_replace_instance.size() != vec_pattern.size())
	{
		std::cout << "vec_replace_instance.size=" << vec_replace_instance.size() << std::endl;
		std::cout << "vec_pattern.size=" << vec_pattern.size() << std::endl;
		std::cout << "vec_replace_instance.size()!=vec_pattern.size() error" << std::endl;
		exit(0);
	}
}
/******************************************************************
 * 		workload_nbr:Number of workloads<<
 * 		pattern_frequency:Frequency of each pattern  of each workload
 * 		nbr_q_per_workload:The number of queries per workload(每个workload的query数量)
 *
 * ***************************************************************/


int get_workloads_for_lubm2(HeliosConfig* config, std::vector<int>& vec_pattern, int workload_nbr, std::vector<std::vector<int>>& pattern_frequency,
	std::vector<int>& nbr_q_per_workload, std::vector<QueryBank>& vec_lubm_qb)
{
	//std::cout << "get_workloads begin" << std::endl;
	if (workload_nbr != pattern_frequency.size() || workload_nbr != nbr_q_per_workload.size())
	{
		std::cout << "workload_nbr=" << workload_nbr << std::endl;
		std::cout << " pattern_frequency.size=" << pattern_frequency.size() << std::endl;
		std::cout << "nbr_q_per_workload.size=" << nbr_q_per_workload.size() << std::endl;
		std::cout << "Parameter error" << std::endl;
		return -1;
	}

	std::vector<std::map<int, std::string>> vec_replace_instance;
	//std::map<int, std::string> map_rp;
	get_replace_instance(vec_pattern, vec_replace_instance);

	for (int i = 0; i < workload_nbr; ++i)
	{

		Workload_T2 workload_test(config, vec_pattern, vec_replace_instance, nbr_q_per_workload[i], pattern_frequency[i]);
		if (1 != workload_test.init())
		{
			std::cout << "workload_test.init failed" << std::endl;
			return -2;
		}
		QueryBank qb_workload("lubm");
		vec_lubm_qb.push_back(qb_workload);
		if (1 != workload_test.reset_lubm_query_bank(qb_workload, vec_lubm_qb[i]))
		{
			std::cout << "workload_test.reset_lubm_query_bank failed" << std::endl;
			return -3;
		}
	}

	std::cout << "get_workloads_for_lubm end" << std::endl;
	return 1;
}




int get_workloads_for_yago2(HeliosConfig* config, std::vector<int>& vec_pattern, int workload_nbr, std::vector<std::vector<int>>& pattern_frequency,
	std::vector<int>& nbr_q_per_workload, std::vector<QueryBank>& vec_lubm_qb)
{
	std::cout << "get_workloads begin" << std::endl;
	if (workload_nbr != pattern_frequency.size() || workload_nbr != nbr_q_per_workload.size())
	{
		std::cout << "workload_nbr=" << workload_nbr << std::endl;
		std::cout << " pattern_frequency.size=" << pattern_frequency.size() << std::endl;
		std::cout << "nbr_q_per_workload.size=" << nbr_q_per_workload.size() << std::endl;
		std::cout << "Parameter error" << std::endl;
		return -1;
	}

	std::vector<std::map<int, std::string>> vec_replace_instance;
	std::map<int, std::string> map_rp;
	map_rp.insert({ 100,"non-select" });
	vec_replace_instance.push_back(map_rp);//q2
	map_rp.clear();

	map_rp.insert({ 100,"non-select" });
	vec_replace_instance.push_back(map_rp);//q3


	if (vec_replace_instance.size() != vec_pattern.size())
	{
		std::cout << "vec_replace_instance.size=" << vec_replace_instance.size() << std::endl;
		std::cout << "vec_pattern.size=" << vec_pattern.size() << std::endl;
		std::cout << "vec_replace_instance.size()!=vec_pattern.size() error" << std::endl;
		return -2;
	}

	for (int i = 0; i < workload_nbr; ++i)
	{

		Workload_T2 workload_test(config, vec_pattern, vec_replace_instance, nbr_q_per_workload[i], pattern_frequency[i]);
		if (1 != workload_test.init())
		{
			std::cout << "workload_test.init failed" << std::endl;
			return -2;
		}
		QueryBank qb_workload("yago2");
		vec_lubm_qb.push_back(qb_workload);
		if (1 != workload_test.reset_lubm_query_bank(qb_workload, vec_lubm_qb[i]))
		{
			std::cout << "workload_test.reset_lubm_query_bank failed" << std::endl;
			return -3;
		}
	}

	std::cout << "get_workloads_for_lubm end" << std::endl;
	return 1;
}



/**************************
 * 		config:
 * HeliosConfig* config,std::vector<int> &vec_pattern,int workload_nbr, std::vector<std::vector<int>> &pattern_frequency,
								 std::vector<int> & nbr_q_per_workload,std::vector<LUBM_queryBank> &vec_lubm_qb
 *
 * ***************/

int get_workloads_for_watdiv2(HeliosConfig* config, std::vector<int>& vec_pattern, int workload_nbr, std::vector<std::vector<int>>& pattern_frequency,
	std::vector<int>& nbr_q_per_workload, std::vector<QueryBank>& vec_lubm_qb)
{
	std::cout << "get_workloads_for_watdiv begin" << std::endl;
	std::string str_file_path_wt = "/usr/data/helios_test/run_helios/query_workload";
	for (int i = 0; i < workload_nbr; ++i)
	{
		Watdic_workload_T2 QB_wd_change(str_file_path_wt, vec_pattern, nbr_q_per_workload[i], pattern_frequency[i]);
		if (1 != QB_wd_change.init())
		{
			std::cout << "Watdic_workload_T_init error" << std::endl;
			return -1;
		}
		QueryBank QB_workload_wd("watdiv");
		if (QB_wd_change.set_query(QB_workload_wd) != 1)
		{
			std::cout << "set_query error" << std::endl;
			return -2;
		}
		vec_lubm_qb.push_back(QB_workload_wd);
	}
	std::cout << "get_workloads_for_watdiv endl" << std::endl;
	return 1;
}

bool get_cfg2(const std::string& file_path, int& num_workloads, std::vector<int>& vec_patterns,
	std::vector<std::vector<int>>& vec_pattern_frequency, std::vector<int>& num_query_per_workload,
	bool& is_load_query_from_file, std::string& query_file_path,
	std::string& str_save_workloads_file_path, std::string& str_data_set_type)
{
	std::cout << "file_path=" << file_path << std::endl;
	Config cfg;
	try {
		cfg.readFile(file_path.c_str());
	}
	catch (const FileIOException & fioex) {
		printf("CONFIG %s ERROR: %s\n", file_path.c_str(), fioex.what());
		return false;
	}
	catch (const ParseException & pex) {
		printf("CONFIG PARSE ERROR AT: %s: %d - %s \n",
			pex.getFile(), pex.getLine(), pex.getError());
		return false;
	}

	cfg.lookupValue("is_load_query_from_file", is_load_query_from_file);
	cfg.lookupValue("query_file_path", query_file_path);
	cfg.lookupValue("num_workloads", num_workloads);
	cfg.lookupValue("data_set_type", str_data_set_type);
	cfg.lookupValue("save_workloads_file_path", str_save_workloads_file_path);
	str_save_workloads_file_path += "_p_w";
	query_file_path += "_p_w";
	Setting& pattern_seting = cfg.lookup("workload_setting.pattern");
	for (int i = 0; i < pattern_seting.getLength(); ++i)
	{
		vec_patterns.push_back(pattern_seting[i]);
		//std::cout << "vec_patterns = " << vec_patterns[vec_patterns.size() - 1] << std::endl;
	}

	Setting& nbr_of_workloads = cfg.lookup("workload_setting.num_query_per_workload");
	for (int i = 0; i < nbr_of_workloads.getLength(); ++i)
	{
		num_query_per_workload.push_back(nbr_of_workloads[i]);
	}
	for (auto it = num_query_per_workload.begin(); it != num_query_per_workload.end(); ++it)
	{
		str_save_workloads_file_path = str_save_workloads_file_path + "_" + std::to_string(*it);
		query_file_path = query_file_path + "_" + std::to_string(*it);
	}

	Setting& pattern_frequency = cfg.lookup("workload_setting.pattern_frequency");
	for (int i = 0; i < pattern_frequency.getLength(); ++i)
	{
		std::vector<int> vec_tmp;
		int tmp_count_frequency = 0;
		for (int j = 0; j < pattern_frequency[i].getLength(); ++j)
		{

			vec_tmp.push_back(pattern_frequency[i][j]);
			tmp_count_frequency += vec_tmp[vec_tmp.size() - 1];
		}
		if (tmp_count_frequency != 100)
		{
			std::cout << "tmp_count_frequency error " << "tmp_count_frequency=" << tmp_count_frequency << std::endl;
			return false;
		}
		vec_pattern_frequency.push_back(vec_tmp);
	}
	return true;
}


//get vec_QB_workload info

std::vector<QueryBank> get_workload_config_info(HeliosConfig* config) {

	std::string str_file_path_config = config->str_test_cfg_path;
	//get config
	int num_workloads;//workload的数量
	std::vector<int> vec_patterns;
	std::vector<std::vector<int>> vec_pattern_frequency;
	std::vector<int> num_query_per_workload;   //每个workload的query数量
	bool is_load_query_from_file;
	std::string str_query_file_path;//从文件中加载workload
	std::string str_save_workloads_file_path;//保存workload的路径
	std::string str_data_set_type;


	if (!get_cfg2(str_file_path_config, num_workloads, vec_patterns, vec_pattern_frequency, num_query_per_workload,
		is_load_query_from_file, str_query_file_path, str_save_workloads_file_path, str_data_set_type))
	{
		std::cout << "get_cfg error" << std::endl;
		exit(0);
	}
	else {
		//打印信息: 略
		/*for (auto it = vec_patterns.begin(); it != vec_patterns.end(); ++it)
		{
			std::cout << "vec_patterns" << *it << std::endl;
		}

		for (auto it = num_query_per_workload.begin(); it != num_query_per_workload.end(); it++)
		{
			std::cout << "num_query_per_workload=" << *it << std::endl;
		}*/
		/*for (auto it = vec_pattern_frequency.begin(); it != vec_pattern_frequency.end(); ++it)
		{
			for (auto it_k = it->begin(); it_k != it->end(); ++it_k)
			{
				std::cout << "f = " << *it_k << std::endl;
			}
			std::cout << ';' << std::endl;
		}*/
		/*std::cout << "is_load_query_from_file=" << is_load_query_from_file << std::endl;
		std::cout << "num_workloads=" << num_workloads << std::endl;*/

	}
	std::vector<QueryBank> vec_QB_workload;

	//判定 is_load_query_from_file
	if (!is_load_query_from_file)
	{
		std::cout << "load data from QB_workload" << std::endl;
		if (str_data_set_type == "lubm")
		{
			std::cout << "str_data_set_type = lubm" << std::endl;
			if (1 != get_workloads_for_lubm2(config, vec_patterns, num_workloads, vec_pattern_frequency, num_query_per_workload, vec_QB_workload))
			{
				std::cout << "get_workloads_for_lubm error" << std::endl;
				exit(0);
			}
			for (auto it = vec_QB_workload.begin(); it != vec_QB_workload.end(); ++it)
			{
				std::cout << "lubm_qlist_size=" << it->get_qlist_size() << std::endl;
			}
			if (vec_QB_workload.size() <= 0)
			{
				std::cout << "vec_QB_workload.size = 0  error" << std::endl;
				exit(0);
			}
			std::cout << "ok" << std::endl;
			if (1 != save_querybank2(vec_QB_workload, str_save_workloads_file_path))
			{
				std::cout << "save_querybank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/

		}
		else if (str_data_set_type == "watdiv")
		{
			std::cout << "str_data_set_type = watdiv" << std::endl;
			//std::vector<WATDiv_queryBank> vec_QB_workload;
			if (1 != get_workloads_for_watdiv2(config, vec_patterns, num_workloads, vec_pattern_frequency, num_query_per_workload, vec_QB_workload))
			{
				std::cout << "get_workloads_for_watdiv error" << std::endl;
				exit(0);
			}
			for (auto it = vec_QB_workload.begin(); it != vec_QB_workload.end(); ++it)
			{
				std::cout << "watdiv_qlist_size=" << it->get_qlist_size() << std::endl;
			}
			if (vec_QB_workload.size() <= 0)
			{
				std::cout << "vec_QB_workload.size = 0  error" << std::endl;
				exit(0);
			}
			std::cout << "ok" << std::endl;
			if (1 != save_querybank2(vec_QB_workload, str_save_workloads_file_path))
			{
				std::cout << "save_querybank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/
			//return vec_QB_workload;
		}
		else if (str_data_set_type == "yago")
		{
			std::cout << "str_data_set_type = yago" << std::endl;
			//std::vector<YAGO2_queryBank> vec_QB_workload;
			if (1 != get_workloads_for_yago2(config, vec_patterns, num_workloads, vec_pattern_frequency, num_query_per_workload, vec_QB_workload))
			{
				std::cout << "get_workloads_for_watdiv error" << std::endl;
				exit(0);
			}
			for (auto it = vec_QB_workload.begin(); it != vec_QB_workload.end(); ++it)
			{
				std::cout << "yago_qlist_size=" << it->get_qlist_size() << std::endl;
			}
			if (vec_QB_workload.size() <= 0)
			{
				std::cout << "vec_QB_workload.size = 0  error" << std::endl;
				exit(0);
			}
			std::cout << "ok" << std::endl;
			if (1 != save_querybank2(vec_QB_workload, str_save_workloads_file_path))
			{
				std::cout << "save_querybank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/
			//return vec_QB_workload;
		}
		else
		{
			std::cout << "str_data_set_type error " << std::endl;
			exit(0);
		}
	}
	else
	{
		std::cout << "load_query_from_file" << std::endl;
		if (str_data_set_type == "lubm")
		{
			//std::vector<LUBM_queryBank> vec_QB_workload;
			QueryBank qb("lubm");
			std::cout << "start call load_query2" << std::endl;
			if (1 != load_query2(vec_QB_workload, qb, str_query_file_path, str_data_set_type))
			{
				std::cout << "load_query LUBM_queryBank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/
			//return vec_QB_workload;

		}
		else if (str_data_set_type == "watdiv")
		{
			//std::vector<WATDiv_queryBank> vec_QB_workload;
			QueryBank qb("watdiv");
			//WATDiv_queryBank qb;
			if (1 != load_query2(vec_QB_workload, qb, str_query_file_path, str_data_set_type))
			{
				std::cout << "load_query WATDiv_queryBank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/
			//return vec_QB_workload;
		}
		else if (str_data_set_type == "yago")
		{
			//std::vector<QueryBank> vec_QB_workload;
			//YAGO2_queryBank qb;
			QueryBank qb("yago2");
			if (1 != load_query2(vec_QB_workload, qb, str_query_file_path, str_data_set_type))
			{
				std::cout << "load_query WATDiv_queryBank error" << std::endl;
			}
			/*else
			{
				test_query(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
			}*/
		}
		else
		{
			std::cout << "str_data_set_type error" << std::endl;
		}
	}
	return vec_QB_workload;
}

//////////////////////////////////////////////////////////////////////////////////
//template<typename T >
void client_task2(string current_nodeName, int numberOfQueries, int client_nbr, HeliosConfig* config, vector<QueryBank>& vec_QB_workload)
{
	std::cout << "client_task begin" << std::endl;
	//	old(current_nodeName,numberOfQueries,client_nbr,config);
	std::cout << "numberofQueries=" << numberOfQueries << std::endl;
	std::cout << "client_task thread lwpid = " << syscall(SYS_gettid) << "currentNode=" << current_nodeName << std::endl;
	std::vector<unsigned int> needBindCPUVector = { 0,1,2,3,4,5,6,7,8,9 };
	BindCPU bind_cpu;
	bind_cpu.get_set_cpu_operation(needBindCPUVector);

	// workload_test2(current_nodeName, numberOfQueries, client_nbr, config);
	//
	//get config
	int num_workloads;//workload的数量
	std::vector<int> vec_patterns;
	std::vector<std::vector<int>> vec_pattern_frequency;
	std::vector<int> num_query_per_workload;   //每个workload的query数量
	bool is_load_query_from_file;
	std::string str_query_file_path;//从文件中加载workload
	std::string str_save_workloads_file_path;//保存workload的路径
	std::string str_data_set_type;
	std::string str_file_path_config = config->str_test_cfg_path;

	if (!get_cfg2(str_file_path_config, num_workloads, vec_patterns, vec_pattern_frequency, num_query_per_workload,
		is_load_query_from_file, str_query_file_path, str_save_workloads_file_path, str_data_set_type))
	{
		std::cout << "get_cfg error" << std::endl;
		exit(0);
	}
	//T vec_QB_workload = get_workload_config_info(config)
	test_query2(config, vec_QB_workload, num_workloads, num_query_per_workload, current_nodeName, client_nbr, str_save_workloads_file_path);
	
	std::cout << "client_task end" << std::endl;

}


#endif // !HELIOS_PART_CLIENT2_H
