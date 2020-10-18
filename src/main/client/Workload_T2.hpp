//
// Created by 古宇文 on 2018/9/29.
// modify in 2020/08/11
//

#ifndef HELIOS_PART_WORKLOAD2_H
#define HELIOS_PART_WORKLOAD2_H

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include "Database.hpp"
//#include "RDF.hpp"

class Workload_T2 {

public:
	Workload_T2(HeliosConfig* cfg,
		const std::vector<int>& pattern,
		const std::vector<std::map<int, std::string>>& replace_instance, int workload_nbr, const std::vector<int>& patter_fq) :
		config(cfg),
		vec_pattern(pattern),
		vec_replace_instance(replace_instance),
		nbr_q_per_workload(workload_nbr),
		pattern_frequency(patter_fq) {}

	int init();
	//template <class T>
	int reset_lubm_query_bank(QueryBank & QB_worload, QueryBank& QB);
private:
	int set_file_path();
	int set_direction();

	//template <class T>
	int add_query_to_query_bank(QueryBank& QB_worload, QueryBank& QB, std::string s, int workload_index, int pattern_index, int index) const;

private:
	std::string str_normal_file_path;
	std::vector<int> vec_pattern;//这个是一开始，没有delete之前的query_index;
	//这里vector的顺序是每条语句的顺序，map<a,b>表示的是一条query语句的body可能有多个三元组需要替换,map的a表示的是第几个三元组需要替换
	//map的b，我们保存了两个数据，使用'^'符号切割，前部分用于表示需要替换的是主语或者是宾语（主语用s表示，宾语用o表示），
	// 后部分表示需要替换的对象(比如university),
	std::vector<std::map<int, std::string>> vec_replace_instance;
	//这里基本上跟上面一样，但是有一点不一样，对于每一个需要替换的对象(object)，它可以替换的选择有很多种(比如university0...university100)。
	std::vector<std::map<int, std::vector<std::string>>> vec_replace_instance_result;
	HeliosConfig* config;
	int nbr_q_per_workload;
	std::vector<int> pattern_frequency;

};

int Workload_T2::init()
{
	std::cout << "init begin" << std::endl;
	if (1 != set_file_path())
	{
		std::cout << "set_file_path error" << std::endl;
		return -1;
	}
	if (vec_pattern.size() != vec_replace_instance.size())
	{
		std::cout << "vec_pattern.size()!=vec_replace_instance.size()" << std::endl;
		std::cout << "vec_pattern.size()=" << vec_pattern.size() << std::endl;
		std::cout << "vec_replace_instance.size() =" << vec_replace_instance.size() << std::endl;
		return -2;
	}
	if (vec_pattern.size() < 1)
	{
		std::cout << "vec_pattern.size()<1" << std::endl;
		return -3;
	}
	if (vec_replace_instance.size() < 1)
	{
		std::cout << "vec_replace_instance.size()<1" << std::endl;
		return -4;
	}
	if (1 != set_direction())
	{
		std::cout << "set_direction error" << std::endl;
		return  -5;
	}
	else
	{
		std::cout << "set_direction was successful" << std::endl;
	}

	std::cout << "init end" << std::endl;
	return 1;
}

int Workload_T2::set_file_path()
{
	{
		std::cout << "get_str_normal_file_path begin" << std::endl;
		std::cout << "dict_files.size=" << config->dict_files.size() << std::endl;
		for (auto it = config->dict_files.begin(); it != config->dict_files.end(); ++it)
		{
			if (it->find("str_normal") != std::string::npos)
			{
				str_normal_file_path = *it;
				std::cout << "*it=" << *it << std::endl;
				return 1;
			}
		}
		return -2;
	}
}

int Workload_T2::set_direction()
{

	//这里vector的顺序是每条语句的顺序，map<a,b>表示的是一条query语句的body可能有多个三元组需要替换,map的a表示的是第几个三元组需要替换
	//map的b，我们保存了两个数据，使用'^'符号切割，前部分用于表示需要替换的是主语或者是宾语（主语用s表示，宾语用o表示），
	// 后部分表示需要替换的对象(比如university),
	// std::vector<std::map<int,std::string>> vec_replace_instance;
	//这里基本上跟上面一样，但是有一点不一样，对于每一个需要替换的对象(object)，它可以替换的选择有很多种(比如university0...university100)。
	// std::vector<std::map<int,std::vector<std::string>>> vec_replace_instance_result;
	for (int i = 0; i < vec_replace_instance.size(); ++i)
	{
		std::cout << "vec_replace_instance_i=" << i << std::endl;
		std::map<int, std::vector<std::string>> map_result;
		//deal with non-select query
		if (vec_replace_instance[i].size() == 1 && vec_replace_instance[i].begin()->second == "non-select")
		{
			std::cout << "set_direction_non_select" << std::endl;
			std::vector<std::string> vec_tmp1;
			std::map<int, std::vector<std::string>> map_tmp2;
			map_tmp2.insert({ 0,vec_tmp1 });
			vec_replace_instance_result.push_back(map_tmp2);
		}
		else
		{
			for (auto it = vec_replace_instance[i].begin(); it != vec_replace_instance[i].end(); ++it) {

				std::cout << "get_direction begin" << std::endl;
				std::cout << "str_normal_file_path=" << str_normal_file_path << std::endl;
				std::string str_replace_object = (it->second).substr(2);
				if (!str_replace_object.find('\t')) {
					std::cout << "tab was not found in s" << std::endl;
					return -1;
				}

				std::ifstream inf(str_normal_file_path);
				if (!inf.is_open()) {
					std::cout << "str_normal_file_path error" << std::endl;
					std::cout << "str_normal_file_path=" << str_normal_file_path << std::endl;
					inf.close();
					return -2;
				}

				std::vector<std::string> vec_s;
				//base on '\t' split str_replace_object
				boost::split(vec_s, str_replace_object, boost::is_any_of("\t"), boost::token_compress_on);
				if (vec_s.size() != 2)
				{
					std::cout << "vec_s.size=" << vec_s.size() << std::endl;
					std::cout << "str_replace_object=" << str_replace_object << std::endl;
					std::cout << "it->second" << it->second << std::endl;
					return -3;
				}
				std::string object, string_to_id;
				std::vector<std::string> vec_result;
				std::cout << "process object" << std::endl;
				//TODO 待优化可能str_replace_object 可以分为超过两段
				while (inf >> object >> string_to_id)
				{

					if (object.find(vec_s[0]) == std::string::npos)
					{
						continue;
					}
					else
					{
						if (object.find(vec_s[1]) == std::string::npos)
						{
							continue;
						}
						else
						{
							std::string str_check = object.substr(vec_s[0].size(), object.size() - (vec_s[0].size() + vec_s[1].size()));
							int is_true = 1;
							for (int l = 0; l < str_check.size(); ++l) {
								int tmp = (int)str_check[l];
								//Determine if it is a numeric type
								if (tmp >= 48 && tmp <= 57)
								{
									;
								}
								else {
									is_true = 0;
								}
							}
							if (is_true == 1)
							{
								vec_result.push_back(object);
							}
						}
					}
				}
				inf.close();
				if (vec_result.size() == 0)
				{
					std::cout << "not replaced" << std::endl;
				}
				map_result.insert({ it->first,vec_result });
			}
			vec_replace_instance_result.push_back(map_result);
		}
	}

	for (int i = 0; i < vec_replace_instance_result.size(); ++i)
	{
		std::cout << "replace_pattern_no=" << vec_pattern[i] << std::endl;
		for (auto it = vec_replace_instance_result[i].begin(); it != vec_replace_instance_result[i].end(); ++it)
		{
			std::cout << "\treplace_instance_no=" << it->first << std::endl;
			std::cout << "\treplace_instance_nbr=" << it->second.size() << std::endl;
		}
	}
	std::cout << "set_derection end" << std::endl;
	return 1;

}

/***********************************************************
 *
 * @tparam T  RDF_query_bank
 * @param   QB_worload
 * @param   QB
 * pattern_frequency    每个pattern的频率是多少
 * @return
 */
//template <class T>
int Workload_T2::reset_lubm_query_bank(QueryBank& QB_worload, QueryBank& QB)
{
	std::cout << "reset_lubm_query_bank begin" << std::endl;

	//std::cout << "QB.Delete" << std::endl;
	for (int j = QB.get_qlist_size() - 1; j >= 0; j--)
	{

		if (1 != QB.delelet_qList(j))
		{
			std::cout << "QB.delelet_qList error" << std::endl;
			return -3;
		}
	}
	//std::cout << "QB Delete finish! left QB size = " << QB.get_qlist_size() << std::endl;

	std::cout << "QB_worload.Delete" << std::endl;
	//delete query in vec_pattern [1,6,7,8,11]
	for (int i = QB_worload.get_qlist_size() - 1; i >= 0; i--) {
		if (find(vec_pattern.begin(), vec_pattern.end(), i) == vec_pattern.end())
		{
			if (1 != QB_worload.delelet_qList(i))
			{
				std::cout << "QB_worload.delelet_qList error" << std::endl;
				return -4;
			}
		}
	}
	//std::cout << "QB_worload.Delete finish! left QB_worload size = " << QB_worload.get_qlist_size() << std::endl;
	
	/*for (int i = 0; i < QB_worload.get_qlist_size(); i++) {
		QB_worload.getQuery(i).printQueryInfo();
	}*/
	std::vector<int> vec_rand;

	for (int j = 0; j < nbr_q_per_workload; ++j)
	{
		//std::cout << "j" << j << std::endl;
		float tmp_count = 0.0;
		int pattern_index = 0;
		//function : base pattern_frequency select pattern
		for (; pattern_index < vec_pattern.size(); ++pattern_index)
		{
			tmp_count += pattern_frequency[pattern_index] * nbr_q_per_workload / 100.0;
			if (tmp_count <= 0.0)
				continue;
			
			if (tmp_count >= float(j))
				break;

		}
		if (pattern_index >= vec_replace_instance.size())
		{
			std::cout << "tmp_coutn=" << tmp_count << std::endl;
			std::cout << "pattern_index" << pattern_index << std::endl;
			std::cout << "pattern_index =" << std::endl;
			std::cout << "vec_replace_instance.size=" << vec_replace_instance.size() << std::endl;
			std::cout << "nbr_q_per_workload=" << nbr_q_per_workload << std::endl;
			std::cout << "pattern_error" << std::endl;
			return -6;
		}
		//    std::vector<std::map<int,std::string>> vec_replace_instance;
	//deal with non-select query
		//std::cout << "pattern_index=" << pattern_index << std::endl;
		if (vec_replace_instance[pattern_index].size() == 1 && vec_replace_instance[pattern_index].begin()->second == "non-select")
		{
			//std::cout << "non-select" << std::endl;
			Query q_set = QB_worload.getQuery(pattern_index);
			q_set.query_pattern = vec_pattern[pattern_index];
			QB.add_qlist_element(q_set);
		}
		else
		{
			int k_rand = rand() % vec_replace_instance[pattern_index].size();
			//cout << "k_rand = " << k_rand << "vec_replace_instance[pattern_index].size() = " << vec_replace_instance[pattern_index].size() << endl;
			std::vector<int> vec_tmp;
			for (auto it_ri = vec_replace_instance[pattern_index].begin(); it_ri != vec_replace_instance[pattern_index].end(); ++it_ri) {
				vec_tmp.push_back(it_ri->first);
			}
			if (1 != add_query_to_query_bank(QB_worload, QB, vec_replace_instance[pattern_index].at(vec_tmp[k_rand]).substr(0, 1),
				j, pattern_index, vec_tmp[k_rand]))
			{
				std::cout << "add_query_to_query_bank error" << std::endl;
				return -5;
			}
		}


	}
	std::cout << "qlist.size=" << QB.get_qlist_size() << std::endl;
	QB.qlist_random_shuffle();
	std::cout << "reset_lubm_query_bank end" << std::endl;
	return 1;
}
/*************************
 *
 * @tparam T
 * @param QB_worload
 * @param QB
 * @param s                 这里表示的是要替换的是主语还是宾语
 * @param workload_index    比如要生成1000条workload，这里表示的是目前生成到第workload条
 * @param pattern_index     这里表示的是根据那条query来生成新的query。
 * @param index             这里表示的是要替换第pattern_index条query的body中的第几个三元组。
 * @return
 */
//template <class T>
int Workload_T2::add_query_to_query_bank(QueryBank& QB_worload, QueryBank& QB, std::string s, int workload_index, int pattern_index, int index) const
{
	//std::cout << "add_query_to_query_bank begin" << std::endl;
	Query q_set = QB_worload.getQuery(pattern_index);
	q_set.query_pattern = vec_pattern[pattern_index];
	//   std::cout << "query_pattern=" << q_set.query_pattern << std::endl;
	//   std::cout << "vec_pattern[pattern_index]=" << vec_pattern[pattern_index] << std::endl;
	QB.add_qlist_element(q_set);
	//    std::cout << "pattern_index=" << pattern_index <<std::endl;
	//    std::cout << "index=" << index << std::endl;
	//    std::cout << "size=" << vec_replace_instance_result[pattern_index].at(index).size() << std::endl;
	/*for (auto it = vec_replace_instance_result[pattern_index].begin(); it != vec_replace_instance_result[pattern_index].end(); ++it) {
		std::cout << "it=" << it->first << " size= " << it->second.size() << std::endl;
	}*/
	RDFterm tmp(Type::URI, vec_replace_instance_result[pattern_index].at(index)[rand() % vec_replace_instance_result[pattern_index].at(index).size()]);
	//    std::cout << "222222" << std::endl;
	if (s == "s")
	{
		//    std::cout << "s" << std::endl;
		RDFtermList subject;
		subject.add(tmp);
		//cout << "subject = " << subject.get(0).getName() << ";predict = " << QB_worload.getQuery(pattern_index).getPattern(index).getPred().get(0).getName() << ";object = " << QB_worload.getQuery(pattern_index).getPattern(index).getObj().get(0).getName() << endl;

		triplePattern p_new(subject, QB_worload.getQuery(pattern_index).getPattern(index).getPred(), QB_worload.getQuery(pattern_index).getPattern(index).getObj());
		QB.reset_query_body(workload_index, QB_worload.getQuery(pattern_index).getPattern(index), p_new);//这里去看具体的query语句是要重置那个tripset
	}
	else if (s == "o")
	{
		//   std::cout << "o" << std::endl;
		RDFtermList object;
		object.add(tmp);
		//cout << "subject = " << QB_worload.getQuery(pattern_index).getPattern(index).getSub().get(0).getName() << ";predict = " << QB_worload.getQuery(pattern_index).getPattern(index).getPred().get(0).getName() << ";object = " << object.get(0).getName() << endl;
		
		triplePattern p_new(QB_worload.getQuery(pattern_index).getPattern(index).getSub(), QB_worload.getQuery(pattern_index).getPattern(index).getPred(), object);
		QB.reset_query_body(workload_index, QB_worload.getQuery(pattern_index).getPattern(index), p_new);//这里去看具体的query语句是要重置那个tripset
	}
	else
	{
		std::cout << "s error" << std::endl;
		return  -1;
	}
	//    std::cout << "query_pattern_test=" << QB.getQuery(QB.get_qlist_size()-1).query_pattern << std::endl;
	//    std::cout << "add_query_to_query_bank end" << std::endl;
	return  1;

}

#endif
