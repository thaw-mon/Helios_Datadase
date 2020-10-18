//
// Created by 古宇文 on 2018/9/30.
//

#ifndef HELIOS_PART_WATDIC_WORKLOAD2_H
#define HELIOS_PART_WATDIC_WORKLOAD2_H

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <map>
#include <dirent.h>
#include <unistd.h>


class Watdic_workload_T2
{
public:
    Watdic_workload_T2(std::string str_fp,std::vector<int> vec_patt,int workload_nbr,std::vector<int> pattern_fq):
                str_file_path(str_fp),
                vec_pattern(vec_patt),
                nbr_q_per_workload(workload_nbr),
                pattern_frequency(pattern_fq)
                {}

    int init();
    template <class T>
            int set_query(T & QB);
private:
    void sort_vec_pattern();
    int set_replace_triple_pattern_file_path();
    int set_replace_triple_pattern();
    int set_query_head(Query &p,int pattern_index, int k);
    int set_query_body(Query &p,int pattern_index, int k);
    template <class T>
        int add_query_to_QB(T & QB, int pattern_index);//这里的pattern_index指的是vec_pattern中按从小到大排序后的第pattern_index的索引
private:
    std::string str_file_path;
    std::vector<int> vec_pattern;//query3,query8,query9{3,8,9}
    int nbr_q_per_workload;
    std::vector<std::string> vec_str_file_path;
    std::map<int,std::vector<std::vector<std::string>>> map_query;
    std::vector<int> pattern_frequency;
};

int Watdic_workload_T2::init()
{
    sort_vec_pattern();
    if (1!= set_replace_triple_pattern_file_path())
    {
        std::cout << "set_replace_triple_pattern_file_path failed" << std::endl;
        return  -1;
    }

    if (1!=set_replace_triple_pattern())
    {
        std::cout << "set_replace_triple_pattern failed" << std::endl;
        return  -2;
    }
    return  1;
}

void Watdic_workload_T2::sort_vec_pattern()
{
    std::sort(vec_pattern.begin(),vec_pattern.end(),std::less<int>());
}
int Watdic_workload_T2::set_replace_triple_pattern_file_path()
{
    std::cout << "set_replace_triple_pattern_file_path begin " << std::endl;
    DIR *dir;
    struct dirent *ptr;
    //char base[1000];

    if ((dir=opendir(str_file_path.c_str())) == NULL)
    {
        perror("Open dir error...");
        closedir(dir);
        return -1;
    }

    while ((ptr=readdir(dir)) != NULL)
    {
        if(ptr->d_name == "." || ptr->d_name == "..")    ///current dir OR parrent dir
            continue;
        if(ptr->d_type == 8)    ///file
        {

          //  std::cout << "d_name=" << ptr->d_name << std::endl;
            std::string str_tmp_name =ptr->d_name;
            std::string str_split_name = "query_workload_Q";
            int file_name_index = std::stoi(str_tmp_name.substr(str_split_name.size()));
            if (std::find(vec_pattern.begin(),vec_pattern.end(),file_name_index)!=vec_pattern.end())
            {
                std::cout << "input index = " << file_name_index << std::endl;
                vec_str_file_path.push_back(str_file_path+"/"+ptr->d_name);
            }

        }

    }
    closedir(dir);
    std::cout << "sort" << std::endl;
    std::sort(vec_str_file_path.begin(),vec_str_file_path.end(),[](std::string a,std::string b){
        std::string str_tmp_workload= "query_workload_Q";
       // std::cout << "a.substr(a.rfind()+1)=" << a.substr(a.rfind("/")+1+str_tmp_workload.size()) << std::endl;
       // std::cout << "std::stoi(b.substr(b.rfind()+1))=" << b.substr(b.rfind("/")+1+str_tmp_workload.size()) << std::endl;
        return std::stoi(a.substr(a.rfind("/")+1+str_tmp_workload.size()))<std::stoi(b.substr(b.rfind("/")+1+str_tmp_workload.size()));
    });
    std::cout << "file" << std::endl;
    for (int i = 0; i < vec_str_file_path.size() ; ++i) {
        std::cout << "file_name = " << vec_str_file_path[i] << std::endl;
    }
    std::cout << "set_replace_triple_pattern_file_path finish" << std::endl;
    return 1;
}

int Watdic_workload_T2::set_replace_triple_pattern()
{
    for (int i = 0; i < vec_str_file_path.size(); ++i) {
        std::vector<std::vector<std::string>> vec_tmp;
        std::ifstream ifd(vec_str_file_path[i]);
        if (!ifd.is_open())
        {
            std::cout << "open file failed" << std::endl;
            ifd.close();
            return -1;
        }

        std::string s;
        std::vector<std::string> vec_instance;
        while (getline(ifd,s))
        {
            if (s.length()== 0)
            {
                vec_tmp.push_back(vec_instance);
                vec_instance.clear();
            }
            else {
                if (s.substr(0,1)=="\t")
                {
                    vec_instance.push_back(s.substr(1));
                }
                else
                {
                    vec_instance.push_back(s);
                }

            }

        }
        map_query.insert({vec_pattern[i],vec_tmp});
    }
    for (auto it = map_query.begin();  it!=map_query.end() ; ++it)
    {
        std::cout << "it=" << it->first << " size=" << it->second.size() << std::endl;
    }
    return 1;

}

int Watdic_workload_T2::set_query_head(Query &p,int pattern_index, int k)
{
 //   std::cout << "set_query_head begin" << std::endl;
    std::vector <std::string> fields;
    split(fields, map_query.at(pattern_index)[k][0], boost::is_any_of(" "));
    if (fields.size() < 2)
    {
        std::cout << "param= " << map_query.at(pattern_index)[k][0] << std::endl;
        return -1;
    }
    if (fields[0]!="SELECT" || fields[fields.size()-1]!="{")
    {
        std::cout << "!=SELECT" << fields[0] << std::endl;
        std::cout << "!={  " << fields[fields.size()-1] << std::endl;
        return -2;
    }
    for (int i = 1; i < fields.size()-2; ++i) {
       // std::cout << "head param=" << fields[i].substr(1) << std::endl;
        RDFterm t(Type::Variable, fields[i].substr(1));
        p.addToHead(t);
    }
 //   std::cout << "set_query_head end" << std::endl;
    return 1;
}

int Watdic_workload_T2::set_query_body(Query &p, int pattern_index, int k)
{
 //   std::cout << "set_query_body begin " << std::endl;
    for (int  i = 1; i< map_query.at(pattern_index)[k].size()-1; ++i)
    {
        std::vector <std::string> fields;
        split(fields, map_query.at(pattern_index)[k][i], boost::is_any_of(" "));
        if (fields.size()!=4)
        {
            std::cout << "error  fields.size()=" << fields.size() << std::endl;
            return  -1;
        }
        std::vector<RDFtermList> vec_rdftermlist;
        for (int j = 0; j < fields.size()-1; ++j) {
            RDFtermList tmp;
            auto find_index = fields[j].find("?");
            if (find_index!=std::string::npos)
            {
                RDFterm t(Type::Variable,fields[j].substr(find_index+1));
                tmp.add(t);
                vec_rdftermlist.push_back(tmp);
            }
            else
            {
                RDFterm t (Type::URI,fields[j]);
                tmp.add(t);
                vec_rdftermlist.push_back(tmp);
            }
        }
        const std::size_t N_size = 3;
        if (vec_rdftermlist.size() != N_size)
        {
            std::cout << "vec_rdftermlist.size()=" << vec_rdftermlist.size() << std::endl;
            return -2;
        }
        triplePattern body(vec_rdftermlist[0],vec_rdftermlist[1],vec_rdftermlist[2]);
        p.addToBody(body);
    }
   // std::cout << "set_query_body end " << std::endl;
    return 1;
}

template <class T>
    int Watdic_workload_T2::set_query(T &QB)
    {
        std::cout << "set_query begin" << std::endl;
        for (int j=QB.get_qlist_size()-1;j>=0;j--)
        {

            if(1!=QB.delelet_qList(j))
            {
                std::cout << "QB.delelet_qList error" << std::endl;
                return -1;
            }
        }

        for (int i = 1; i <= nbr_q_per_workload; ++i)
        {

            float tmp_count =0.0;
            int pattern_index=0;
            for(;pattern_index< vec_pattern.size();++pattern_index)
            {
                tmp_count += pattern_frequency[pattern_index]*nbr_q_per_workload/100;
                if (tmp_count <=0.0)
                    continue;
               // std::cout << "tmp_coutn=" <<tmp_count << std::endl;
              //  std::cout << "pattern_index" << pattern_index << std::endl;
                if (tmp_count>=i  )
                    break;

            }
            if (add_query_to_QB(QB,pattern_index)!=1)
            {
                std::cout << "add_query_to_QB0.7 failed" << std::endl;
                return  -2;
            }
        }
	if (QB.get_qlist_size()!=nbr_q_per_workload)
	{
		std::cout << "QB.qlist!=nbr_q_per_workload" << std::endl;
	}
        QB.qlist_random_shuffle();
        std::cout << "set_query end" << std::endl;
        return 1;
    }

template <class T>
int Watdic_workload_T2::add_query_to_QB(T & QB, int pattern_index)
{
 //   std::cout << "add_query_to_QB begin" << std::endl;
    Query p;
    if (map_query.at(vec_pattern[pattern_index]).size()==0)
    {
        std::cout << "error map_query.at(pattern_index).size()==0" << std::endl;
        return  -1;
    }
    int rand_index = rand()%map_query.at(vec_pattern[pattern_index]).size();
    if (1!=set_query_head(p,vec_pattern[pattern_index],rand_index))
    {
        std::cout << "set_query_head error" << std::endl;
        return -2;
    }
    if (1!=set_query_body(p,vec_pattern[pattern_index],rand_index))
    {
        std::cout << "set_query_body error" << std::endl;
        return -3;
    }
    p.query_pattern = vec_pattern[pattern_index];
   // std::cout << "p.query_pattern=" << p.query_pattern << std::endl;
    if (p.query_pattern>18)
    {
    	std::cout << "error,p.query_pattern=" << p.query_pattern << std::endl;
    }
    QB.add_head_list(p);
 //   std::cout << "add_query_to_QB end" << std::endl;
    return 1;
}



#endif //HELIOS_PART_WATDIC_WORKLOAD_H

