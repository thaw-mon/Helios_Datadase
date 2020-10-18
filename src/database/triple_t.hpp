#ifndef TRIPLE_T_H
#define TRIPLE_T_H

#include <sstream>
#include <string>
extern "C"{
#include "type.h"
}
#include <vector>
#include <map>
#include <cassert>
class QueryNode;
using namespace std;

template <class Type>
Type stringToNum(const std::string& str){
	std::stringstream iss(str);
	Type num;
	iss >> num;
	return num;
}

template <class Type>
string numToString(Type num) {
	std::stringstream iss;
	iss << num;
	string ret = iss.str();
	return ret;
}

struct triple_t{
	sid_t s;
	sid_t p;
	sid_t o;
	dir_t d;
	vector<sid_t> o_vals;

	triple_t() {o_vals.resize(0);}
	
	triple_t(sid_t _s, sid_t _p, sid_t _o, dir_t _d)
		:s(_s), p(_p), o(_o), d(_d)
	{
		o_vals.resize(0);;
	}
	triple_t(sid_t s, sid_t p, sid_t o)
		:s(s), p(p), o(o) 
	{o_vals.resize(0);}
	triple_t(sid_t _s, sid_t _p, dir_t _d)
		:s(_s), p(_p), d(_d) 
	{
		o_vals.resize(0);
		o = 0;
		assert(s == _s && p == _p && d == _d);
	}
	
	/////////////////////////////
	template <typename Archive> void serialize(Archive &ar, const unsigned int version){
			ar & s;	
			ar & p;
			ar & o;	
			ar & d;	
			ar & o_vals;			
		}
		
	bool operator < (const triple_t& rhs) const { 
    return ((s < rhs.s) ||
			((s == rhs.s) && (p < rhs.p)) || 
			((s == rhs.s) && (p == rhs.p) && (d < rhs.d)));
	}
};
struct itriple_t{
	sid_t s;
	sid_t p;
	sid_t o;
	itriple_t(sid_t s, sid_t p, sid_t o)
		:s(s), p(p), o(o){}
	itriple_t():s(0), p(0), o(0){}
};
struct edge_sort_by_spo {
    inline bool operator()(const itriple_t &t1, const itriple_t &t2) {
        if (t1.s < t2.s)
            return true;
        else if (t1.s == t2.s)
            if (t1.p < t2.p)
                return true;
            else if (t1.p == t2.p && t1.o < t2.o)
                return true;
        return false;
    }
};

struct edge_sort_by_ops {
    inline bool operator()(const itriple_t &t1, const itriple_t &t2) {
        if (t1.o < t2.o)
            return true;
        else if (t1.o == t2.o)
            if (t1.p < t2.p)
                return true;
            else if ((t1.p == t2.p) && (t1.s < t2.s))
                return true;
        return false;
    }
};

struct async_pam{
	vector<int> *buf;
	int* executed;

	async_pam(vector<int> *buf, int* executed)
		:buf(buf), executed(executed){}
	~async_pam(){}
};

struct readset_pam{
	vector<sid_t> *results;
	int* executed;

	readset_pam(vector<sid_t> *buf, int* executed)
		:results(buf), executed(executed){}
	~readset_pam(){}
};

struct traverse_pam{
	sid_t s;
	sid_t p;
	dir_t d;
	int* executed;
	QueryNode *node;
	QueryNode* root;
	vector<triple_t>* results;

	traverse_pam(sid_t _s, sid_t _p, dir_t _d, int* _executed)
		:s(_s), p(_p), d(_d), executed(_executed){}
	~traverse_pam(){}
};

struct batch_traverse_pam {
	vector<sid_t> s;
	sid_t p;
	dir_t d;
	int* executed;
	QueryNode* node;
	QueryNode* root;
	vector<triple_t>* results;

	batch_traverse_pam(vector<sid_t> _s, sid_t _p, dir_t _d, int* _executed)
		:s(_s), p(_p), d(_d), executed(_executed) {}
	
	~batch_traverse_pam() {}
};


struct getloc_pam{
	map<int, vector<sid_t>> *v_locs;
	int* executed;
	sid_t vid;
	int connectto_sid ;
	getloc_pam(map<int, vector<sid_t>>* v_locs,
		int* executed, sid_t vid, int connectto_sid)
		:v_locs(v_locs), executed(executed),vid(vid), 
		connectto_sid(connectto_sid)
	{}
	~getloc_pam(){}
};

//add get_loc_pam2
struct getloc_pam2 {
	map<int, vector<sid_t>>* v_locs;
	int* executed;
	vector<sid_t> vids;
	int connectto_sid;
	getloc_pam2(map<int, vector<sid_t>>* v_locs,
		int* executed, vector<sid_t> vids, int connectto_sid)
		:v_locs(v_locs), executed(executed), vids(vids),
		connectto_sid(connectto_sid)
	{}
	~getloc_pam2() {}
};

struct getloc_pam3 {
	map<sid_t,int>* v_locs;
	int* executed;
	sid_t vid;
	int connectto_sid;
	getloc_pam3(map<sid_t, int>* v_locs,
		int* executed, sid_t vid, int connectto_sid)
		:v_locs(v_locs), executed(executed), vid(vid),
		connectto_sid(connectto_sid)
	{}
	~getloc_pam3() {}
};
/*struct reqlock_pam{
	vector<sid_t> *failed_set;
	int * executed;
	int vid;

	reqlock_pam(vector<sid_t>* failed_set, int* executed, int vid)
		:failed_set(failed_set), executed(executed), vid(vid){}
};*/

const char* intToChar(sid_t num);

vector<sid_t> intersect(const vector<sid_t> &set1, const vector<sid_t> &set2);

int sort_rem_dup(vector<sid_t> *set);

vector<sid_t> difference(vector<sid_t> &set1, vector<sid_t> &set2);

void dedup_triples(vector<triple_t> &triples);

vector<sid_t> deserial_vector(char* bytes, int len);

void deserial_vector(char* bytes, int len, vector<sid_t>* sets);

uint64_t get_usec();

void cpu_relax(int u);
#endif
