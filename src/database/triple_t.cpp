#include "triple_t.hpp"
#include <algorithm>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <emmintrin.h>
const char* intToChar(sid_t num){
	std::string temp_str = std::to_string(num);
 	char* array = new char[temp_str.length()+1];
	strcpy(array, temp_str.c_str());
	return array;
}

/*set1 and set2 are sorted*/ //获取集合的交集
vector<sid_t> intersect(const vector<sid_t> &set1, const vector<sid_t> &set2){
	int len = 0, m = 0;
	size_t len1 = set1.size(), len2 = set2.size();
	vector<sid_t> results;
	vector<sid_t> smaller, larger;
	size_t s_len, l_len;
	if(len1 > len2){
		s_len = len2; l_len = len1;
		smaller = set2; larger = set1;
	}else{
		s_len = len1; l_len = len2;
		smaller = set1; larger = set2;
	}

	results.resize(s_len);
	for(int i = 0; i < s_len; i++){
		sid_t v_1 = smaller[i];	
		for(int j = m; j < l_len; j++){
			sid_t v_2 = larger[j];
			if(v_1 < v_2)
				break;
			if(v_1 == v_2){
				results[len] = v_1;
				len++;
				m = j + 1;
				break;
			}
		}
	}
	results.resize(len);	
	return results;
}

vector<sid_t> deserial_vector(char* bytes, int len){
	vector<sid_t> sets = vector<sid_t>();
	for(int i = 0; i < len; ){
		sets.push_back(deserial_vid(bytes, i));
		i += NBYTES_VID;
	}
	//printf("o_vals size %d\n", sets.size());
	return sets;
}

void deserial_vector(char* bytes, int len, vector<sid_t>* sets){
	for(int i = 0; i < len; ){
		sets -> push_back(deserial_vid(bytes, i));
		i += NBYTES_VID;
	}
	//printf("o_vals size %d\n", sets.size());
}

int sort_rem_dup(vector<sid_t> *set){
	int set_size = set->size();
	if(set_size < 1)
		return set_size;

	sort(set->begin(), set->end());

	int prev_index = 0;
	sid_t prev_ele = (*set)[prev_index];	
	for(int i = 1; i < set_size; i++){
		sid_t cur_ele = (*set)[i];
		if(cur_ele == prev_ele)
			continue;
		prev_index++;
		if(i != prev_index){
			(*set)[prev_index] = cur_ele;
		}
		prev_ele = (*set)[prev_index]; //这条语句原来缺失了，导致结果出现问题
	}
	prev_index++;
	set->resize(prev_index);
	return prev_index;
}

/* set 1 - set2*/
//返回set1中存在但是set2中不存在的元素集合，即为仅在set1中存在的元素
vector<sid_t> difference(vector<sid_t> &set1, vector<sid_t> &set2){
	int sz1 = set1.size(), sz2 = set2.size();
	if(sz1 < 1)
		return vector<sid_t>();
	else if(sz1 > 0 && sz2 < 1)
		return set1;

	sort(set1.begin(), set1.end());
	sort(set2.begin(), set2.end());
	vector<sid_t> res;

	int m = 0;
	for(int i = 0; i < sz1; i++){
		sid_t cur_ele = set1[i];
		bool found = false;
		for(int j = m; j < sz2; j++){
			if(cur_ele == set2[j]){
				m = j;
				found = true;
				break;
			}
			if(set2[j] > cur_ele){
				res.push_back(cur_ele);
				m = j;
				found = true;
				break;
			}
		}
		if(!found)
			res.push_back(cur_ele);
	}
	return res;
}

void dedup_triples(vector<triple_t> &triples){
	if (triples.size() <= 1)
		return;

	uint64_t n = 1;
	for (uint64_t i = 1; i < triples.size(); i++) {
		if (triples[i].s == triples[i - 1].s
		        && triples[i].p == triples[i - 1].p
	        	&& triples[i].d == triples[i - 1].d)
			continue;

		triples[n++] = triples[i];
	}
	triples.resize(n);
}

//return microseconds
uint64_t get_usec() {
	struct timespec tp;
	/* POSIX.1-2008: Applications should use the clock_gettime() function
	   instead of the obsolescent gettimeofday() function. */
	   /* NOTE: The clock_gettime() function is only available on Linux.
		  The mach_absolute_time() function is an alternative on OSX. */
	clock_gettime(CLOCK_MONOTONIC, &tp);
	return ((tp.tv_sec * 1000 * 1000) + (tp.tv_nsec / 1000));
}

void cpu_relax(int u) {
        int t = 166 * u;
        while ((t--) > 0)
            _mm_pause(); // a busy-wait loop
    }
