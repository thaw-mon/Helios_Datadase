#include "type.h"
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "HashFunction.h"
/*unsigned long hash_index(helios_key* k){
	sid_t	key = 0;
	key += k->vid;
	key <<= NBITS_PID;
	key += k->pid;
	key <<= NBITS_IDX;
	key += k->idx;
	return hash_u64(key);
}*/

/*
 * key = [vid | predicate | IN/OUT] value = [vid, ...] == SET
 * key = [vid | 0  	  | IN/OUT] value = [pid, ...] == SET
 * key = [0   | predicate | IN/OUT] value = [vid, ...] == SET
 * key = [vid | 0     | LOC       | 0     ] value = [sid] == String
 * key = [0   | pid   | LOC       | IN/OUT] value = [sid, ...] == SET
 * key = [vid | 0     | LOCK 	  | 0     ] value = [RW - #; ISREASSIGN - #] == Hash
 * key = [vid | 0     | COUNTER	  | 0	  ] value = [WEIGHT - #; EDGENUM - #] == Hash
 * key = [0   | pid   | COUNTER	  | 0	  ] value = [IN - #; OUT - #] == Hash
 * key = [vid | 0     | LOCALITY  | 0	  ] value = [EXISTS - #; TOP - #; WORK - #] == Hash
 * key = [vid | 0     | EDGE_WEI  | 0	  ] value = [vid - #; ] == Hash
 * */

sid_t key_vpid_t(sid_t s, sid_t p, index_t i, dir_t d){
	sid_t	key = 0;
	key += s;
	key <<= NBITS_PID;
	key += p;
	key <<= NBITS_IDX;
	key += i;
	key <<= NBITS_DIR;
	key += d;
	return key;
}

/* As the serial will put small end at the begining,
 * Put in the order of the index, p, s
 * */
/*const char* key_vpid_c(sid_t s, sid_t p, index_t i, dir_t d){
	int cap = NBYTES_SID , pos = 0;
	char* v = (char*)malloc(cap * sizeof(unsigned char));
	pos = serial_put_sid(v, pos, cap, i + d , NBYTES_IDX);
	pos = serial_put_sid(v, pos, cap, p, NBYTES_PID);
	pos = serial_put_sid(v, pos, cap, s, NBYTES_VID);
	assert(pos == cap);
	return v;
}*/

/*helios_key *key_vpid_k(sid_t s, sid_t p, index_t i, dir_t d){
	helios_key *key = (helios_key*)malloc(sizeof(*key));
	//key->vid = s;
	//key->pid = p;
	//key->idx = i+d;
	key->key = key_vpid_t(s, p, i, d);
	//key->len = NBYTES_SID;
	return key;
}*/

/*helios_key *key_cpid_k(int8_t contents[], uint32_t c_pos, uint32_t len,
		sid_t p, index_t i, dir_t d){
	helios_key *key = (helios_key*)malloc(sizeof(*key));
	
	int cap = NBYTES_SID, pos = 0;
	char* v = (char*)malloc(cap * sizeof(unsigned char));
	pos = serial_put_sid(v, pos, cap, i + d , NBYTES_IDX);
	pos = serial_put_sid(v, pos, cap, p, NBYTES_PID);
	pos = serial_put_array(v, pos, cap, contents, c_pos, len);
	assert(pos <= cap);
	key->key = v;
	key->len = NBYTES_SID;
	return key;
}*/
/*
 * Put the order as key_vpid_c
 * */
/*const char* key_cpid_c(const char* c, int c_pos, 
		sid_t p, index_t i, dir_t d){
	int cap = NBYTES_SID, pos = 0;
	char* v = (char*)malloc(cap * sizeof(unsigned char));
	pos = serial_put_sid(v, pos, cap, i + d , NBYTES_IDX);
	pos = serial_put_sid(v, pos, cap, p, NBYTES_PID);
	pos = serial_put_char(v, pos, cap, c, c_pos, NBYTES_VID);
	assert(pos == cap);
	return v;
}*/

dir_t inverse_dir(dir_t d){
	if(d == OUT)
		return IN;
	if(d == IN)
		return OUT;
}

/*int serial_put_sid(char* serial, int pos, int cap, sid_t v, int t_size){
	assert(pos + t_size <= cap);
	for(int i = 0; i< t_size; i++){
		serial[pos] = (unsigned char)((v >> (8 * i)) & 0xFF);
		pos ++;
	}
	return pos;
}

int serial_put_char(char* serial, int pos, int cap, 
		const char* v, int v_pos, int t_size){
	assert(pos + t_size <= cap);
	for(int i = 0; i< t_size; i++){
		serial[pos] = (unsigned char)v[v_pos + i];
		pos ++;
	}
	return pos;
}

int serial_put_array(char* serial, int pos, int cap, 
		int8_t contents[], int v_pos, int t_size){
	assert(pos + t_size <= cap);
	for(int i = 0; i< t_size; i++){
		serial[pos] = (unsigned char)contents[v_pos + i];
		pos ++;
	}
	return pos;
}
const char* serial_sid(sid_t o, int t_size){
	char* v = (char*)malloc((t_size ) * sizeof(char));
	for(int i = 0; i < t_size; i++){
		v[i] = (char)((o >> (8 * i)) & 0xFF);
	}
	return v;
}

sid_t deserial_sid(const char* bytes, int pos, int t_size){
	sid_t v = 0;
	if(t_size >= NBYTES_IDX){
	    v |= (bytes[pos]) & 0xFF;
	    if(t_size >= NBYTES_PID){
	        v |= (bytes[pos+1] << 8)  & 0xFF00;
	    	if(t_size >= NBYTES_VID){
		    v |= (bytes[pos+2] << 16) & 0xFF0000;
		    v |= (bytes[pos+3] << 24) & 0xFF000000;
		    v |= ((sid_t)bytes[pos+4] << 32) & 0xFF00000000;
		    if(t_size >= NBYTES_SID){		
			v |= ((sid_t)bytes[pos+5] << 40) & 0xFF0000000000;
			v |= ((sid_t)bytes[pos+6] << 48) & 0xFF000000000000;
			v |= ((sid_t)bytes[pos+7] << 56) & 0xFF00000000000000;
	            }
		}
	    }
	}
	return v;
}*/

const char* serial_vid(sid_t o){
	int t_size = NBYTES_VID;
	char* v = (char*)malloc((t_size ) * sizeof(char));
	for(int i = 0; i < t_size; i++){
		v[i] = (char)((o >> (8 * i)) & 0xFF);
	}
	return v;
}

sid_t deserial_vid(const char* bytes, int pos){
	sid_t v = 0;
	int t_size = NBYTES_VID;
	if(t_size >= 4){
	    v |= (bytes[pos]) & 0xFF;
	    v |= (bytes[pos+1] << 8)  & 0xFF00;
	    v |= (bytes[pos+2] << 16) & 0xFF0000;
	    v |= (bytes[pos+3] << 24) & 0xFF000000;
	    if(t_size >= 5){
	        v |= ((sid_t)bytes[pos+4] << 32) & 0xFF00000000;
		if(t_size >= 6){		
			v |= ((sid_t)bytes[pos+5] << 40) & 0xFF0000000000;
		    if(t_size>= 7){
			v |= ((sid_t)bytes[pos+6] << 48) & 0xFF000000000000;
			if(t_size>=8)
			      v |= ((sid_t)bytes[pos+7] << 56) & 0xFF00000000000000;
		    }
	        }
	    }
	}
	return v;
}
