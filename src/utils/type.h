#ifndef TYPE_H_
#define TYPE_H_

#include <stdint.h>
#include <string.h>
#include <math.h>
//#include <sstream>

#ifdef DTYPE_32Bit
typedef uint32_t sid_t; // hash id
typedef int32_t ssid_t; //string id

#else 
typedef uint64_t sid_t; // hash id
typedef int64_t ssid_t; //string id
#endif

typedef enum {IN=0, OUT, BID}dir_t;

enum {NBITS_PID = 17};
enum {NBITS_IDX = 6}; 
enum {NBITS_DIR = 1};
enum {NBITS_VID = 64 - NBITS_PID - NBITS_IDX - NBITS_DIR};

enum {NBYTES_VID = 5};//ceil(NBITS_VID/8);

//vertex or server counter
enum {WEIGHT = 0};
enum {EDGENUM = 1};
enum {OBJNUM = 2};
enum {PREV_REASSIGN_WEIGHT = 3};
enum { VERTEX_WEIGHT = 4 };

//vertex locality
enum {EXISTS = 2};
enum {WORKLOAD = 1};
enum {TOP = 0};

typedef enum{
	RW_LOCK = 0,
	REASSIGN_LOCK
}lock_t;

static const char* SERVER_INFO = "SERVER_INFO"; //SET
static const char* SERVER_ALL = "SERVER_ALL"; // Hashi
static int NUM_LOG = 4;
static const char* ACTIVE_OBJECT = "ACTIVE_OBJECT";
static const char* ACTIVE_OBJECT_SIZE = "ACTIVE_OBJECT_SIZE";
static const char* ACTIVE_OBJECT_2 = "ACTIVE_OBJECT_2";
static const char* ACTIVE_OBJECT_3 = "ACTIVE_OBJECT_3";
static const char* ACTIVE_OBJECT_4 = "ACTIVE_OBJECT_4";
static const char* EDGELOG = "EDGE_LOG";
static const char* EDGELOG_1 = "EDGE_LOG_1";//zset or List
static const char* EDGELOG_2 = "EDGE_LOG_2";//zset or List
static const char* EDGELOG_3 = "EDGE_LOG_3";//zset or List
static const char* EDGELOG_4 = "EDGE_LOG_4";//zset or List
//static const char* DROPED_EDGELOG = "DROPED_EDGELOG";
//static const char* EDGEWEIGHT_COUNTER = "EDGEWEIGHT_COUNTER";//string
//static const int EDGELOG_LEN = 100000;

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

/*only allows 0 - (2^6 - 1)*/
typedef enum {
	NO_INDEX = 0,
	LOC,
	LOCK,
	COUNTER,
	LOCALITY,
	EDGE_WEIGHT, // Redis HashMap and List together
	INDEX,
	DoI_INDEX,
	REASSIGN_DEL //for some v reassign to anthor node but v_log need to del 
}index_t;

sid_t key_vpid_t(sid_t s, sid_t p, index_t i, dir_t d);

/*const char* key_vpid_c(sid_t s, sid_t p, index_t i, dir_t d);
	
const char* key_cpid_c(const char* c, int c_pos, 
		sid_t p, index_t i, dir_t d);*/

dir_t inverse_dir(dir_t d);

/*int serial_put_sid(char* serial, int pos, int cap, sid_t v, int t_size);

int serial_put_char(char* serial, int pos, int cap, 
		const char* v, int v_pos, int t_size);
int serial_put_array(char* serial, int pos, int cap, 
		int8_t contents[], int v_pos, int t_size);*/

const char* serial_vid(sid_t o);

sid_t deserial_vid(const char *bytes, int pos);


#endif
