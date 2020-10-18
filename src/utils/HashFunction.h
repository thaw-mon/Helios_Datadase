#ifndef HASHFUNCTION_H_
#define HASHFUNCTION_H_

#include "type.h"
#include <string.h>
#include <stdio.h>

sid_t hash(const void* buffer, unsigned length, sid_t init); 

sid_t hash64(const char* text, sid_t init);

int32_t jumpConsistentHash(sid_t key, int32_t num_servers);

uint64_t hash_u64(uint64_t key);

uint64_t hash_prime_u64(uint64_t upper); 
#endif
