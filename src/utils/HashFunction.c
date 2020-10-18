#include "HashFunction.h"

const unsigned goldenRatioHigh = 0x9e3779b9;
const unsigned goldenRatioLow = 0x7f4a7c13;


#define mix64(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>43); \
  b -= c; b -= a; b ^= (a<<9); \
  c -= a; c -= b; c ^= (b>>8); \
  a -= b; a -= c; a ^= (c>>38); \
  b -= c; b -= a; b ^= (a<<23); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>35); \
  b -= c; b -= a; b ^= (a<<49); \
  c -= a; c -= b; c ^= (b>>11); \
  a -= b; a -= c; a ^= (c>>12); \
  b -= c; b -= a; b ^= (a<<18); \
  c -= a; c -= b; c ^= (b>>22); \
}

sid_t hash(const void* buffer, unsigned length, sid_t init) {

	sid_t goldenRatio =
		(((sid_t) (goldenRatioHigh) << 32) | goldenRatioLow);
	
	register const unsigned char* key =
			(const unsigned char*) (buffer);
	register sid_t a = init, b = init, c = goldenRatio;
	// Hash the main part
	while (length >= 24) {
		a += ((sid_t) (key[0])) | ((sid_t) (key[1])
				<< 8) | ((sid_t) (key[2]) << 16)
				| ((sid_t) (key[3]) << 24)
				| ((sid_t) (key[4]) << 32)
				| ((sid_t) (key[5]) << 40)
				| ((sid_t) (key[6]) << 48)
				| ((sid_t) (key[7]) << 56);
		b += ((sid_t) (key[8])) | ((sid_t) (key[9])
				<< 8) | ((sid_t) (key[10]) << 16)
				| ((sid_t) (key[11]) << 24)
				| ((sid_t) (key[12]) << 32)
				| ((sid_t) (key[13]) << 40)
				| ((sid_t) (key[14]) << 48)
				| ((sid_t) (key[15]) << 56);
		c += ((sid_t) (key[16]))
				| ((sid_t) (key[17]) << 8)
				| ((sid_t) (key[18]) << 16)
				| ((sid_t) (key[19]) << 24)
				| ((sid_t) (key[20]) << 32)
				| ((sid_t) (key[21]) << 40)
				| ((sid_t) (key[22]) << 48)
				| ((sid_t) (key[23]) << 56);
		mix64(a, b, c);
		key += 24;
		length -= 24;
	}
	// Hash the tail (max 23 bytes)
	c += length;
	switch (length) {
		case 23:
			c += (sid_t) (key[22]) << 56;
		case 22:
			c += (sid_t) (key[21]) << 48;
		case 21:
			c += (sid_t) (key[20]) << 40;
		case 20:
			c += (sid_t) (key[19]) << 32;
		case 19:
			c += (sid_t) (key[18]) << 24;
		case 18:
			c += (sid_t) (key[17]) << 16;
		case 17:
			c += (sid_t) (key[16]) << 8;
		case 16:
			b += (sid_t) (key[15]) << 56;
		case 15:
			b += (sid_t) (key[14]) << 48;
		case 14:
			b += (sid_t) (key[13]) << 40;
		case 13:
			b += (sid_t) (key[12]) << 32;
		case 12:
			b += (sid_t) (key[11]) << 24;
		case 11:
			b += (sid_t) (key[10]) << 16;
		case 10:
			b += (sid_t) (key[9]) << 8;
		case 9:
			b += (sid_t) (key[8]);
		case 8:
			a += (sid_t) (key[7]) << 56;
		case 7:
			a += (sid_t) (key[6]) << 48;
		case 6:
			a += (sid_t) (key[5]) << 40;
		case 5:
			a += (sid_t) (key[4]) << 32;
		case 4:
			a += (sid_t) (key[3]) << 24;
		case 3:
			a += (sid_t) (key[2]) << 16;
		case 2:
			a += (sid_t) (key[1]) << 8;
		case 1:
			a += (sid_t) (key[0]);
		default:
			break;
	}
	mix64(a, b, c);

	return c;
}

sid_t hash64(const char* text, sid_t init) {
	return hash(text, strlen(text), init);
}

int32_t jumpConsistentHash(sid_t key, int32_t num_servers){
	sid_t b = -1, j = 0;
	while (j < num_servers){
		b = j;
		key = key * 2862933555777941757ULL + 1;
		j = (b + 1) * ((double)(1LL << 31) / (double)((key >> 33) + 1));
	}
	return b;
}
//
//Thomas wang's 64 bit hash function
uint64_t hash_u64(uint64_t key) {
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
 }
uint64_t hash_prime_u64(uint64_t upper) {
        if (upper >= (1l << 31)) {
            printf( "WARNING: %d is too large\n", upper);
            return upper;
        }

        if (upper >= 1610612741l) return 1610612741l;     // 2^30 ~ 2^31
        else if (upper >= 805306457l) return 805306457l;  // 2^29 ~ 2^30
        else if (upper >= 402653189l) return 402653189l;  // 2^28 ~ 2^29
        else if (upper >= 201326611l) return 201326611l;  // 2^27 ~ 2^28
        else if (upper >= 100663319l) return 100663319l;  // 2^26 ~ 2^27
        else if (upper >= 50331653l) return 50331653l;    // 2^25 ~ 2^26
        else if (upper >= 25165843l) return 25165843l;    // 2^24 ~ 2^25
        else if (upper >= 12582917l) return 12582917l;    // 2^23 ~ 2^24
        else if (upper >= 6291469l) return 6291469l;      // 2^22 ~ 2^23
        else if (upper >= 3145739l) return 3145739l;      // 2^21 ~ 2^22
        else if (upper >= 1572869l) return 1572869l;      // 2^20 ~ 2^21
        else if (upper >= 786433l) return 786433l;        // 2^19 ~ 2^20
        else if (upper >= 393241l) return 393241l;        // 2^18 ~ 2^19
        else if (upper >= 196613l) return 196613l;        // 2^17 ~ 2^18
        else if (upper >= 98317l) return 98317l;          // 2^16 ~ 2^17

        printf( "WARNING: %d is too small\n", upper);
        return upper;
    }
