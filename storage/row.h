#pragma once

#include <cassert>
#include <global.h>


#ifdef USE_INLINED_DATA
class row_t;
#include "row_lock.h"
#include "row_tictoc.h"
#include "row_silo.h"
#endif
#include "row_hekaton.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#if !TPCC_CF
#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		memcpy(&value, data + pos, sizeof(type));\
	}
		// value = *(type *)&data[pos];
#else
#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		int cf_id = get_schema()->get_field_cf_id(col_id);\
		memcpy(&value, cf_data[cf_id] + pos, sizeof(type));\
	}
		// value = *(type *)&cf_data[cf_id][pos];
#endif

class table_t;
class Catalog;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;

class itemid_t;


class row_m{
public:
    //64 bits
    uint64_t meta;

    ~row_m() {};

    row_m() : meta(0) {}
    explicit row_m(uint64_t meta) : meta(meta)  {}

    static const uint64_t kControlMask = uint64_t{0x1} << 63;          // Bits 64
    static const uint64_t kVisibleMask = uint64_t{0x1} << 62;          // Bit 63
    static const uint64_t kKeyLengthMask = uint64_t{0x2FFF} << 48;     // Bits 62-49
    static const uint64_t kTotalLengthMask = uint64_t{0xFFFF} << 32;   // Bits 48-33
    static const uint64_t kOffsetMask = uint64_t{0xFFFFFFFF};          // Bits 32-1

    bool IsNull() const {
        return (meta == std::numeric_limits<uint64_t>::max() && meta == 0);
    }
    bool operator==(const row_m &rhs) const {
        return meta == rhs.meta ;
    }
    bool operator!=(const row_m &rhs) const {
        return !operator==(rhs);
    }
    inline bool IsVacant() { return meta == 0; }
    inline void SetVacant() {  meta == 0; }
    inline bool IsControl() const {
        bool t_i_r = (meta & kControlMask) > 0;
        return t_i_r;
    }
    inline uint32_t GetKeyLength() const { return (uint32_t) ((meta & kKeyLengthMask) >> 48); }
    // Get the padded key length from accurate key length
    inline uint32_t GetPaddedKeyLength() const{
        auto key_length = GetKeyLength();
        return PadKeyLength(key_length);
    }
    inline uint32_t GetTotalLength() { return (uint32_t) ((meta & kTotalLengthMask) >> 32 ); }
    static inline constexpr uint32_t PadKeyLength(uint32_t key_length) {
        return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
    }
    inline uint32_t GetOffset() { return (uint32_t) (meta & kOffsetMask); }
    inline void SetOffset(uint16_t offset) {
        meta = (meta & (~kOffsetMask)) | (uint64_t{offset} << 32);
    }
    inline bool IsVisible() {
        bool i_v = (meta & kVisibleMask) > 0;
        return i_v;
    }
	inline void SetVisible(bool visible) {
		if (visible) {
			meta = meta | kVisibleMask;
		} else {
			meta = meta & (~kVisibleMask);
		}
	}
//    inline void SetControl(bool control) {
//        if (control) {
//            meta = meta | kControlMask;
//        } else {
//            meta = meta & (~kControlMask);
//        }
//    }
    inline bool IsInserting() {
        bool is_insert = !IsVisible() ;
        is_insert = is_insert && IsControl();
        return is_insert;
    }
    inline void PrepareForInsert() {
        assert(IsVacant());
        //is not visible, control is true
        meta = uint64_t{1} << 61;
        meta = meta | kControlMask;
        assert(IsInserting());
    }
    inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint64_t total_size) {
        // Set the visible bit,the actual offset,key length,
        meta =  (key_len << 48) | kVisibleMask | (total_size << 32) | (offset);
        meta = meta & (~kControlMask);

        auto g_k_l = GetKeyLength();
        auto g_t_l = GetTotalLength();
        auto g_v = IsVisible();
        auto g_ctl = IsControl();
        assert(g_k_l == key_len);
        assert(!IsInserting());
    }
};

class row_t
{
public:

	RC init(uint64_t idx_key, table_t * host_table, uint64_t part_id, uint64_t row_id = 0);
	void init(int size);
	RC switch_schema(table_t * host_table);
	// not every row has a manager
	void init_manager(row_t * row);

	table_t * get_table();
	Catalog * get_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();
	uint64_t get_row_id() { return _row_id; };

	void copy(row_t * src);

	void 		set_primary_key(uint64_t key) { _primary_key = key; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_value(int id);
	char * get_value(char * col_name);

	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);
	DECL_SET_VALUE(UInt16);
	DECL_SET_VALUE(SInt16);
	DECL_SET_VALUE(UInt8);
	DECL_SET_VALUE(SInt8);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);
	DECL_GET_VALUE(UInt16);
	DECL_GET_VALUE(SInt16);
	DECL_GET_VALUE(UInt8);
	DECL_GET_VALUE(SInt8);


#if !TPCC_CF
	void set_data(char * data, uint64_t size);
	char * get_data();
#endif

	void free_row();
	
	static size_t alloc_size(table_t* t);
	static size_t max_alloc_size();

	// for concurrency control. can be lock, timestamp etc.
	RC get_row(access_t type, txn_man * txn, row_t *& row, row_t *& org_row );
	void return_row(access_t type, txn_man * txn, row_t * row);

#if CC_ALG == MICA
#if !TPCC_CF
	static RC get_row(access_t type, txn_man * txn, table_t* table, row_t* access_row, uint64_t row_id, void *row_head, uint64_t part_id);
#else
	static RC get_row(access_t type, txn_man * txn, table_t* table, row_t* access_row, uint64_t row_id, uint64_t part_id, const access_t* cf_access_type);
#endif
#endif

private:
	// primary key should be calculated from the data stored in the row.
//	uint64_t 		_primary_key;
//	uint64_t		_part_id;
//	uint64_t 		_row_id;
	
public:
    table_t * table;
	volatile uint8_t			is_deleted;
    #if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
        #if defined(USE_INLINED_DATA)
            Row_lock manager[1];
            #else
            Row_lock * manager;
            #endif
    #elif CC_ALG == TIMESTAMP
        Row_ts * manager;
    #elif CC_ALG == MVCC
        Row_mvcc * manager;
    #elif CC_ALG == HEKATON
        Row_hekaton * manager;
    #elif CC_ALG == OCC
        Row_occ * manager;
      #elif CC_ALG == TICTOC
            #if defined(USE_INLINED_DATA)
            Row_tictoc manager[1];
            #else
            Row_tictoc * manager;
            #endif
      #elif CC_ALG == SILO
            #if defined(USE_INLINED_DATA)
            Row_silo manager[1];
            #else
            Row_silo * manager;
            #endif
      #elif CC_ALG == VLL
        Row_vll * manager;
    #endif
    uint64_t 		_primary_key;
    uint64_t		_part_id;
    uint64_t 		_row_id;
#if AGGRESSIVE_INLINING
	volatile uint8_t			is_updated;
    row_t *next_row; // for the same key
    bool begin_txn;
    bool end_txn;
    ts_t begin;
    ts_t end;
    char data[0];
#else
    #if !TPCC_CF
	   char * data;
    #else
       char * cf_data[4];
    #endif
#endif

	void set_part_id(uint64_t part_id) { _part_id = part_id; }
	void set_row_id(uint64_t row_id) { _row_id = row_id; }

#if defined(USE_INLINED_DATA) && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == SILO || CC_ALG == TICTOC)
	char data[0] __attribute__((aligned(8)));
#else

#endif
};
