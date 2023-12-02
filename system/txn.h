#pragma once

#include "global.h"
#include "helper.h"
#include <unordered_map>
#include <atomic>
#include "manager.h"
#include "row_hekaton.h"
#include "row.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;
class ARRAY_INDEX;
class ORDERED_INDEX;
class HASH_INDEX;
//class IndexMBTree;
class IndexBtree;
// each thread has a txn_man.
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class Access {
public:
#if CC_ALG != MICA
	access_t 	type;
	row_t * 	orig_row;
    ORDERED_INDEX* idx;
    uint64_t    pky_key;
    itemid_t *  idx_location;
#endif
	row_t * 	data;
#if CC_ALG != MICA
	row_t * 	orig_data;
#endif
	// void cleanup();
#if CC_ALG == TICTOC
	ts_t 		wts;
	ts_t 		rts;
#elif CC_ALG == SILO
	ts_t 		tid;
	ts_t 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;
#endif

};

class txn_man
{
public:
	virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
	void release();
	thread_t * h_thd;
	workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;

	virtual RC 		run_txn(base_query * m_query) = 0;
	uint64_t 		get_thd_id();
	workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
#if CC_ALG == HEKATON
	void * volatile history_entry;
#endif
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready;
	// [HSTORE]
	int volatile 	ready_part;

    //    template <typename Func>
    //	RC 				finish(RC rc,  const Func& func);
    template <typename Func>
    RC finish(RC rc, const Func& func) {
            #if CC_ALG == HSTORE
                    rc = apply_index_changes(rc);
                return rc;
            #endif
                    // uint64_t starttime = get_sys_clock();
            #if CC_ALG == OCC
                    if (rc == RCOK)
                    rc = occ_man.validate(this);
                else
                    cleanup(rc);
            #elif CC_ALG == TICTOC
                    if (rc == RCOK)
                    rc = validate_tictoc();
                else
                    cleanup(rc);
            #elif CC_ALG == SILO
                    if (rc == RCOK)
                    rc = validate_silo();
                else
                    cleanup(rc);
            #elif CC_ALG == HEKATON
                   rc = validate_hekaton(rc, func);
                   cleanup(rc);
            #elif CC_ALG == MICA
                    if (rc == RCOK) {
                        if (mica_tx->has_began()) {
            #if AGGRESSIVE_INLINING

                            rc = mica_tx->commit(func) ? RCOK : Abort;
            #else
                            rc = mica_tx->commit(func) ? RCOK : Abort;
                            rc = apply_index_changes(rc);
        //    #else
        //                    auto write_func = [this]() { return apply_index_changes(RCOK) == RCOK; };
        //                    rc = mica_tx->commit(NULL, write_func) ? RCOK : Abort;
        //                    if (rc != RCOK) rc = apply_index_changes(rc);
            #endif
                        } else {
                            rc = RCOK;
                        }
                    } else if (mica_tx->has_began() && !mica_tx->abort())
                        assert(false);
                    cleanup(rc);
            #else
                    rc = apply_index_changes(rc);
                cleanup(rc);
            #endif

            // uint64_t timespan = get_sys_clock() - starttime;
            // INC_TMP_STATS(get_thd_id(), time_man,  timespan);
            // INC_STATS(get_thd_id(), time_cleanup,  timespan);
            return rc;
    }

	void  cleanup(RC rc);
#if CC_ALG == TICTOC
	ts_t 			get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(ts_t max_wts);
	ts_t 			last_wts;
	ts_t 			last_rts;
#elif CC_ALG == SILO
	ts_t 			last_tid;
#elif CC_ALG == MICA
  MICATransaction* mica_tx;
	// bool			readonly;
#endif

	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	std::atomic<int>   row_cnt;
	int	 			wr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;

	// For VLL
	TxnType 		vll_txn_type;

	// index_read methods
#if CC_ALG == MICA
	template <typename IndexT>
	RC index_read(IndexT* index, idx_key_t key, void*& row, access_t type, int part_id);
    template <typename IndexT>
    RC index_read_buffer(IndexT* index, idx_key_t key, void*& row, access_t type, int part_id) ;
    template <typename IndexT>
    bool index_read_buffer_again(IndexT* index, idx_key_t key, void*& row, access_t type, int part_id);

    /**
    template <typename IndexT>
	RC index_read_multiple(IndexT* index, idx_key_t key, void*  rows, size_t& count, int part_id);
    template <typename IndexT>
    RC index_read_range(IndexT* index, idx_key_t min_key, idx_key_t max_key, void*  rows, size_t& count, int part_id);
    template <typename IndexT>
    RC index_read_range_rev(IndexT* index, idx_key_t min_key, idx_key_t max_key, void*  rows, size_t& count, int part_id);
     */
#else
    template <typename IndexT>
    RC index_read(IndexT* index, idx_key_t key, void*& row, itemid_t*& idx_location, access_t type, int part_id);
    template <typename IndexT>
    RC index_read_buffer(IndexT* index, idx_key_t key, void*& row, access_t type, int part_id);
    template <typename IndexT>
    bool index_read_buffer_again(IndexT* index, idx_key_t key, void*& row, access_t type, int part_id);
#endif
    /**
    template <typename IndexT>
    RC index_read_multiple(IndexT* index, idx_key_t key, void*& rows, size_t& count, int part_id);
    template <typename IndexT>
    RC index_read_range(IndexT* index, idx_key_t min_key, idx_key_t max_key, void*& rows, size_t& count, int part_id);
    template <typename IndexT>
    RC index_read_range_rev(IndexT* index, idx_key_t min_key, idx_key_t max_key, void*& rows, size_t& count, int part_id);
     */

	// get_row methods
#if CC_ALG == MICA
#if !TPCC_CF
	template <typename IndexT>
	row_t* get_row(IndexT* index, uint64_t row_id, uint64_t primary_key,
                   int part_id, access_t type, void* row_head = nullptr);
#else
	template <typename IndexT>
	row_t* get_row(IndexT* index, row_t* row, int part_id, access_t type, const access_t* cf_access_type = NULL);
#endif
#else
#if !TPCC_CF
    template <typename IndexT>
    row_t* get_row(IndexT* index, row_t* row, itemid_t *idx_location, int part_id, access_t type );
#else
    template <typename IndexT>
	row_t* get_row(IndexT* index, row_t* row, int part_id, access_t type, const access_t* cf_access_type = NULL);
#endif
#endif

	// search (index_read + get_row)
#if !TPCC_CF
  template <typename IndexT>
  row_t* search(IndexT* index, size_t key, int part_id, access_t type);
#else
  template <typename IndexT>
  row_t* search(IndexT* index, size_t key, int part_id, access_t type, const access_t* cf_access_type = NULL);
#endif

	// insert_row/remove_row
  bool insert_row(table_t* tbl, row_t*& row, int part_id, uint64_t& row_id, uint64_t inst_key);
  bool remove_row(row_t* row);

	// index_insert/index_remove
  template <typename IndexT>
  bool insert_idx(IndexT* idx, uint64_t key, row_t* row, int part_id);
  template <typename IndexT>
  bool remove_idx(IndexT* idx, uint64_t key, row_t* row, int part_id);

	RC apply_index_changes(RC rc);
    RC read_index_again( int rid);

private:
	// insert/remove rows
	uint64_t 		insert_cnt;
	row_t * 		insert_rows[MAX_ROW_PER_TXN];
	uint64_t 		remove_cnt;
	row_t * 		remove_rows[MAX_ROW_PER_TXN];

	// insert/remove indexes
	uint64_t 		   insert_idx_cnt;
//    HASH_INDEX*    insert_idx_idx[MAX_ROW_PER_TXN];
    ORDERED_INDEX*   insert_idx_idx[MAX_ROW_PER_TXN];
	idx_key_t	     insert_idx_key[MAX_ROW_PER_TXN];
	row_t* 		     insert_idx_row[MAX_ROW_PER_TXN];
	int	       	     insert_idx_part_id[MAX_ROW_PER_TXN];
	uint64_t         insert_idx_ptr[MAX_ROW_PER_TXN];

	uint64_t 		   remove_idx_cnt;
//    HASH_INDEX*   remove_idx_idx[MAX_ROW_PER_TXN];
    ORDERED_INDEX*   remove_idx_idx[MAX_ROW_PER_TXN];
	idx_key_t	     remove_idx_key[MAX_ROW_PER_TXN];
	int	      	   remove_idx_part_id[MAX_ROW_PER_TXN];

    // node set for phantom avoidance
    std::unordered_map<void*, uint64_t>  node_map;
    friend class IndexHash;
    friend class IndexArray;
    friend class IndexMBTree;
    friend class IndexMBTree_cb;
    friend class IndexMICAMBTree;
    friend class IndexMICAMBTree_cb;

	txnid_t 		txn_id;
	ts_t 			timestamp;

	bool _write_copy_ptr;
#if CC_ALG == TICTOC || CC_ALG == SILO
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	ts_t 			_max_wts;
	// the following methods are defined in concurrency_control/tictoc.cpp
	RC				validate_tictoc();
#elif CC_ALG == SILO
	ts_t 			_cur_tid;
	RC				validate_silo();
#elif CC_ALG == HEKATON
    template <typename Func>
	RC  validate_hekaton(RC rc, const Func& func)
    {
        uint64_t starttime = get_sys_clock();
        INC_STATS(get_thd_id(), debug1, get_sys_clock() - starttime);
        ts_t commit_ts = glob_manager->get_ts(get_thd_id());
        // validate the read set.
     #if ISOLATION_LEVEL == SERIALIZABLE
        if (rc == RCOK)
        {
            for (int rid = 0; rid < row_cnt; rid ++)
            {
                if (accesses[rid]->type == WR) {
                    continue;
                }
              #if AGGRESSIVE_INLINING
                if (accesses[rid]->orig_row != accesses[rid]->data){
                    continue;
                }
              #endif
                rc = accesses[rid]->orig_row->manager->prepare_read(this, accesses[rid]->data, commit_ts);
                if (rc == Abort){
                    break;
                }
            }
        }
       #endif

     #if AGGRESSIVE_INLINING == false
        rc = apply_index_changes(rc);

        // postprocess, for inserts
        if (rc == RCOK) {
            for (UInt32 i = 0; i < insert_cnt; i++) {
                row_t * row = insert_rows[i];
                row->manager->set_ts(commit_ts);
            }
        }
     #endif

        //for updates
        for (int rid = 0; rid < row_cnt; rid ++) {
            if (accesses[rid]->type == RD ){
                continue;
            }

        #if AGGRESSIVE_INLINING
            if (accesses[rid]->type == INS){
//                #if BUFFERING == false
//				  rc = read_index_again(rid);
//			    #endif
                row_t * row = accesses[rid]->data;
                row->end = commit_ts;
                continue;
            }

            rc = read_index_again(rid);//split push update to buffer, update donot know
        #endif

            accesses[rid]->orig_row->manager->post_process(this, commit_ts, rc, accesses[rid]->data);

        #if AGGRESSIVE_INLINING == false
            //apply index pointing to the latest,update the indirection layer
            if (rc == RCOK){
                auto newst_row = accesses[rid]->data;
                auto idx_location = accesses[rid]->idx_location;
                auto idx_location_row = idx_location->location;
                auto ret = ATOM_CAS(idx_location->location, idx_location_row, reinterpret_cast<void *>(newst_row));
                assert(ret);
            }
        #endif

        #if AGGRESSIVE_INLINING
            if (rc == RCOK){
                func(accesses[rid]->data->get_data());
            }
        #endif
        }

//        printf(" verify rc:%u. \n", rc);
        return rc;
    }
#endif

};
