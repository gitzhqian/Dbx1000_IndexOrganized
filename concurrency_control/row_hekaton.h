#pragma once
#include "row_mvcc.h"

class table_t;
class Catalog;
class txn_man;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == HEKATON

struct WriteHisEntry {
	bool begin_txn;	
	bool end_txn;
	ts_t begin;
	ts_t end;
//    WriteHisEntry *next_history;
//    WriteHisEntry *pre_history;
	row_t * row;
};

#define INF UINT64_MAX

class Row_hekaton {
public:
	void 			init(row_t * row);
	RC 				access(txn_man * txn, TsType type, row_t * row );
	RC 				prepare_read(txn_man * txn, row_t * row, ts_t commit_ts);
	void 			post_process(txn_man * txn, ts_t commit_ts, RC rc, row_t * row= nullptr);
	void            pushdown_version(txn_man * txn, row_t * row );

  void      lock();
  void      release();
  void      set_ts(ts_t commit_ts);
  bool      get_exists_prewriter(){return _exists_prewrite;}
  void      set_exists_prewriter(bool write){_exists_prewrite = write;}

private:
	volatile bool 	blatch;
	uint32_t 		reserveRow(txn_man * txn);
	void 			doubleHistory();

	uint32_t 		_his_latest;
	uint32_t 		_his_oldest;
	WriteHisEntry * _write_history; // circular buffer
	bool  			_exists_prewrite;

	uint32_t 		_his_len;

	uint32_t        tuple_size;
};

#endif
