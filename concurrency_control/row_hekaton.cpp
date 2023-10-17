#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_hekaton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>
#include "table.h"
#include "catalog.h"

#if CC_ALG == HEKATON

void Row_hekaton::init(row_t * row) {
	_his_len = 30;


    tuple_size = row->get_table()->get_schema()->get_tuple_size();

	_write_history = (WriteHisEntry *) mem_allocator.alloc(sizeof(WriteHisEntry) * _his_len, -1);
	for (uint32_t i = 0; i < _his_len; i++)
		_write_history[i].row = NULL;
	_write_history[0].row = row;
	_write_history[0].begin_txn = false;
	_write_history[0].end_txn = false;
	_write_history[0].begin = 0;
	_write_history[0].end = INF;

	_exists_prewrite = false;

    _his_latest = 0;
    _his_oldest = 0;

	blatch = false;
}

void Row_hekaton::doubleHistory()
{
	WriteHisEntry * temp = (WriteHisEntry *) mem_allocator.alloc(sizeof(WriteHisEntry) * _his_len * 2, -1);
	uint32_t idx = _his_oldest;
	for (uint32_t i = 0; i < _his_len; i++) {
		temp[i] = _write_history[idx];
		idx = (idx + 1) % _his_len;
		temp[i + _his_len].row = NULL;
		temp[i + _his_len].begin_txn = false;
		temp[i + _his_len].end_txn = false;

	}

	_his_oldest = 0;
	_his_latest = _his_len - 1;

	mem_allocator.free(_write_history, sizeof(WriteHisEntry) * _his_len);
	_write_history = temp;

	_his_len *= 2;

}

RC Row_hekaton::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts();
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
#if AGGRESSIVE_INLINING == false
	assert(_write_history[_his_latest].end == INF || _write_history[_his_latest].end_txn);
#endif
	if (type == R_REQ) {
		if (ISOLATION_LEVEL == REPEATABLE_READ) {
            rc = RCOK;
            txn->cur_row = _write_history[_his_latest].row;
        } else if (ts < _write_history[_his_oldest].begin) {
            rc = Abort;
		} else if (ts > _write_history[_his_latest].begin) {
			// TODO. should check the next history entry. If that entry is locked by a preparing txn,
			// may create a commit dependency. For now, I always return non-speculative entries.
			rc = RCOK;
			txn->cur_row = _write_history[_his_latest].row;
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
				if (_write_history[i].begin < ts && _write_history[i].end_txn== false) {
					assert(_write_history[i].end > ts);
					txn->cur_row = _write_history[i].row;
					find = true;
					break;
				}
				if (i == _his_oldest){
                    break;
				}
			}
			assert(find);
		}
	} else if (type == P_REQ) {
#if AGGRESSIVE_INLINING
        if (_exists_prewrite || ts < row->begin) {
#else
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
#endif
			rc = Abort;
		} else {
			rc = RCOK;
			_exists_prewrite = true;
			uint32_t id = reserveRow(txn);
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
#if AGGRESSIVE_INLINING
            _write_history[id].begin_txn = row->begin_txn;
            _write_history[id].begin = row->begin;
            _write_history[id].end_txn = true;
            _write_history[id].end = txn->get_txn_id();
            row->end_txn = true;
            row_t * res_row = _write_history[id].row;
            assert(res_row);
            res_row->copy(row);
            res_row->manager = row->manager;

            txn->cur_row = res_row;
#else
            _write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			row_t * res_row = _write_history[id].row;
			assert(res_row);
			res_row->copy(_write_history[_his_latest].row);

			txn->cur_row = res_row;
#endif
		}
	} else {
        assert(false);
	}

	blatch = false;
	return rc;
}

uint32_t Row_hekaton::reserveRow(txn_man * txn)
{
    // Garbage Collection
    uint32_t idx;
    ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
    uint32_t mod_len;

    mod_len = _his_len;

    if ((_his_latest + 1) % mod_len == _his_oldest // history is full
        && min_ts > _write_history[_his_oldest].end)
    {
        while (_write_history[_his_oldest].end < min_ts)
        {
            assert(_his_oldest != _his_latest);
            _his_oldest = (_his_oldest + 1) % mod_len;
        }
    }

    if ((_his_latest + 1) % mod_len != _his_oldest)
        // _write_history is not full, return the next entry.
        idx = (_his_latest + 1) % mod_len;
    else {
        // write_history is already full
        // If _his_len is small, double it.
        if (mod_len < g_thread_cnt) {
            doubleHistory();
            idx = (_his_latest + 1) % mod_len;
        } else {
            // _his_len is too large, should replace the oldest history
            idx = _his_oldest;
            _his_oldest = (_his_oldest + 1) % mod_len;
        }
    }

    // some entries are not taken. But the row of that entry is NULL.
    if (!_write_history[idx].row) {
#if  AGGRESSIVE_INLINING
        _write_history[idx].row = (row_t *) mem_allocator.alloc((sizeof(row_t) + tuple_size), -1);
        _write_history[idx].row->init(tuple_size);
#else
        _write_history[idx].row = (row_t *) mem_allocator.alloc((sizeof(row_t)), -1);
        _write_history[idx].row->init(tuple_size);
#endif
    }
    return idx;
}

RC Row_hekaton::prepare_read(txn_man * txn, row_t * row, ts_t commit_ts)
{
	RC rc;
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	// TODO may pass in a pointer to the history entry to reduce the following scan overhead.
#if AGGRESSIVE_INLINING
    if (txn->get_ts() < row->begin) {
        rc = Abort;
    } else if (!row->end_txn && row->end > commit_ts)
        rc = RCOK;
    else if (!row->end_txn && row->end < commit_ts) {
        rc = Abort;
    } else {
        // TODO. if the end is a txn id, should check that status of that txn.
        // but for simplicity, we just commit
        rc = RCOK;
    }
    blatch = false;
    return rc;
#else
    uint32_t idx = _his_latest;
    auto his_history = _write_history[_his_latest];
	while (true) {
		if (his_history.row == row) {
		    if (txn->get_ts() < his_history.begin) {
				rc = Abort;
			} else if (!his_history.end_txn && his_history.end > commit_ts)
				rc = RCOK;
			else if (!his_history.end_txn && his_history.end < commit_ts) {
				rc = Abort;
			} else {
				// TODO. if the end is a txn id, should check that status of that txn.
				// but for simplicity, we just commit
				rc = RCOK;
			}
			break;
		}
		if (idx == _his_oldest) {
			rc = Abort;
			break;
		}
//#if AGGRESSIVE_INLINING
//        idx = (idx == 0)? _latest_index - 1 : idx - 1;
//#else
		idx = (idx == 0)? _his_len - 1 : idx - 1;
//#endif
	}
	blatch = false;
	return rc;
#endif
}

void Row_hekaton::post_process(txn_man * txn, ts_t commit_ts, RC rc, row_t * row)
{
	while (!ATOM_CAS(blatch, false, true))
		PAUSE

#if AGGRESSIVE_INLINING
    WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
    _exists_prewrite = false;
    assert(row->end_txn && entry->end == txn->get_txn_id());
    if (rc == RCOK) {
        assert(commit_ts > entry->begin);
        entry->end_txn = false;
        entry->end = commit_ts;
        row->begin = commit_ts;
        row->end = INF;
        row->end_txn = false;

        _his_latest = (_his_latest + 1) % _his_len;
        assert(_his_latest != _his_oldest);
    } else {
        row->end_txn = false;
        row->end = INF;
    }
#else
    WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
    _exists_prewrite = false;
    assert(entry->begin_txn && entry->begin == txn->get_txn_id());
	_write_history[ _his_latest ].end_txn = false;
	if (rc == RCOK) {
		assert(commit_ts > _write_history[_his_latest].begin);
		_write_history[ _his_latest ].end = commit_ts;
		entry->begin = commit_ts;
		entry->end = INF;
		 _his_latest = (_his_latest + 1) % _his_len;
		assert(_his_latest != _his_oldest);
	} else {
        _write_history[ _his_latest ].end = INF;
	}
#endif

	blatch = false;
}

void
Row_hekaton::lock()
{
  while (!ATOM_CAS(blatch, false, true))
		PAUSE
}

void
Row_hekaton::release()
{
  assert(blatch);
  blatch = false;
}

void Row_hekaton::set_ts(ts_t commit_ts)
{
  assert(blatch);
  assert(_his_latest == 0);

  _write_history[0].begin = commit_ts;

  blatch = false;
}

#endif
