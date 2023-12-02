#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "mem_alloc.h"

#if CC_ALG==SILO

void
Row_silo::init(row_t * row)
{
#if !AGGRESSIVE_INLINING
	_row = row;
#endif
#if ATOMIC_WORD
	_tid_word = 0;
#else
	_latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t), -1);
	pthread_mutex_init( _latch, NULL );
	_tid = 0;
#endif
}

RC
Row_silo::access(txn_man * txn, TsType type, row_t * local_row) {
#if ATOMIC_WORD
	uint64_t v = 0;
	uint64_t v2 = 1;
	while (v2 != v) {
		v = _tid_word;
		while (v & LOCK_BIT) {
			PAUSE
			v = _tid_word;
		}
#if AGGRESSIVE_INLINING
		//local row already holds data
		//local row is going to be overwritten
#else
		local_row->copy(_row);
#endif
		COMPILER_BARRIER
		v2 = _tid_word;
	}
	txn->last_tid = v & (~LOCK_BIT);
#else
	lock();
#if AGGRESSIVE_INLINING
    //local row already holds data
    //local row is going to be overwritten
#else
    local_row->copy(_row);
#endif
	txn->last_tid = _tid;
	release();
#endif
	return RCOK;
}

bool
Row_silo::validate(ts_t tid, bool in_write_set) {
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (in_write_set)
		return tid == (v & (~LOCK_BIT));

	if (v & LOCK_BIT)
		return false;
	else if (tid != (v & (~LOCK_BIT)))
		return false;
	else
		return true;
#else
	if (in_write_set)
		return tid == _tid;
	if (!try_lock())
		return false;
	bool valid = (tid == _tid);
	release();
	return valid;
#endif
}

void
Row_silo::write(row_t * data, uint64_t tid) {
#if AGGRESSIVE_INLINING
#else
	_row->copy(data);
#endif

#if ATOMIC_WORD
	uint64_t v = _tid_word;
	M_ASSERT(tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
	_tid_word = (tid | LOCK_BIT);
#else
	_tid = tid;
#endif
}

void Row_silo::set_tid(uint64_t tid) {
#if ATOMIC_WORD
    assert(_tid_word & LOCK_BIT);
	_tid_word = tid;
#else
	_tid = tid;
#endif
}

void Row_silo::lock() {
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT)) {
		PAUSE
		v = _tid_word;
	}
#else
	pthread_mutex_lock(_latch);
#endif
}

void Row_silo::release() {
#if ATOMIC_WORD
	assert(_tid_word & LOCK_BIT);
	_tid_word = _tid_word & (~LOCK_BIT);
#else
	pthread_mutex_unlock(_latch);
#endif
}

bool Row_silo::try_lock()
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (v & LOCK_BIT) // already locked
		return false;
	return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
#else
	return pthread_mutex_trylock(_latch) != EBUSY;
#endif
}

uint64_t Row_silo::get_tid()
{
#if ATOMIC_WORD
	assert(ATOMIC_WORD);
	return _tid_word & (~LOCK_BIT);
#else
    return _tid;
#endif
}

#endif
