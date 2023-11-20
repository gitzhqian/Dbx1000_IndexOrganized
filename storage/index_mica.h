#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

#if INDEX_STRUCT == IDX_MICA

class row_t;
class txn_man;

template <typename MICAIndexT>
class IndexMICAGeneric : public index_base {
 public:
  RC init(uint64_t part_cnt, table_t* table);
  RC init(uint64_t part_cnt, table_t* table, int part_id);
  RC index_insert(txn_man* txn, MICATransaction* tx, idx_key_t key, uint64_t row_id, int part_id);
  RC index_read(txn_man* txn, idx_key_t key, void*& row, access_t type, int part_id);
  RC index_read_buffer(txn_man* txn, idx_key_t key, void*& row, access_t type, int part_id);

    // This method requires the current row value to remove the index entry.
  RC index_remove(txn_man* txn, MICATransaction* tx, idx_key_t key, row_t* row, int part_id){return RCOK;}
  RC index_read_multiple(txn_man* txn, idx_key_t key, void*  rows, size_t& count, int part_id){return RCOK;}
  RC index_read_range(txn_man* txn, idx_key_t min_key, idx_key_t max_key,
                          void* rows, size_t& count, int part_id){return RCOK;}
  RC index_read_range_rev(txn_man* txn, idx_key_t min_key, idx_key_t max_key,
                          void* rows, size_t& count, int part_id){return RCOK;}

  table_t* table;
  std::vector<MICAIndexT *> mica_idx;

// private:
//  uint64_t bucket_cnt;
};

//class IndexMICA : public IndexMICAGeneric<MICAIndex> {};

//class OrderedIndexMICA : public IndexMICAGeneric<MICAOrderedIndex> {};
class OrderedIndexMICA : public IndexMICAGeneric<MICACBtreeIndex> {};

#endif
