#pragma once
#ifndef MICA_TRANSACTION_HASH_INDEX_IMPL_INSERT_H_
#define MICA_TRANSACTION_HASH_INDEX_IMPL_INSERT_H_

namespace mica {
namespace transaction {
template <class StaticConfig, bool UniqueKey, class Key, class Hash, class KeyEqual>
uint64_t HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::insert(
    Transaction* tx, const Key& key, uint64_t value) {
#if AGGRESSIVE_INLINING
#else
  Timing t(tx->context()->timing_stack(), &Stats::index_write);

  auto bkt_id = get_bucket_id(key);
  RowAccessHandle rah(tx);

  if (!rah.peek_row(idx_tbl_, 0, bkt_id, nullptr, true, true, false) ||
      !rah.read_row(data_copier_))
    return kHaveToAbort;
  // printf("HashIndex::insert() 1\n");
  auto cbkt = reinterpret_cast<const Bucket*>(rah.cdata());

  // Find any duplicate key or the last bucket in the chain.
  while (true) {
    if (UniqueKey) {
      for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
        if (cbkt->values[j] != kNullRowID && key_equal_(cbkt->keys[j], key)) {
          // A duplicate key has been found.  Do not insert anything.
          return 0;
        }
    }

    if (cbkt->next == kNullRowID) break;
    bkt_id = cbkt->next;

    rah.reset();
    if (!rah.peek_row(idx_tbl_, 0, bkt_id, nullptr, true, true, false) ||
        !rah.read_row(data_copier_))
      return kHaveToAbort;
    // printf("HashIndex::insert() 2\n");
    cbkt = reinterpret_cast<const Bucket*>(rah.cdata());
  }

  // Note that we did not specify write_hint earlier before calling
  // write_row().  It may have better or worse insert speed, but it is totally
  // safe to do so.
  rah.write_row(kDataSize, data_copier_);
  // printf("HashIndex::insert() 3\n");
  auto bkt = reinterpret_cast<Bucket*>(rah.data());

  uint64_t j;
  for (j = 0; j < Bucket::kBucketSize; j++)
    if (bkt->values[j] == kNullRowID) break;

  if (j == Bucket::kBucketSize) {
    RowAccessHandle new_rah(tx);
    if (!new_rah.new_row(0, false, idx_tbl_, 0, Transaction::kNewRowID, true, kDataSize))
      return kHaveToAbort;

    // printf("HashIndex::insert() 4\n");

    auto new_bkt = reinterpret_cast<Bucket*>(new_rah.data());
    for (j = 0; j < Bucket::kBucketSize; j++) new_bkt->values[j] = kNullRowID;
    new_bkt->next = kNullRowID;
    j = 0;

    bkt->next = new_rah.row_id();
    bkt = new_bkt;
  }

  bkt->keys[j] = key;
  bkt->values[j] = value;
#endif
  // printf("HashIndex::insert() 5\n");
  return 1;
}
template <class StaticConfig, bool UniqueKey, class Key, class Hash,
        class KeyEqual>
AggressiveRowHead<StaticConfig>* HashIndex<StaticConfig, UniqueKey, Key, Hash, KeyEqual>::idx_head
                                        (RowAccessHandle& rah, const Key& key) {
    auto bkt_id = get_bucket_id(key);

    //1.find bucket by bucket id
    auto idx_head = idx_tbl_->head(0, bkt_id);
    // printf("HashIndex::insert() 1\n");
    auto cbkt = reinterpret_cast<Bucket*>(idx_head->inlined_rv->data);

    //2.find bucket by key
    AggressiveRowHead<StaticConfig>* agg_RHead;
    // Find any duplicate key or the last bucket in the chain.
    while (true) {
        if (UniqueKey) {
            for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
            {
                agg_RHead = &cbkt->aggressiveRowHead[j];
//                printf("bucket key = %lu \n", cbkt->keys[j]);
                if (key_equal_(cbkt->keys[j], key))
                {
                    // A duplicate key has been found.  Do not insert anything.
                    return 0;
                }
            }
        }else{
            for (uint64_t j = 0; j < Bucket::kBucketSize; j++)
            {
                agg_RHead = &cbkt->aggressiveRowHead[j];
                if (cbkt->keys[j] == kNullRowID)
                {
                    break;
                }
            }
        }

        if (cbkt->next == kNullRowID) {
            break;
        }
        bkt_id = cbkt->next;

        idx_head = idx_tbl_->head(0, bkt_id);
        // printf("HashIndex::insert() 2\n");
        cbkt = reinterpret_cast<Bucket*>(idx_head->inlined_rv->data);
    }

    cbkt->keys[0] = key;
//    if (agg_RHead == NULL){
//        printf("aggressive inlining get new row head fail.");
//    }

    return agg_RHead;
}

}
}

#endif
