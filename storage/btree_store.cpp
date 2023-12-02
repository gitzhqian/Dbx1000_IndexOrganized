//
// Created by zhangqian on 2023/4/11.
//

#include <thread>
#include "btree_store.h"
#include "hash_map"
#include "txn.h"

#if CC_ALG != MICA

bool BaseNode::Freeze() {
    NodeHeader::StatusWord expected = header.GetStatus();
    if (expected.IsFrozen()) {
        return false;
    }
    auto ret =  ATOM_CAS(GetHeader()->status.word,
                         (expected.word),
                         expected.Freeze().word);

    return ret;
}

bool BaseNode::UnFreeze() {
    NodeHeader::StatusWord expected = header.GetStatus();
    if (!expected.IsFrozen()) {
        return false;
    }
    auto ret =  ATOM_CAS(GetHeader()->status.word,
                         (expected.word),
                         expected.UnFreeze().word);

    return ret;
}


//==========================================================
//-------------------------InnernalNode
//==========================================================
InternalNode::InternalNode(uint32_t node_size, const char * key,
                           const uint32_t key_size, uint64_t left_child_addr,
                           uint64_t right_child_addr)
        : BaseNode(false, node_size) {
    // Initialize a new internal node with one key only
    header.sorted_count = 2;  // Includes the null dummy key
    header.size = node_size;

    // Fill in left child address, with an empty key, key len =0
    uint64_t offset = node_size - sizeof(left_child_addr);
    //invalid commit id = 0
    uint64_t total_size = 0 + sizeof(uint64_t);
    row_meta[0].FinalizeForInsert(offset, 0, total_size);
    char *ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, &left_child_addr, sizeof(left_child_addr));

    // Fill in right child address, with the separator key
    auto padded_key_size = row_m::PadKeyLength(key_size);
    auto total_len = padded_key_size + sizeof(right_child_addr);
    offset -= total_len;
    total_size = padded_key_size + sizeof(uint64_t);
    row_meta[1].FinalizeForInsert(offset, key_size, total_size);
    ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, key, key_size);
    memcpy(ptr + padded_key_size, &right_child_addr, sizeof(right_child_addr));

    assert((uint64_t) ptr == (uint64_t) this + sizeof(*this) + 2 * sizeof(row_m));

}

InternalNode::InternalNode(uint32_t node_size, InternalNode *src_node,
                           uint32_t begin_meta_idx, uint32_t nr_records,
                           const char * key, const uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           uint64_t left_most_child_addr, uint32_t value_size)
        : BaseNode(false, node_size) {
    M_ASSERT(src_node,"InternalNode src_node is null.");
    __builtin_prefetch((const void *) (src_node), 0, 1);
    auto padded_key_size = row_m::PadKeyLength(key_size);
    uint64_t total_size = 0;

    uint64_t offset = node_size;
    bool need_insert_new = key;
    uint32_t insert_idx = 0;

    // See if we need a new left_most_child_addr, i.e., this must be the new node
    // on the right
    if (left_most_child_addr) {
        offset -= sizeof(uint64_t);
        total_size = 0 + sizeof(uint64_t);
        row_meta[0].FinalizeForInsert(offset, 0, total_size);
//        LOG_DEBUG("left_most_child_addr = %lu",left_most_child_addr);
        memcpy(reinterpret_cast<char *>(this) + offset, &left_most_child_addr,
                                                                    sizeof(uint64_t));
        ++insert_idx;
    }

    assert(nr_records > 0);

    for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
        row_m meta = src_node->row_meta[i];
        assert(meta.IsVisible());
        if (!meta.IsVisible()) continue;
        uint64_t m_payload = 0;
        char *m_key = nullptr;
        char *m_data = nullptr;
        src_node->GetRawRow(meta, &m_data, &m_key, &m_payload);
        auto m_key_size = meta.GetKeyLength();

        if (!need_insert_new) {
            // New key already inserted, so directly insert the key from src node
            assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
            offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
            total_size = m_key_size + sizeof(uint64_t);
            row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
            memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                    (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
        } else {
            // Compare the two keys to see which one to insert (first)
            auto cmp = KeyCompare(m_key, m_key_size, key, key_size);
//            if (m_key != nullptr && key != nullptr){
//                uint64_t mk = *reinterpret_cast<uint64_t *>(m_key);
//                uint64_t kk = *reinterpret_cast<uint64_t *>(key);
//                printf("mk:%lu,kk:%lu. \n", mk, kk);
//            }

            assert(!(cmp == 0 && key_size == m_key_size));

            if (cmp > 0) {
                assert(insert_idx >= 1);
                // Modify the previous key's payload to left_child_addr
                auto prev_meta = row_meta[insert_idx-1];

                memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
                       prev_meta.GetPaddedKeyLength(), &left_child_addr, sizeof(left_child_addr));

                // Now the new separtor key itself
                offset -= (padded_key_size + sizeof(right_child_addr));
                total_size = key_size + sizeof(uint64_t);
                row_meta[insert_idx].FinalizeForInsert(offset, key_size, total_size);

                ++insert_idx;
                memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
                memcpy(reinterpret_cast<char *>(this) + offset + padded_key_size,
                                              &right_child_addr, sizeof(right_child_addr));

                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                total_size = m_key_size + sizeof(uint64_t);
                row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                        (meta.GetPaddedKeyLength() + sizeof(uint64_t)));

                need_insert_new = false;
            } else {
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                total_size = m_key_size + sizeof(uint64_t);
                row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                        (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
            }
        }
        ++insert_idx;
    }

    if (need_insert_new) {
        // The new key-payload pair will be the right-most (largest key) element
        offset -= total_size;
        total_size = key_size + sizeof(uint64_t);
        row_meta[insert_idx].FinalizeForInsert(offset, key_size, total_size);
        memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
        memcpy(reinterpret_cast<char *>(this) + offset + row_m::PadKeyLength(key_size),
                    &right_child_addr, sizeof(right_child_addr));

        // Modify the previous key's payload to left_child_addr
        auto prev_meta = row_meta[insert_idx - 1];
        memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
               prev_meta.GetPaddedKeyLength(), &left_child_addr, sizeof(left_child_addr));

        ++insert_idx;
    }


    header.size = node_size;
    header.sorted_count = insert_idx;

}
void InternalNode::InternalNodeUpdates(StdMapp::iterator src_ben, StdMapp::iterator src_end){
    this->update_messages->insert(src_ben, src_end);
}
// Insert record to this internal node. The node is frozen at this time.
bool InternalNode::PrepareForSplit(
                    Stack &stack, uint32_t split_threshold, const char * key, uint32_t key_size,
                    uint64_t left_child_addr,    // [key]'s left child pointer
                    uint64_t right_child_addr,   // [key]'s right child pointer
                    InternalNode **new_node, bool backoff) {
    uint32_t data_size = header.size ;
    data_size = data_size + key_size + sizeof(right_child_addr);
    data_size = data_size + sizeof(row_m);

    uint32_t new_node_size = sizeof(InternalNode) + data_size;
    if (new_node_size < split_threshold) {
        InternalNode::New(this, key, key_size, left_child_addr, right_child_addr, new_node);
#if AGGRESSIVE_INLINING
#if BUFFERING
        if (this->update_messages->size()>0){
            auto upt_ben = this->update_messages->begin();
            auto upt_end = this->update_messages->end();
            (*new_node)->InternalNodeUpdates(upt_ben, upt_end);
        }
#endif
#endif
        assert(IsFrozen());
        UnFreeze();
        return true;
    }

    // After adding a key and pointers the new node would be too large.
    // This means we are effectively 'moving up' the tree to do split
    // So now we split the node and generate two new internal nodes.
    M_ASSERT(header.sorted_count >= 2, "header.sorted_count >= 2.");
    uint32_t n_left = header.sorted_count >> 1;

    char *l_pt ;
    char *r_pt ;
    InternalNode **ptr_l = reinterpret_cast<InternalNode **>(&l_pt);
    InternalNode **ptr_r = reinterpret_cast<InternalNode **>(&r_pt);

    auto separator_meta = row_meta[n_left];
    char *separator_key = nullptr;
    uint32_t separator_key_size = separator_meta.GetKeyLength();
    uint64_t separator_payload = 0;
    bool success = GetRawRow(separator_meta, nullptr, &separator_key, &separator_payload);
    M_ASSERT(success, "InternalNode::PrepareForSplit GetRawRecord fail.");

    int cmp = KeyCompare(key, key_size, separator_key, separator_key_size);

//    printf("separator_key, key, %lu, %lu, \n", *reinterpret_cast<uint64_t *>(separator_key),
//                *reinterpret_cast<uint64_t *>(key));

    if (separator_payload == 0){
        printf("separator_payload =0. \n");
    }

    if (cmp == 0) {
        cmp = key_size - separator_key_size;
    }
    M_ASSERT(cmp != 0,"InternalNode::PrepareForSplit KeyCompare fail.");
#if BUFFERING
    auto upt_messgs = this->update_messages;
    auto upt_sz = upt_messgs->size();
    StdMapp::iterator upt_ben;
    StdMapp::iterator upt_end1;
    StdMapp::iterator upt_end2;
    if (upt_sz >0){
        upt_ben = upt_messgs->begin();
        upt_end1 = upt_messgs->begin();
        std::advance(upt_end1, upt_sz / 2);
        upt_end2 = upt_messgs->end();
    }
#endif
    if (cmp < 0) {
        // Should go to left
        InternalNode::New(this, 0, n_left,
                          key, key_size,
                          left_child_addr, right_child_addr, ptr_l, 0 );
        InternalNode::New( this, n_left + 1, (header.sorted_count - n_left - 1),
                           0, 0,
                           0, 0, ptr_r, separator_payload );
#if BUFFERING
        if (upt_sz >0) {
            (*ptr_l)->InternalNodeUpdates(upt_ben, upt_end1);
            (*ptr_r)->InternalNodeUpdates(upt_end1, upt_end2);
        }
#endif
    } else {
        InternalNode::New( this, 0, n_left,
                           0, 0,
                           0, 0, ptr_l, 0 );
        InternalNode::New(this, n_left + 1, (header.sorted_count - n_left - 1),
                          key, key_size,
                          left_child_addr, right_child_addr, ptr_r, separator_payload );
#if BUFFERING
        if (upt_sz >0) {
            (*ptr_l)->InternalNodeUpdates(upt_ben, upt_end1);
            (*ptr_r)->InternalNodeUpdates(upt_end1, upt_end2);
        }
#endif
    }
    assert(*ptr_l);
    assert(*ptr_r);

    auto node_l = reinterpret_cast<uint64_t>(*ptr_l);
    auto node_r = reinterpret_cast<uint64_t>(*ptr_r);

    // Pop here as if this were a leaf node so that when we get back to the
    // original caller, we get stack top as the "parent"
    stack.Pop();

    // Now get this internal node's real parent
    InternalNode *parent = stack.Top() ? stack.Top()->node : nullptr;
    if (parent == nullptr) {
        InternalNode::New(separator_key, separator_key_size, (uint64_t) node_l, (uint64_t) node_r, new_node);
#if BUFFERING
        if (this->update_messages->size()>0){
            auto upt_ben = this->update_messages->begin();
            auto upt_end = this->update_messages->end();
            (*new_node)->InternalNodeUpdates(upt_ben, upt_end);
        }
#endif
        return true;
    }

    __builtin_prefetch((const void *) (parent), 0, 2);

    // Try to freeze the parent node first
    bool frozen_by_me = false;
    while (!parent->IsFrozen()) {
        frozen_by_me = parent->Freeze();
    }

//    while (parent->IsFrozen())
//        PAUSE
//    frozen_by_me = parent->Freeze();
    // Someone else froze the parent node and we are told not to compete with
    // others (for now)
    if (!frozen_by_me && backoff) {
        return false;
    }

    auto parent_split_ret = parent->PrepareForSplit(stack, split_threshold,
                                                    separator_key , separator_key_size,
                                                    (uint64_t) node_l, (uint64_t) node_r,
                                                    new_node, backoff );
    return parent_split_ret;
}

uint32_t InternalNode::GetChildIndex(const char *key, uint32_t key_size, bool get_le) {
    // Keys in internal nodes are always sorted, visible
    int32_t left = 0, right = header.sorted_count - 1, mid = 0;
//    char *key_ = reinterpret_cast<char *>(&key);
    assert(!IsLeaf());

    //binary search
    while (true) {
        mid = (left + right) / 2;
        auto meta = row_meta[mid];
        char *record_key = nullptr;
        GetRawRow(meta, nullptr, &record_key, nullptr);

        auto cmp = KeyCompare(key, key_size, record_key, meta.GetKeyLength());
        if (cmp == 0) {
            // Key exists
            if (get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        }
        if (left > right) {
            if (cmp <= 0 && get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        } else {
            if (cmp > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
    }
}
ReturnCode InternalNode::Update(row_m meta, InternalNode *old_child, InternalNode *new_child){
    auto status = header.GetStatus();
//    if (status.IsFrozen()) {
//        return ReturnCode::NodeFrozen();
//    }

    bool ret_header = ATOM_CAS(header.status.word,status.word, status.word);

    uint64_t old_ = reinterpret_cast<uint64_t> (old_child);
    uint64_t new_ = reinterpret_cast<uint64_t> (new_child);
    auto payload_ptr_ = GetPayloadPtr(meta);
    bool ret_addr =  ATOM_CAS(*payload_ptr_, old_, new_);

    if (ret_header && ret_addr) {
        return ReturnCode::Ok();
    } else {
        return ReturnCode::CASFailure();
    }
}


//==========================================================
//--------------------------LeafNode
//==========================================================
void LeafNode::New(LeafNode **mem, uint32_t node_size ) {
    //initialize the root node(Dram block), using value 0
//    *mem = reinterpret_cast<LeafNode *>(leaf_node_pool->Get(0));
    *mem = reinterpret_cast<LeafNode *>(mem_allocator.alloc(node_size, -1));

    memset(*mem, 0, node_size);
    new(*mem) LeafNode(node_size);

    (*mem)->update_messages = new StdMapp();
}

ReturnCode LeafNode::SearchRowMeta(const char * key, uint32_t key_size, row_m **out_metadata_ptr,
                                   uint32_t start_pos, uint32_t end_pos, bool check_concurrency) {
    ReturnCode rc = ReturnCode::NotFound();
//    char *key_ = reinterpret_cast<char *>(&key);

    for (uint32_t i = 0; i < header.sorted_count; i++) {
        row_m current = row_meta[i];
        char *current_key = GetKey(current);
        assert(current_key);
        auto cmp_result = KeyCompare(key, key_size, current_key, current.GetKeyLength());
        if (cmp_result == 0) {
            if (!current.IsVisible()) {
                break;
            }
            if (out_metadata_ptr) {
                *out_metadata_ptr = row_meta + i;
                rc = ReturnCode::Ok();
            }
            return rc;
        }
    }

    for (uint32_t i = header.sorted_count; i < header.GetStatus().GetRecordCount(); i++) {
        row_m current_row = row_meta[i];
        //delete/select
//        if (current_row.IsInserting()) {
//            if (check_concurrency) {
//                // Encountered an in-progress insert, recheck later
//                *out_metadata_ptr = &row_meta[i];
//                rc = ReturnCode::Ok();
//                return rc;
//            } else {
//                continue;
//            }
//        }
        if (current_row.IsVisible() ) {
            auto current_size = current_row.GetKeyLength();
            auto current_key = GetKey(current_row);
            if (current_size == key_size && KeyCompare(key, key_size, current_key, current_size) == 0) {
                *out_metadata_ptr = &row_meta[i];
                rc = ReturnCode::Ok();
                return rc;
            }
        }
    }

    return rc;
}

ReturnCode LeafNode::Insert(const char * key, uint32_t key_size,
                            row_t *&payload, uint32_t payload_size, uint32_t split_threshold) {
    //1.frozee the location/offset
    //2.copy record to the location
    ReturnCode rc=ReturnCode::Ok();

    NodeHeader::StatusWord expected_status = header.GetStatus();
    assert(expected_status.IsFrozen());

    Uniqueness uniqueness = CheckUnique(key, key_size);
    if (uniqueness == Duplicate) {
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t used_space = LeafNode::GetUsedSpace(expected_status);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t new_size;
    new_size = used_space +row_m_sz +key_size +payload_size;

//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;
    uint32_t total_size;
    total_size = key_size + payload_size;
    desired_status.PrepareForInsert(total_size);
    auto ret = ATOM_CAS(header.status.word,expected_status.word, desired_status.word);
//    assert(ret);
    if (!ret){
        return ReturnCode::CASFailure();
    }
    header.sorted_count++;

    auto insert_index = GetFirstGreater(key);
    row_m *row_meta_ptr = &row_meta[insert_index];
    row_m expected_meta = *row_meta_ptr;
    assert(row_meta_ptr);
    assert(expected_meta.IsVacant());

    row_m desired_meta = expected_meta;
    desired_meta.PrepareForInsert();
    ret = ATOM_CAS(row_meta_ptr->meta,expected_meta.meta, desired_meta.meta);
    assert(ret);

    auto block_size = desired_status.GetBlockSize();
    auto offset = header.size - block_size;
    char *data_ptr = &(reinterpret_cast<char *>(this))[offset];
    std::memcpy(data_ptr, key, key_size);

#if AGGRESSIVE_INLINING
    if (payload == nullptr){
        std::memset(data_ptr + key_size, 0, payload_size);
    } else {
        std::memcpy(data_ptr + key_size, payload, payload_size);
    }
#else
    itemid_t* m_item = (itemid_t*)mem_allocator.alloc(sizeof(itemid_t), 0);
    m_item->init();
    m_item->type = DT_row;
    m_item->location = payload;
    m_item->valid = true;

    uint64_t payload_ = reinterpret_cast<uint64_t>(m_item);
    std::memcpy(data_ptr + key_size, &payload_, sizeof(uint64_t));
#endif

    auto new_meta = desired_meta;
    new_meta.FinalizeForInsert(offset, key_size, total_size);
    ret = ATOM_CAS(row_meta_ptr->meta,desired_meta.meta, new_meta.meta);
    assert(ret);

    payload = reinterpret_cast<row_t *>(data_ptr + key_size);
//    printf("insert k1:%lu \n.", *reinterpret_cast<uint64_t *>(data_ptr));
    return rc;
}

#if AGGRESSIVE_INLINING
ReturnCode LeafNode::Insert_append(const char * key, uint32_t key_size,
                            row_t *&payload, uint32_t payload_size,
                            uint32_t split_threshold) {
    //1.frozee the location/offset
    //2.copy record to the location
    ReturnCode rc=ReturnCode::Ok();

    NodeHeader::StatusWord expected_status = header.GetStatus();

    Uniqueness uniqueness = CheckUnique(key, key_size);
    if (uniqueness == Duplicate) {
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t used_space = LeafNode::GetUsedSpace(expected_status);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t new_size = used_space + row_m_sz +key_size + payload_size;

//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;
    uint32_t total_size;
    total_size = key_size + payload_size;
    desired_status.PrepareForInsert(total_size);
    auto ret = ATOM_CAS(header.status.word,expected_status.word, desired_status.word);
//    assert(ret);
    if (!ret){
        return ReturnCode::CASFailure();
    }

    auto expected_status_record_count = expected_status.GetRecordCount();
    row_m *row_meta_ptr = &row_meta[expected_status_record_count];
    auto expected_meta = *row_meta_ptr;

    row_m desired_meta = expected_meta;
    desired_meta.PrepareForInsert();
    ret = ATOM_CAS(row_meta_ptr->meta,expected_meta.meta, desired_meta.meta);
    assert(ret);

    auto block_size = desired_status.GetBlockSize();
    auto offset = header.size - block_size;
    char *data_ptr = &(reinterpret_cast<char *>(this))[offset];
    std::memcpy(data_ptr, key, key_size);
    if (payload == nullptr){
        std::memset(data_ptr + key_size, 0, payload_size);
    } else {
        std::memcpy(data_ptr + key_size, payload, payload_size);
    }

    auto new_meta = desired_meta;
    new_meta.FinalizeForInsert(offset, key_size, total_size);
    ret = ATOM_CAS(row_meta_ptr->meta,desired_meta.meta, new_meta.meta);
    assert(ret);

    payload = reinterpret_cast<row_t *>(data_ptr + key_size);
//    printf("insert k1:%lu \n.", *reinterpret_cast<uint64_t *>(data_ptr));
    return rc;
}
#endif
uint32_t LeafNode::GetFirstGreater(const char *key){
    assert(header.status.IsFrozen());
    uint32_t total_len = 0;
    uint32_t count = header.GetStatus().GetRecordCount();
    uint32_t indx = 0;

    if (count == 0){
        return 0;
    }

    for (uint32_t i = 0; i < count; ++i) {
        auto meta = row_meta[i];
        char *record_key = nullptr;
        GetRawRow(meta, nullptr, &record_key, nullptr);
        uint32_t len = meta.GetKeyLength();
        total_len = meta.GetTotalLength();

        auto cmp = KeyCompare(key, len, record_key, len);
        if (cmp < 0){
            indx = i;
        }
    }

    auto moving_sz = count-indx;
    for (uint32_t i = 0; i < moving_sz; ++i) {
        //from end to indx
        auto mvfrom_offset = count-1-i;
        auto mvto_offset = count-1-i+1;

        auto pre_meta  = row_meta[mvfrom_offset];
        auto dest_meta = &row_meta[mvto_offset];
        char *pre_ptr  = &(reinterpret_cast<char *>(this))[mvfrom_offset];
        char *dest_ptr = &(reinterpret_cast<char *>(this))[mvto_offset];


        auto expected_row_meta = *dest_meta;
        auto ret = ATOM_CAS(dest_meta->meta,expected_row_meta.meta, pre_meta.meta);
        assert(ret);
        memcpy(dest_ptr, pre_ptr, total_len);
    }

    auto dest_meta = &row_meta[indx];
    auto expected_row_meta = *dest_meta;
    auto ret = ATOM_CAS(dest_meta->meta,expected_row_meta.meta, 0);
    assert(ret);

    return indx;
}
RC LeafNode::Read(idx_key_t key, uint32_t key_size, row_m **row_meta) {
    ReturnCode rc;

    rc = SearchRowMeta(reinterpret_cast<char *>(&key), key_size, row_meta, 0, (uint32_t)-1,
                       false);

    if (rc.IsNotFound()) {
        return RC::ERROR;
    }
//    auto roww = reinterpret_cast<row_t *>(*payload);
//    char *data = roww->get_data();
//    uint32_t total_size = row_meta->GetTotalLength();

    return RC::RCOK;
}
/**
ReturnCode LeafNode::RangeScanBySize(const char * key1, uint32_t key1_size,
                                     uint32_t to_scan, std::list<Roww *> *result) {
    thread_local std::vector<Roww *> tmp_result;
    tmp_result.clear();

    if (to_scan == 0) {
        return ReturnCode::Ok();
    }

    // Have to scan all keys
    auto count = header.GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < count; ++i) {
        auto curr_meta = GetRowMeta(i);
        if (curr_meta.IsVisible()) {
            int cmp = KeyCompare(key1, key1_size, GetKey(curr_meta), curr_meta.GetKeyLength());
            if (cmp <= 0) {
                tmp_result.emplace_back(Roww::New(curr_meta, this));
            }
        }
    }

    std::sort(tmp_result.begin(), tmp_result.end(),
              [this](Roww *a, Roww *b) -> bool {
                  auto cmp = KeyCompare(a->GetKey(), a->meta.GetKeyLength(),
                                        b->GetKey(), b->meta.GetKeyLength());
                  return cmp < 0;
              });

    for (auto item : tmp_result) {
        result->emplace_back(item);
    }

    return ReturnCode::Ok();
}
*/
LeafNode::Uniqueness LeafNode::CheckUnique(const char * key, uint32_t key_size) {
//    char *key_ = reinterpret_cast<char *>(&key);
    row_m *row_meta = nullptr;
    ReturnCode rc = SearchRowMeta(key, key_size, &row_meta);
    if (rc.IsNotFound() || !row_meta->IsVisible()) {
        return IsUnique;
    }

    if(row_meta->IsInserting()) {
        printf("is inserting. \n ");
        return ReCheck;
    }

    M_ASSERT(row_meta->IsVisible(), "LeafNode::CheckUnique metadata Is not Visible.");

    char *curr_key;
    curr_key = GetKey(*row_meta);

    if (KeyCompare(key, key_size, curr_key, key_size) == 0) {
//        char *k11 = const_cast<char *>(key);
//        printf("Duplicate. k1: %lu k2: %lu. \n ", *reinterpret_cast<uint64_t *>(k11), *reinterpret_cast<uint64_t *>(curr_key));
        return Duplicate;
    }

    return ReCheck;
}

LeafNode::Uniqueness LeafNode::RecheckUnique(char * key, uint32_t key_size,
                                             uint32_t end_pos) {
    auto current_status = GetHeader()->GetStatus();
    if (current_status.IsFrozen()) {
        return NodeFrozen;
    }

    // Linear search on unsorted field
    uint32_t linear_end = std::min<uint32_t>(header.GetStatus().GetRecordCount(),
                                             end_pos);
    thread_local std::vector<uint32_t> check_idx;
    check_idx.clear();

    bool i_kz_ = (key_size > 8);
    auto check_metadata = [key, key_size, this](
            uint32_t i, bool push) -> LeafNode::Uniqueness {
        row_m md = row_meta[i];
        if (md.IsInserting()) {
            if (push) {
                check_idx.push_back(i);
            }
            return ReCheck;
        } else if (md.IsVacant() || !md.IsVisible()) {
            return IsUnique;
        } else {
            M_ASSERT(md.IsVisible(), "LeafNode::RecheckUnique meta is not visible.");
            char *curr_key = GetKey(md);
            uint32_t curr_key_len = md.GetKeyLength();
            if (key_size == curr_key_len && std::memcmp(key, curr_key, key_size) == 0) {
                return Duplicate;
            }
            return IsUnique;
        }
    };

    for (uint32_t i = header.sorted_count; i < linear_end; i++) {
        if (check_metadata(i, true) == Duplicate) {
            return Duplicate;
        }
    }

    uint32_t need_check = check_idx.size();
    while (need_check > 0) {
        for (uint32_t i = 0; i < check_idx.size(); ++i) {
            auto result = check_metadata(i, false);
            if (result == Duplicate) {
                return Duplicate;
            } else {
                --need_check;
            }
        }
    }
    return IsUnique;
}

uint32_t LeafNode::SortMetaByKey(std::vector<row_m> &vec, bool visible_only) {
    // Node is frozen at this point
    assert(header.status.IsFrozen());
    uint32_t total_size = 0;
    uint32_t count = header.GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < count; ++i) {
        auto meta = row_meta[i];
//        char *kk = GetKey(meta);
//        printf("k1:%lu \n.", *reinterpret_cast<uint64_t *>(kk));
        if (meta.IsVisible() && visible_only) {
            vec.emplace_back(meta);
            total_size += (meta.GetTotalLength());
            assert(meta.GetTotalLength());
        }
    }

    // Lambda for comparing two keys
    auto key_cmp = [this](row_m &m1, row_m &m2) -> bool {
        auto l1 = m1.GetKeyLength();
        auto l2 = m2.GetKeyLength();
        char *k1 = GetKey(m1);
        char *k2 = GetKey(m2);
        return KeyCompare(k1, l1, k2, l2) < 0;
    };

    std::sort(vec.begin(), vec.end(), key_cmp);
    return total_size;
}

void LeafNode::CopyFrom(LeafNode *node, std::vector<row_m>::iterator begin_it,
                        std::vector<row_m>::iterator end_it,
                        std::vector<std::pair<uint64_t, message_upt>> *conflicts) {
    assert(node->IsFrozen());
    uint32_t offset = header.size;
    uint32_t nrecords = 0;
    for (auto it = begin_it; it != end_it; ++it) {
        auto row_meta_ = *it;
        char *key_ptr;
        key_ptr = node->GetKey(row_meta_);
        uint32_t key_len = row_meta_.GetKeyLength();
        uint32_t total_len = row_meta_.GetTotalLength();

#if AGGRESSIVE_INLINING
#if BUFFERING
        char *payload = key_ptr + key_len;
        auto orgroww = reinterpret_cast<row_t *>(payload);
        if (orgroww->is_updated){
            auto roww = (row_t *) mem_allocator.alloc(total_len, -1);
            memcpy(roww, orgroww, total_len);
            message_upt messgupt(total_len, roww);
            uint64_t ky = *reinterpret_cast<uint64_t *>(key_ptr);
            conflicts->emplace_back(std::make_pair(ky, messgupt));

            continue;
        }
#endif
#endif

        assert(offset >= total_len);
        offset -= total_len;
        char *dest_ptr = &(reinterpret_cast<char *>(this))[offset];

        memcpy(dest_ptr, key_ptr, total_len);

        row_m *row_m_ptr = &row_meta[nrecords];
        auto expected_row_meta = *row_m_ptr;
        auto new_row_meta = expected_row_meta;
        new_row_meta.FinalizeForInsert(offset, key_len, total_len);
        auto ret = ATOM_CAS(row_m_ptr->meta,expected_row_meta.meta, new_row_meta.meta);
        assert(ret);

        ++nrecords;
    }

    // Finalize header stats
    header.status.SetBlockSize(header.size - offset);
    header.status.SetRecordCount(nrecords);
    header.sorted_count = nrecords;
}

bool LeafNode::PrepareForSplit(Stack &stack,
                               uint32_t split_threshold,
                               uint32_t key_size, uint32_t payload_size,
                               LeafNode **left, LeafNode **right,
                               InternalNode **new_parent, bool backoff,
                               std::vector<std::pair<uint64_t, message_upt>> *conflicts) {
    assert(key_size<=8);
    if (!header.status.IsFrozen()){
        return false;
    }
    if(header.GetStatus().GetRecordCount() < 3){
        return false;
    }

    thread_local std::vector<row_m> meta_vec;
    uint32_t total_size = 0;
    uint32_t nleft = 0;
    // Prepare new nodes: a parent node, a left leaf and a right leaf
    LeafNode::New(left, header.size );
    LeafNode::New(right, header.size );
    uint32_t totalSize = key_size + payload_size;
    uint32_t count = header.GetStatus().GetRecordCount();
    if(count <3) return false;

    meta_vec.clear();
    total_size = SortMetaByKey(meta_vec, true );

    int32_t left_size = total_size / 2;
    for (uint32_t i = 0; i < meta_vec.size(); ++i) {
        ++nleft;
        left_size -= totalSize;
        if (left_size <= 0) {
            break;
        }
    }

    if(nleft <= 0) return false;

    auto left_end_it = meta_vec.begin() + nleft;
    auto node_left = *left;
    auto node_right = *right;

    if (!IsFrozen()) return false;
    (*left)->CopyFrom(this, meta_vec.begin(), left_end_it , conflicts);
    (*right)->CopyFrom(this, left_end_it, meta_vec.end(), conflicts);

    row_m separator_meta = meta_vec.at(nleft - 1);
    uint32_t separator_key_size = separator_meta.GetKeyLength();
    char *separator_key = GetKey(separator_meta);

//    printf("separator_key:%lu \n.", *reinterpret_cast<uint64_t *>(separator_key));

    if(separator_key == nullptr){
        return false;
    }

    COMPILER_BARRIER;

    auto stack_node = stack.Top() ? stack.Top()->node : nullptr;
    InternalNode *parent = reinterpret_cast<InternalNode *>(stack_node);

    if (parent == nullptr) {
        InternalNode::New( separator_key , separator_key_size,
                           reinterpret_cast<uint64_t>(node_left),
                           reinterpret_cast<uint64_t>(node_right),
                           new_parent );
        return true;
    }

    bool frozen_by_me = false;
    while (!parent->IsFrozen()){
        frozen_by_me = parent->Freeze();
    }

    if (!frozen_by_me && backoff) {
        return false;
    } else {
         auto ret = parent->PrepareForSplit(
                  stack, split_threshold,  separator_key, separator_key_size,
                      reinterpret_cast<uint64_t>(node_left), reinterpret_cast<uint64_t>(node_right),
                      new_parent, backoff );

         parent->UnFreeze();

        return ret;
    }
}

//==========================================================
//-------------------------BTree store
//==========================================================
void IndexBtree::Init(ParameterSet param, uint32_t key_size, table_t *table_ )  {
    parameters = param;
    table = table_;
    table->add_table_index(this);

//    root = reinterpret_cast<BaseNode *>(leaf_node_pool->Get(0));
    root = reinterpret_cast<BaseNode *>(mem_allocator.alloc(sizeof(parameters.leaf_node_size), -1));
    LeafNode **root_node = reinterpret_cast<LeafNode **>(&root);
    LeafNode::New(root_node, parameters.leaf_node_size );
}

#if AGGRESSIVE_INLINING
RC IndexBtree::TraverseToTarget(txn_man* txn, Stack *stack, idx_key_t key,
                                       uint32_t key_size, message_upt ** messge_upt,
                                       LeafNode **l_node, InternalNode **i_node,
                                       bool le_child) {
    BaseNode *node = GetRootNodeSafe();
    __builtin_prefetch((const void *) (root), 0, 1);

    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode *parent = nullptr;
    uint32_t meta_index = 0;
    assert(node);
    while (!node->IsLeaf()) {
        parent = reinterpret_cast<InternalNode *>(node);
        if (parent == nullptr) return RC::ERROR;
        //if find in the inner node, and txn'ts > version's begin
        //else goto the leaf node (if WR, return in the leaf node)
        //else goto the version store (if RD, may return in the version store)
        auto buffer_ = parent->update_messages;
        if (!buffer_->empty()){
//            printf("inner node buffer size: %lu. \n", buffer_->size());
//            auto fin = buffer_->find(key);
//            MyKey ky{reinterpret_cast<char *>(&key), sizeof(idx_key_t)};
            auto fin = buffer_->find(std::to_string(key));
            if (fin != buffer_->end()){
                auto mess = fin->second;
                *messge_upt = &mess;
                *i_node = parent;
                return RC::RCOK;
            }
        }
        //binary search in inner node
        meta_index = parent->GetChildIndex(reinterpret_cast<char *>(&key), key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);
        if (node == nullptr) return RC::ERROR;
        assert(node);
    }

    *l_node = reinterpret_cast<LeafNode *>(node);

    return RC::RCOK;
}
#endif

LeafNode *IndexBtree::TraverseToLeaf(Stack *stack, const char * key,
                                       uint32_t key_size, bool le_child) {
    static const uint32_t kCacheLineSize = 64;
    BaseNode *node = nullptr;
    node = GetRootNodeSafe();
    __builtin_prefetch((const void *)(root), 0, 3);

    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode *parent = nullptr;
    uint32_t meta_index = 0;
    assert(node);
    while (!node->IsLeaf()) {
        parent = reinterpret_cast<InternalNode *>(node);
        meta_index = parent->GetChildIndex(key, key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);
//        assert(node);
        if (node == nullptr){
            return nullptr;
        }
        if (stack != nullptr) {
            stack->Push(parent, meta_index);
        }
    }

    return reinterpret_cast<LeafNode *>(node);
}
/**
 * when multi threads, if sh down recursionly
 * then it will incur the mutual wait lcok(frozon)
 * then use the tbb::concurent_map, not the std::map
 * @param node_i
 * @param node_l
 * @return
 */
bool IndexBtree::ChooseChildtoPushdown(BaseNode *node_i, LeafNode **node_l ){
//    printf("node i buffer size: %lu. \n", node_i->update_messages->size());
//    while (node_i->IsFrozen())
//        PAUSE

    auto ret = node_i->Freeze();
    if(!ret){
        return false;
    }

    auto sz = node_i->header.sorted_count;
    if (sz <=0 || node_i->update_messages->size() <= MESSAGE_COUNT){
        node_i->UnFreeze();
        return false;
    }
    assert(sz >= 2);
    auto upt_messages = node_i->update_messages;
    auto upt_sz = upt_messages->size();
    if (upt_sz <= MESSAGE_COUNT) {
        node_i->UnFreeze();
        return false;
    }
    uint32_t max_size = 0;
    uint32_t dist=0;
    uint64_t child = 0;
//    std::string itrb_k;
//    std::string itre_k;
    auto itrb_k = upt_messages->begin();
    auto itre_k = upt_messages->end();
    auto itr1 = upt_messages->begin();
    auto itr2 = upt_messages->end();
    for (int i = 0; i < sz; ++i) {
        auto meta = node_i->row_meta[i];
        char *m_key = nullptr;
        uint64_t m_payload = 0;
        node_i->GetRawRow(meta, nullptr, &m_key, &m_payload);

        if (m_key == nullptr){
            itr1 = upt_messages->begin();
        } else{
            string str = m_key;
            itr1 = upt_messages->lower_bound(str);
        }

        if ((i+1) >= sz){
            itr2 = upt_messages->end();
        } else{
            auto next_meta = node_i->row_meta[i+1];
            char *m_key2 = nullptr;
            node_i->GetRawRow(next_meta, nullptr, &m_key2, nullptr);
            string str2 = m_key2;
            itr2 = upt_messages->lower_bound(str2);
        }

        auto dist1 = distance(upt_messages->begin(), itr1);
        auto dist2 = distance(upt_messages->begin(), itr2);
        if (dist1 > dist2){
            continue;
//            dist = distance(itr2, itr1);
        }else{
            dist = distance(itr1, itr2);
        }
//        dist = distance(itr1, itr2);
        if (dist > max_size){
            itrb_k = itr1 ;
            itre_k = itr2;
            child = m_payload;
            max_size = dist;
        }
    }
    //insert into child's buffer
    auto node_child = reinterpret_cast<BaseNode *>(child);
    if (node_child == nullptr) {
        node_i->UnFreeze();
        return false;
    }

    if (node_child->IsFrozen()){
        node_i->UnFreeze();
        return false;
    }
    bool frozen_by_me1 = node_child->Freeze();
    assert(frozen_by_me1);
    auto insrt_b = itrb_k;
    auto insrt_e = itre_k;
    node_child->update_messages->insert(itrb_k, itre_k);
    node_child->UnFreeze();

    //clear this buffer
    auto curr_nd_sz = node_i->update_messages->size();
//    printf("current update buffer size:%lu. \n",curr_nd_sz);
    node_i->update_messages->erase(itrb_k, itre_k);
    node_i->UnFreeze();

    if (node_child->is_leaf){
        *node_l = reinterpret_cast<LeafNode *>(node_child);
        assert((*node_l)->is_leaf);
    }else{
        if (node_child->update_messages->size() > MESSAGE_COUNT){
            ChooseChildtoPushdown(node_child, node_l);
        }
    }

    return true;
}
bool IndexBtree::ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr) {
    bool ret = ATOM_CAS(*reinterpret_cast<uint64_t *>(&root), expected_root_addr, new_root_addr);
    assert(ret);

    return ret;
}

ReturnCode IndexBtree::Insert(const char * key, uint32_t key_size,
                              row_t *&payload, uint32_t payload_size,
                              std::vector<std::pair<uint64_t, message_upt>> *conflicts) {
    thread_local Stack stack = {};
    stack.tree = this;
    uint64_t freeze_retry = 0;
    LeafNode *node = nullptr;

    while(true) {
        stack.Clear();

        node = TraverseToLeaf(&stack, key, key_size);
        if(node == nullptr){
            return ReturnCode::NotFound() ;
        }

        // 1.Try to frozen the node
        volatile bool frozen_by_me = false;
        while(!node->IsFrozen()) {
            frozen_by_me = node->Freeze();
        }
        if(!frozen_by_me) {
            if (++freeze_retry <= MAX_FREEZE_RETRY) {
                continue;
            }
            return ReturnCode::NodeFrozen();
        }

        assert(node->IsFrozen());
        // 2.Try to insert to the leaf node
        auto rc = node->Insert(key, key_size, payload, payload_size, parameters.leaf_node_size);
        if(rc.IsOk() || rc.IsKeyExists()) {
            assert(frozen_by_me);
            node->UnFreeze();
            return rc;
        } else if (rc.IsCASFailure()){
            node->UnFreeze();
            continue;
        }

        assert(rc.IsNotEnoughSpace());
        assert(node->IsFrozen());

        bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);
        // Should split and we have three cases to handle:
        // 1. Root node is a leaf node - install [parent] as the new root
        // 2. We have a parent but no grandparent - install [parent] as the new root
        // 3. We have a grandparent - update the child pointer in the grandparent
        //    to point to the new [parent] (might further cause splits up the tree)
        char *b_r = nullptr;
        char *b_l = nullptr;
        char *b_pt ;
        LeafNode **ptr_r = reinterpret_cast<LeafNode **>(&b_r);
        LeafNode **ptr_l = reinterpret_cast<LeafNode **>(&b_l);
        InternalNode **ptr_parent = reinterpret_cast<InternalNode **>(&b_pt);

        bool should_proceed;
        should_proceed = node->PrepareForSplit(
            stack, parameters.split_threshold, key_size,  payload_size,
               ptr_l, ptr_r, ptr_parent, backoff,  conflicts);

        if (!should_proceed) {
            if (b_r != nullptr){
                memset(b_r, 0 , parameters.leaf_node_size);
                mem_allocator.free(b_r,parameters.leaf_node_size);
            }
            if (b_l != nullptr){
                memset(b_l, 0 , parameters.leaf_node_size);
                mem_allocator.free(b_l,parameters.leaf_node_size);
            }
            continue;
        }

        assert(*ptr_parent);
        auto node_parent = reinterpret_cast<uint64_t>(*ptr_parent);

        auto *top = stack.Pop();
        InternalNode *old_parent = nullptr;
        if (top) {
            old_parent = reinterpret_cast<InternalNode *>(top->node);
        }

        top = stack.Pop();
        InternalNode *grand_parent = nullptr;
        if (top) {
            grand_parent = reinterpret_cast<InternalNode *>(top->node);
        }

        if (grand_parent) {
            M_ASSERT(old_parent, "BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(top->node->GetRowMeta(top->meta_index),
                                               old_parent, *ptr_parent);
            if (!result.IsOk()) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_r, parameters.leaf_node_size);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_l, parameters.leaf_node_size);
                }

                return ReturnCode::CASFailure();
            }
        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result = ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_r, parameters.leaf_node_size);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_l, parameters.leaf_node_size);
                }

                return ReturnCode::CASFailure();
            }
        }

        assert(should_proceed);
        if (should_proceed){
//            memset(reinterpret_cast<char *>(node), 0 , parameters.leaf_node_size);
//            mem_allocator.free(reinterpret_cast<char *>(node), parameters.leaf_node_size);
        }
        if (old_parent) {
//            mem_allocator.free(reinterpret_cast<char *>(old_parent), old_parent->header.size);
        }
    }
}

#if AGGRESSIVE_INLINING
ReturnCode IndexBtree::Insert_Append(const char * key, uint32_t key_size,
                                      row_t *&payload, uint32_t payload_size,
                                      std::vector<std::pair<uint64_t, message_upt>> *conflicts) {
    thread_local Stack stack;
    stack.tree = this;
    uint64_t freeze_retry = 0;
    LeafNode *node = nullptr;

    while(true) {
        stack.Clear();

        node = TraverseToLeaf(&stack, key, key_size);
        if(node == nullptr){
            return ReturnCode::NotFound() ;
        }

        // 1.Try to frozen the node
        bool frozen_by_me = false;
        while(!node->IsFrozen()) {
            frozen_by_me = node->Freeze();
        }
        if(!frozen_by_me) {
            if (++freeze_retry <= MAX_FREEZE_RETRY) {
                continue;
            }
            return ReturnCode::NodeFrozen();
        }

        // 2.Try to insert to the leaf node
        auto rc = node->Insert_append(key, key_size, payload, payload_size, parameters.leaf_node_size );
        if(rc.IsOk() || rc.IsKeyExists()) {
            assert(frozen_by_me);
            node->UnFreeze();
            return rc;
        } else if (rc.IsCASFailure()){
            node->UnFreeze();
            continue;
        }

        bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);
        assert( rc.IsNotEnoughSpace());
        assert(node->IsFrozen());
        // Should split and we have three cases to handle:
        // 1. Root node is a leaf node - install [parent] as the new root
        // 2. We have a parent but no grandparent - install [parent] as the new root
        // 3. We have a grandparent - update the child pointer in the grandparent
        //    to point to the new [parent] (might further cause splits up the tree)
        char *b_r = nullptr;
        char *b_l = nullptr;
        char *b_pt ;
        LeafNode **ptr_r = reinterpret_cast<LeafNode **>(&b_r);
        LeafNode **ptr_l = reinterpret_cast<LeafNode **>(&b_l);
        InternalNode **ptr_parent = reinterpret_cast<InternalNode **>(&b_pt);

        bool should_proceed;
        should_proceed = node->PrepareForSplit(
                stack, parameters.split_threshold, key_size,  payload_size,
                ptr_l, ptr_r, ptr_parent, backoff, conflicts);

        if (!should_proceed) {
            if (b_r != nullptr){
                memset(b_r, 0 , parameters.leaf_node_size);
                mem_allocator.free(b_r,parameters.leaf_node_size);
            }
            if (b_l != nullptr){
                memset(b_l, 0 , parameters.leaf_node_size);
                mem_allocator.free(b_l,parameters.leaf_node_size);
            }
            continue;
        }

        assert(*ptr_parent);
        auto node_parent = reinterpret_cast<uint64_t>(*ptr_parent);

        auto *top = stack.Pop();
        InternalNode *old_parent = nullptr;
        if (top) {
            old_parent = reinterpret_cast<InternalNode *>(top->node);
        }

        top = stack.Pop();
        InternalNode *grand_parent = nullptr;
        if (top) {
            grand_parent = reinterpret_cast<InternalNode *>(top->node);
        }

        if (grand_parent) {
            M_ASSERT(old_parent, "BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(top->node->GetRowMeta(top->meta_index),
                                              old_parent, *ptr_parent);
            if (!result.IsOk()) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_r, parameters.leaf_node_size);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_l, parameters.leaf_node_size);
                }
                return ReturnCode::CASFailure();
            }
        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result =  ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_r, parameters.leaf_node_size);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    mem_allocator.free(b_l, parameters.leaf_node_size);
                }
                return ReturnCode::CASFailure();
            }
        }

        assert(should_proceed);
        if (should_proceed){
            memset(reinterpret_cast<char *>(node), 0 , parameters.leaf_node_size);
            mem_allocator.free(reinterpret_cast<char *>(node), parameters.leaf_node_size);
        }
        if (old_parent) {
            mem_allocator.free(reinterpret_cast<char *>(old_parent), old_parent->header.size);
        }
    }
}

RC IndexBtree::index_insert_batch(txn_man* txn, LeafNode *node_l,
                                  uint32_t key_size, uint32_t payload_size, uint32_t split_threshold){
    RC rc = RCOK;

//    while(node_l->IsFrozen()) {
//        PAUSE
//    }
//    thread_local bool frozen_by_me = node_l->Freeze();
//    if(!frozen_by_me) return Abort;

    if (node_l->IsFrozen()){
        return Abort;
    }
    bool frozen_by_me = node_l->Freeze();
    if(!frozen_by_me) return Abort;

    auto leaf_buffer = node_l->update_messages;
    auto buffer_sz = leaf_buffer->size();
    std::vector<std::pair<uint64_t, message_upt>> conflicts;

    auto statuss = node_l->header.GetStatus();
    uint32_t used_space = LeafNode::GetUsedSpace(statuss);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t one_row_sz = row_m_sz + key_size + payload_size;
    uint32_t new_size = used_space + one_row_sz*buffer_sz;
//    if (new_size >= split_threshold) {
        //1. index insert append
    thread_local Stack stack;
    stack.Clear();

    auto first_item = leaf_buffer->begin();
    auto first_key = first_item->first;
//    auto first_payload = first_item->second.newest;
//    auto first_payload_sz = first_item->second.payload_sz;
    auto node = TraverseToLeaf(&stack, reinterpret_cast<char *>(&first_key), key_size);
    if(node == nullptr){
        rc = Abort;
    }
    auto itr = first_item;
    uint32_t current_sz = used_space;
    uint32_t distance = split_threshold - used_space;
    while (distance > one_row_sz){
        if (itr == leaf_buffer->end()){
            break;
        }
        auto append_ky = itr->first;
        auto append_payload = itr->second.newest;
        auto append_payload_sz = itr->second.payload_sz;

        node->Insert_append(append_ky.c_str() , sizeof(uint64_t),
                            append_payload, append_payload_sz, split_threshold );
        current_sz = current_sz + used_space + one_row_sz;
        distance = split_threshold - current_sz;
        itr++;
    }

    for (; itr != leaf_buffer->end(); ++itr) {
        auto itr_key = itr->first;
        auto itr_payload = itr->second.newest;
        auto itr_payload_sz = itr->second.payload_sz;
        auto retc = Insert_Append(itr_key.c_str(), sizeof(uint64_t),
                                  itr_payload, itr_payload_sz, &conflicts);
        if (retc.IsOk()){
            for (int i = 0; i < conflicts.size(); ++i) {
                auto item = conflicts[i];
                auto key_c = item.first;
                auto mess_c = item.second;
#if CC_ALG == HEKATON
                auto txn_id_c = mess_c.newest->end;
#elif CC_ALG == SILO
                auto txn_id_c = mess_c.newest->manager->get_tid();
#endif
                index_insert_buffer(txn, key_c, mess_c.newest, mess_c.payload_sz);
                conflicts_txn.insert(txn_id_c);
            }
            rc = RCOK;
        }else{
            if (retc.IsKeyExists()){
                printf("insert: %s fail, key:%ld, returnCode:%d.\n ", this->table->get_table_name(), std::stol(itr_key), retc.rc);
                rc =  Abort;
            }
        }
    }

    for (auto itr = leaf_buffer->begin(); itr != leaf_buffer->end(); ++itr) {
        conflicts_k.erase(itr->first);
        leaf_buffer->erase(itr);
    }
    leaf_buffer->clear();

    node_l->UnFreeze();

    return rc;
}
#endif
RC IndexBtree::index_insert(txn_man* txn, idx_key_t key, row_t *&payload, uint32_t payload_size, int part_id){
    RC rc = Abort;
    uint32_t key_size = sizeof(key);
    int retry_count=0;
    //transaction updates and tree splits
    std::vector<std::pair<uint64_t, message_upt>> conflicts;

    retry:
    auto retc = Insert(reinterpret_cast<const char *>(&key), key_size, payload, payload_size, &conflicts);

    if (retc.IsOk()){
#if AGGRESSIVE_INLINING
#if BUFFERING
        //emplace conflicts to root buffer
        for (int i = 0; i < conflicts.size(); ++i) {
            auto item = conflicts[i];
            auto key_c = item.first;
            auto mess_c = item.second;
//            auto txn_id_c = mess_c.newest->end;
            #if CC_ALG == HEKATON
                auto txn_id_c = mess_c.newest->end;
            #elif CC_ALG == SILO
                auto txn_id_c = mess_c.newest->manager->get_tid();
            #endif
            index_insert_buffer(txn, key_c, mess_c.newest, mess_c.payload_sz);
//            conflicts_txn.insert(reinterpret_cast<char *>(txn_id_c));
            conflicts_txn.insert(txn_id_c);
            conflicts_k.insert(std::to_string(key_c));
        }
#endif
#endif
        assert(payload != nullptr);
        rc = RCOK;
    }else{
        if (retc.IsKeyExists()){
//            printf("insert: %s fail, key:%lu, returnCode:%d.\n ", this->table->get_table_name(), key, retc.rc);
            return Abort;
        }
//        else if (retry_count < MAX_INSERT_RETRY){
//            retry_count++;
//            goto retry;
//        }
        else{
            return Abort;
            //printf("insert: %s fail, key:%lu, returnCode:%d.\n ", this->table->get_table_name(), key, retc.rc);
        }

    }
    return rc;
}
#if AGGRESSIVE_INLINING
RC IndexBtree::index_insert_buffer(txn_man* txn, idx_key_t key, row_t *&payload, uint32_t payload_size ,
                                   int part_id){
    RC rc = RCOK;

    auto root_ = GetRootNodeSafe();
    message_upt messgupt(payload_size, payload);

    //1.get root, insert root.
    auto root_i = root_ ;
    if (root_i->IsFrozen()){
        return Abort;
    }
    bool frozen_by_me = root_i->Freeze();
    if (!frozen_by_me){
        return Abort;
    }

    auto root_buff = root_i->update_messages;
    //insert key to root's buffer
    root_buff->insert(std::make_pair(std::to_string(key), messgupt));
    root_i->UnFreeze();
    //insert key to root's BF
//    conflicts_k.insert(std::to_string(key));

    //2.check this node, if the root fill up, pushdown
    if (root_buff->size() > MESSAGE_COUNT){
        //choose a child having the largest set of pending insertion message
//        printf("root buffer size:%lu. \n", root_buff->size());
        LeafNode *node_l = nullptr;
        auto ret = ChooseChildtoPushdown(root_i, &node_l);
//        printf("choose push down ret:%d. \n", ret);
        if (ret && (node_l != nullptr)){
            assert(node_l->is_leaf);
//            printf("node_l is leaf:%d. \n", node_l->is_leaf);
//            printf("node_l buffer size:%lu. \n", node_l->update_messages->size());
            //3.1.if the leaf fill up, split
            //3.2.if split conflits with transaction, insert it to root
            rc = index_insert_batch(txn, node_l,  sizeof(uint64_t), payload_size, parameters.leaf_node_size);
        }
    }else{
//        if (frozen_by_me) root_i->UnFreeze();
    }


    return rc;
}
#endif
RC IndexBtree::index_read(txn_man* txn,  idx_key_t key, void *& item, itemid_t*& idx_location,
                          access_t type, int part_id ) {
    RC rc = RCOK;
    row_m *row_meta;
    uint32_t key_size = sizeof(key);
    char *key_read = reinterpret_cast<char *>(&key);

    LeafNode *node = TraverseToLeaf(nullptr, key_read,  key_size);
    if (node == nullptr) {
        return RC::Abort;
    }

//    for (uint32_t i = 0; i < header.GetStatus().GetRecordCount(); i++) {
//   for (uint32_t i = 0; i < node->header.size / kCacheLineSize; ++i) {
//        __builtin_prefetch((const void *)((char *)node + i * kCacheLineSize), 0, 1);
//    }
    ReturnCode retc = node->SearchRowMeta(key_read, key_size, &row_meta,0,0,false);

    if (retc.IsOk()){
#if AGGRESSIVE_INLINING
        if (type == WR){
            if (node->IsFrozen()){
//                printf("leaf node frozen. ");
                return RC::Abort;
            }
        }
        char *source_addr = (reinterpret_cast<char *>(node) + row_meta->GetOffset());
        char *payload_ = source_addr + row_meta->GetPaddedKeyLength() ;
        item = reinterpret_cast<void *>(payload_);
#else
        char *source_addr = (reinterpret_cast<char *>(node) + row_meta->GetOffset());
        char *payload_ = source_addr + row_meta->GetPaddedKeyLength();
        uint64_t payload__ = *reinterpret_cast<uint64_t *>(payload_);
        auto m_item = (itemid_t *)(payload__);
        idx_location = m_item;
        item = (row_t*)m_item->location;
#endif
    } else{
        printf("search is not ok. \n");
        rc = RC::Abort;
    }

    return rc;
}
#if AGGRESSIVE_INLINING
bool IndexBtree::index_read_buffer_again(txn_man* txn, idx_key_t key, void *&item, access_t type, int part_id){
    auto txn_id = txn->get_txn_id();
//    bool is_in_buffer = conflicts_txn.query(reinterpret_cast<char *>(&txn_id));
    bool is_in_buffer = conflicts_txn.contains(txn_id);
    if (!is_in_buffer){
        return is_in_buffer; //false
    }else{
        index_read_buffer( txn,  key, item,  type, part_id);
    }

    return !is_in_buffer; //true
}
RC IndexBtree::index_read_buffer(txn_man* txn, idx_key_t key, void *&item, access_t type, int part_id) {
    RC rc = RCOK;
    char *payload_;
    row_m *row_meta = nullptr;
    uint32_t key_size = sizeof(key);
    uint32_t payload_sz = 0;
    RC ret = RCOK;
    message_upt *messge_upt = nullptr;
    LeafNode *node_l = nullptr;
    InternalNode *node_i = nullptr;

    bool is_in_buffer = conflicts_k.contains(std::to_string(key));
    if (!is_in_buffer){
        char *key_read = reinterpret_cast<char *>(&key);
        node_l = TraverseToLeaf(nullptr, key_read,  key_size);
    }else{
        ret = TraverseToTarget(txn, nullptr, key,  key_size , &messge_upt, &node_l, &node_i, true);
        if (ret != RCOK) {
            return RC::Abort;
        }
    }

    row_t *leaf_row = nullptr;
    row_t *orgin = nullptr;
//    row_t *update = nullptr;
    //find in the leaf buffer or inner buffer
    if (messge_upt != nullptr) {
        payload_sz = messge_upt->payload_sz;
        orgin = messge_upt->newest;
        payload_ = reinterpret_cast<char *>(orgin);
        if (orgin == nullptr){
//          printf("orgin read error. ");
            return RC::Abort;
        }
    } else{
        if(node_l == nullptr){
//            printf("leaf node error. ");
            return RC::Abort;
        }
        //read the node, if it is leaf
        //find in the leaf node
        auto ret1 = node_l->Read(key, key_size, &row_meta);
        if (ret1 == RCOK){
            char *source_addr = (reinterpret_cast<char *>(node_l) + row_meta->GetOffset());
            payload_ = source_addr + row_meta->GetPaddedKeyLength() ;
            payload_sz = row_meta->GetTotalLength();
            leaf_row = reinterpret_cast<row_t *>(payload_);
            if (leaf_row == nullptr){
                printf("leaf_row read error. ");
                return RC::Abort;
            }

            if (type == WR){
                if (node_l->IsFrozen()){
                    printf("leaf node frozen. ");
                    return RC::Abort;
                }
                auto rett = ATOM_CAS(leaf_row->is_updated, false, true);
                if(!rett){
//                    printf("set control fail. \n");
                    return RC::Abort;
                }
            }

        }else{
            printf("leaf node read error:%lu. \n ", key);
        }
    }

    if (payload_sz >0){
        item = reinterpret_cast<void *>(payload_);
    } else{
        rc = RC::Abort;
        printf("abort, type:%u , node_i: %p, node_l: %p. \n", type, node_i, node_l);
    }

    return rc;
}
#endif

//int index_btree_store::Scan(char * start_key, uint32_t key_size ,
//                            uint32_t range, void **output){
//    int count =0;
//    int scanned = 0;
//    std::vector<row_m *> results;
//
//    auto iter = this->RangeScanBySize( start_key, key_size, range);
//    for (scanned = 0; (scanned < range); ++scanned) {
//        row_m *row_ = iter ->GetNext();
//        if (row_ == nullptr) {
//            break;
//        }
//
//        results.push_back(row_);
//    }
//
//    *output = &results;
//
//    return count;
//}

//int index_btree_store::index_scan(idx_key_t key, int range, void** output) {
//    int count =0;
//
//    uint32_t key_size = parameters.key_size;
//    count = Scan(reinterpret_cast<  char *>(&key),key_size,range, output);
//
//    return count;
//}
//RC IndexBtree::index_read(idx_key_t key, void *& item) {
//    assert(false);
//    return RCOK;
//}
//RC IndexBtree::index_read(idx_key_t key, void *& item, int part_id) {
//    return index_read(key, item, part_id, 0);
//}
//RC IndexBtree::index_read(idx_key_t key, void *& item, int part_id, int thd_id) {
////    std::array<std::pair<void *, void *>,10> ptr_vector;
//    return index_read(key, item, RD,   part_id, 0);
//}

#endif