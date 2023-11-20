//
// Created by zhangqian on 2023/4/11.
//
#pragma once
#ifndef MICA_TRANSACTION_CBTREE_INDEX_H_
#define MICA_TRANSACTION_CBTREE_INDEX_H_

#include <storage/hopscotch_set.h>
#include <storage/row_meta.h>
#include <stack>
#include "../common.h"
#include "../util/type_traits.h"
#include "row_mica.h"

namespace mica {
namespace transaction {
template <class StaticConfig >
class CBtreeIndex;

constexpr static int MAX_FREEZE_RETRY = 4;//2 3 4
constexpr static int MAX_INSERT_RETRY = 2;//4 6 8

struct ParameterSet {
    uint32_t split_threshold;
    uint32_t merge_threshold;
    uint32_t leaf_node_size;
    uint32_t payload_size;
    uint32_t key_size;

    ParameterSet() : split_threshold(3072), merge_threshold(1024),
                     leaf_node_size(4096), payload_size(8), key_size(8) {}

    ParameterSet(uint32_t split_threshold_, uint32_t merge_threshold_,
                 uint32_t leaf_node_size_,  uint32_t payload_size_, uint32_t key_size_)
            : split_threshold(split_threshold_),
              merge_threshold(merge_threshold_),
              leaf_node_size(leaf_node_size_), payload_size(payload_size_),
              key_size(key_size_){}

    ~ParameterSet()  = default;
};

struct ReturnCode {
    enum RCC {
        RetInvalid,           //0
        RetOk,                //1
        RetKeyExists,         //2
        RetNotFound,          //3
        RetNodeFrozen,        //4
        RetCASFail,           //5
        RetNotEnoughSpace,    //6
        RetNotNeededUpdate,   //7
        RetRetryFailure,      //8 //9
        RetDirty
    };

    uint8_t rc;

    constexpr explicit ReturnCode(uint8_t r) : rc(r) {}

    constexpr ReturnCode() : rc(RetInvalid) {}

    ~ReturnCode() = default;

    constexpr bool inline IsInvalid() const { return rc == RetInvalid; }

    constexpr bool inline IsOk() const { return rc == RetOk; }

    constexpr bool inline IsKeyExists() const { return rc == RetKeyExists; }

    constexpr bool inline IsNotFound() const { return rc == RetNotFound; }

    constexpr bool inline IsNotNeeded() const { return rc == RetNotNeededUpdate; }

    constexpr bool inline IsNodeFrozen() const { return rc == RetNodeFrozen; }

    constexpr bool inline IsCASFailure() const { return rc == RetCASFail; }

    constexpr bool inline IsNotEnoughSpace() const { return rc == RetNotEnoughSpace; }
    constexpr bool inline IsRetryFailure() const { return rc == RetRetryFailure; }
    constexpr bool inline IsRetDirty() const { return rc == RetDirty; }

    static inline ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }

    static inline ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }

    static inline ReturnCode CASFailure() { return ReturnCode(RetCASFail); }

    static inline ReturnCode Ok() { return ReturnCode(RetOk); }

    static inline ReturnCode NotNeededUpdate() { return ReturnCode(RetNotNeededUpdate); }

    static inline ReturnCode NotFound() { return ReturnCode(RetNotFound); }

    static inline ReturnCode NotEnoughSpace() { return ReturnCode(RetNotEnoughSpace); }
    static inline ReturnCode RetryFailure() { return ReturnCode(RetRetryFailure); }
    static inline ReturnCode WriteDirty() { return ReturnCode(RetDirty); }
};

struct NodeHeader {
    // Header:
    // |-----64 bits----|---32 bits---|---32 bits---|
    // |   status word  |     size    | sorted count|
    //
    // Sorted count is actually the index into the first metadata entry for unsorted records.
    // Size is node size(node header, record meta entries, block space).
    // Status word(64-bit) is subdivided into five fields.
    //    (Internal nodes only use the first two (control and frozen) while leaf nodes use all the five.)
    // Following the node header is a growing array of record meta entries.
    struct StatusWord {
        uint64_t word = 0;

        StatusWord() : word(0) {}

        explicit StatusWord(volatile uint64_t word) : word(word) {}

        static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
        static const uint64_t kFrozenMask = uint64_t{0x1} << 60;            // Bit 61
        static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
        static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;    // Bits 44-23
        static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;    // Bits 22-1

        inline StatusWord Freeze() {
            return StatusWord{word | kFrozenMask};
        }

        inline StatusWord UnFreeze() {
            return StatusWord{word & ~kFrozenMask};
        }

        inline bool IsFrozen() {
            bool is_frozen = (word & kFrozenMask) > 0;
            return is_frozen;
        }

        inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }

        inline void SetRecordCount(uint16_t count) {
            word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
        }

        inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }

        inline void SetBlockSize(uint32_t sz) {
            word = (word & (~kBlockSizeMask)) | (uint64_t{sz} << 22);
        }

        inline uint32_t GetDeletedSize() { return (uint32_t) (word & kDeleteSizeMask); }

        inline void SetDeleteSize(uint32_t sz) {
            word = (word & (~kDeleteSizeMask)) | uint64_t{sz};
        }

        inline void PrepareForInsert(uint32_t sz) {
            M_ASSERT(sz > 0, "PrepareForInsert fail, sz <=0. ");
            // Increment [record count] by one and [block size] by payload size(total size)
            word += ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
        inline void FailForInsert(uint32_t sz) {
            // decrease [record count] by one and [block size] by payload size
            word -= ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
    };

    uint32_t size = 0;
    uint32_t sorted_count = 0;
    StatusWord status;

    NodeHeader() : size(0), sorted_count(0) {}

    inline StatusWord GetStatus() {
        return status;
    }
};

template <class StaticConfig>
struct Conflicts {
    struct ConflictItem {
        ConflictItem(): key(0), newest(nullptr) {};

        ~ConflictItem() {}
        uint64_t key ;
        AggressiveRowHead<StaticConfig> *newest ;
    };

    std::vector<ConflictItem> items;

    Conflicts()  {}
    ~Conflicts() {  }

    inline void Append(uint64_t key_, AggressiveRowHead<StaticConfig> *newest_) {
        ConflictItem item;
        item.newest = newest_;
        item.key = key_;
        items.emplace_back(item);
    }
    inline ConflictItem Get(uint32_t i) {
        auto get = items[i];
        return get;
    }
    inline void Clear() {
        items.clear();
    }
    inline uint32_t Size(){
        return items.size();
    }
};

//template <class StaticConfig>
//typedef std::map<uint64_t, message_upt> StdMapp; //newest

template <class StaticConfig>
class BaseNode {
public:
    std::map<uint64_t, AggressiveRowHead<StaticConfig>* > *update_messages;
    bool is_leaf = false;
    NodeHeader header;
    row_m row_meta[0];

    static const inline int KeyCompare(const char *key1, uint32_t size1,
                                       const char *key2, uint32_t size2) {
        if (!key1) {
            return -1;
        } else if (!key2) {
            return 1;
        }
        int cmp;

        size_t min_size = std::min<size_t>(size1, size2);
        if (min_size < 16) {
            cmp = my_memcmp(key1, key2, min_size);
        } else {
            cmp = memcmp(key1, key2, min_size);
        }
        if (cmp == 0) {
            return size1 - size2;
        }
        return cmp;
    }
    static const inline int my_memcmp(const char *key1, const char *key2, uint32_t size) {
        for (uint32_t i = 0; i < size; i++) {
            if (key1[i] != key2[i]) {
                return key1[i] - key2[i];
            }
        }
        return 0;
    }

    // Set the frozen bit to prevent future modifications to the node
    bool Freeze(){
        NodeHeader::StatusWord expected = header.GetStatus();
        if (expected.IsFrozen()) { return false; }
        auto ret =  ATOM_CAS(this->GetHeader()->status.word,
                             (expected.word), expected.Freeze().word);
        assert(ret);
        return ret;
    }
    bool UnFreeze(){
        NodeHeader::StatusWord expected = header.GetStatus();
        if (!expected.IsFrozen()) { return false; }
        auto ret =  ATOM_CAS(this->GetHeader()->status.word,
                             (expected.word), expected.UnFreeze().word);
        return ret;
    }

    explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
        header.size = size;
    }

    inline bool IsLeaf() { return is_leaf; }

    inline NodeHeader  *GetHeader() { return &header; }

    inline bool IsFrozen() {
        return GetHeader()->GetStatus().IsFrozen();
    }
    inline bool GetRawRow(row_m meta, char **data, char **key, uint64_t *payload) {
        char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
        if (data != nullptr) {
            *data = tmp_data;
        }
        auto padded_key_len = meta.GetPaddedKeyLength();
        if (key != nullptr) {
            // zero key length dummy record
            *key = padded_key_len == 0 ? nullptr : tmp_data;
        }
        //if innernode this payload may be nullptr
        if (payload != nullptr) {
            uint64_t tmp_payload;
            tmp_payload = *reinterpret_cast<uint64_t *> (tmp_data + padded_key_len);

            *payload = tmp_payload;
        }

        return true;
    }
    inline char *GetKey(row_m meta) {
//        if (!meta.IsVisible()) {
//            return nullptr;
//        }
        return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
    }
    inline row_m GetRowMeta(uint32_t i) {
        // ensure the metadata is installed
        auto meta =  *reinterpret_cast<row_m *>(row_meta + i) ;
        return row_m{meta};
    }


};

template <class StaticConfig>
class LeafNode;

template <class StaticConfig>
class Stack;

template <class StaticConfig>
class InternalNode : public BaseNode<StaticConfig> {
public:

    static void New(InternalNode **new_node, uint32_t alloc_size )   {

        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, CL_SIZE) );

        (*new_node)->header.size = alloc_size;
        (*new_node)->update_messages = new std::map<uint64_t, AggressiveRowHead<StaticConfig>*>();
    }

    static void New(InternalNode *src_node,  char *key, uint32_t key_size,
                     uint64_t left_child_addr, uint64_t right_child_addr,
                     InternalNode **new_node )   {
        size_t alloc_size = src_node->GetHeader()->size;
        alloc_size = alloc_size + row_m::PadKeyLength(key_size);
        alloc_size = alloc_size + sizeof(right_child_addr) + sizeof(row_m);

        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, CL_SIZE) );

        new(*new_node) InternalNode(alloc_size, src_node, 0, src_node->header.sorted_count,
                                    key, key_size, left_child_addr, right_child_addr);

        (*new_node)->update_messages = new std::map<uint64_t, AggressiveRowHead<StaticConfig>*>();

    }

    static void New( char * key, uint32_t key_size, uint64_t left_child_addr,
                     uint64_t right_child_addr, InternalNode **new_node ) {
        size_t alloc_size = sizeof(InternalNode);
        alloc_size = alloc_size + row_m::PadKeyLength(key_size);
        alloc_size = alloc_size + sizeof(left_child_addr) + sizeof(right_child_addr);
        alloc_size = alloc_size + sizeof(row_m) * 2;

        *new_node =  reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, CL_SIZE) );

        new(*new_node) InternalNode(alloc_size, key, key_size, left_child_addr, right_child_addr);
        (*new_node)->update_messages = new std::map<uint64_t, AggressiveRowHead<StaticConfig>*>();
    }
    static void  New(InternalNode *src_node, uint32_t begin_meta_idx,
                           uint32_t nr_records,   char * key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           InternalNode **new_node, uint64_t left_most_child_addr ) {
        // Figure out how large the new node will be
        size_t alloc_size = sizeof(InternalNode);
        if (begin_meta_idx > 0) {
            // Will not copy from the first element (dummy key), so add it here
            alloc_size += src_node->row_meta[0].GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        assert(nr_records > 0);
        for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
            row_m meta = src_node->row_meta[i];
            alloc_size += meta.GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        // Add the new key, if provided
        if (key) {
            M_ASSERT(key_size > 0, "key_size > 0.");
            alloc_size += (row_m::PadKeyLength(key_size) + sizeof(uint64_t) + sizeof(row_m));
        }

        *new_node = reinterpret_cast<InternalNode *>(_mm_malloc(alloc_size, CL_SIZE) );

        new (*new_node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                                     key, key_size, left_child_addr, right_child_addr, left_most_child_addr);
        (*new_node)->update_messages = new std::map<uint64_t, AggressiveRowHead<StaticConfig>*>();

    }

    ~InternalNode() = default;

    InternalNode(uint32_t node_size,   char * key, uint32_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr);

    InternalNode(uint32_t node_size, InternalNode *src_node,
                 uint32_t begin_meta_idx, uint32_t nr_records,
                   char * key, uint32_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr,
                 uint64_t left_most_child_addr = 0,
                 uint32_t value_size = sizeof(uint64_t));

    bool PrepareForSplit(Stack<StaticConfig> &stack, uint32_t split_threshold,
                         char * key, uint32_t key_size,
                         uint64_t left_child_addr, uint64_t right_child_addr,
                         InternalNode **new_node, bool backoff );

    uint32_t GetChildIndex(const char * key, uint32_t key_size, bool get_le = true);

    inline uint64_t *GetPayloadPtr(row_m meta) {
        char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
        return reinterpret_cast<uint64_t *>(ptr);
    }

    ReturnCode Update(row_m meta, InternalNode *old_child, InternalNode *new_child ) ;

    inline BaseNode<StaticConfig> *GetChildByMetaIndex(uint32_t index) {
        uint64_t child_addr;
        row_m red_child = this->row_meta[index];

        this->GetRawRow(red_child, nullptr, nullptr, &child_addr);
//        printf("GetChildByMetaIndex: %u, %lu \n", index, child_addr);
        if (child_addr == 0) return nullptr;

        BaseNode<StaticConfig> *rt_node = reinterpret_cast<BaseNode<StaticConfig> *> (child_addr);
        return rt_node;
    }

};

enum Uniqueness {
    IsUnique, Duplicate, ReCheck, NodeFrozen
};

template <class StaticConfig>
class LeafNode : public BaseNode<StaticConfig> {
public:
    static void New(LeafNode **mem, uint32_t node_size );

    static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
        uint32_t used_space = sizeof(LeafNode);
        used_space = used_space + status.GetBlockSize();
        used_space = used_space + (status.GetRecordCount() * sizeof(row_m));
//        LOG_DEBUG("LeafNode::GetUsedSpace: %u ",used_space);
        return used_space;
    }

    explicit LeafNode(uint32_t node_size) : BaseNode<StaticConfig>(true, node_size){}

    ~LeafNode() = default;

    uint32_t GetFirstGreater(char *key);

    ReturnCode Insert_LF(uint64_t key, uint32_t key_size,
                      AggressiveRowHead<StaticConfig>* &row_head,
                      uint64_t payload_ptr, uint32_t payload_size,
                      uint32_t split_threshold );
    ReturnCode Insert_Append_LF(uint64_t key, uint32_t key_size,
                            AggressiveRowHead<StaticConfig>* &row_head,
                            uint32_t payload_size, uint32_t split_threshold);

    bool Read(uint64_t key, uint32_t key_size, row_m **meta);

    bool PrepareForSplit(Stack<StaticConfig> &stack,
                         uint32_t split_threshold,
                         uint32_t key_size, uint32_t payload_size,
                         LeafNode **left, LeafNode **right,
                         InternalNode<StaticConfig> **new_parent, bool backoff,
                         Conflicts<StaticConfig> *conflicts);

    uint32_t SortMetaByKey(std::vector<row_m> &vec, bool visible_only);

    void CopyFrom(LeafNode *node,
                  typename std::vector<row_m>::iterator begin_it,
                  typename std::vector<row_m>::iterator end_it ,
                  Conflicts<StaticConfig> *conflicts);


    ReturnCode SearchRowMeta(uint64_t key, uint32_t key_size,
                             row_m **out_metadata, uint32_t start_pos = 0,
                             uint32_t end_pos = (uint32_t) -1,
                             bool check_concurrency = true );


private:
    Uniqueness CheckUnique(uint64_t key, uint32_t key_size);
    Uniqueness RecheckUnique(char * key, uint32_t key_size, uint32_t end_pos);

};

template <class StaticConfig >
struct Stack {
    struct Frame {
        Frame() : node(nullptr), meta_index() {}
        ~Frame() {}
        InternalNode<StaticConfig>  *node;
        uint32_t meta_index;
    };

    static const uint32_t kMaxFrames = 32;
    Frame frames[kMaxFrames];
    volatile uint32_t num_frames;
    BaseNode<StaticConfig> *root;
    CBtreeIndex<StaticConfig> *tree;

    Stack() : num_frames(0) {}
    ~Stack() { num_frames = 0; }

    inline void Push(InternalNode<StaticConfig> *node, uint32_t meta_index) {
        M_ASSERT(num_frames < kMaxFrames,"stack push num_frames < kMaxFrames.");
        auto &frame = frames[num_frames++];
        frame.node = node;
        frame.meta_index = meta_index;
    }
    inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }
    inline void Clear() {
        root = nullptr;
        num_frames = 0;
    }
    inline bool IsEmpty() { return num_frames == 0; }
    inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }
    inline BaseNode<StaticConfig> *GetRoot() { return root; }
    inline void SetRoot(BaseNode<StaticConfig> *node) { root = node; }
};

template <class StaticConfig>
class CBtreeIndex{
public:
    CBtreeIndex(DB<StaticConfig>* db, Table<StaticConfig>* main_tbl, Table<StaticConfig>* idx_tbl);

    bool init(Transaction<StaticConfig>* txn, uint32_t key_size );
    bool Initt(ParameterSet param, Transaction<StaticConfig>* txn, uint32_t key_size);

    RC  index_insert(Transaction<StaticConfig>* txn, uint64_t key,
                     void* &row_head,
                     uint64_t payload_ptr, uint32_t payload_size );
    RC  index_insert_buffer( uint64_t key, AggressiveRowHead<StaticConfig>* &row_head,
                             uint32_t payload_size );
    RC  Insert_Batch( LeafNode<StaticConfig> *node_l, uint32_t key_size,
                      uint32_t payload_size, uint32_t split_threshold);

    RC  index_read( uint64_t key, void *&item, access_t type);
    RC  index_read_buffer( uint64_t key, void *&item, access_t type, int part_id);
    bool  index_read_buffer_again(Transaction<StaticConfig>* txn, uint64_t key,
                                  void *&item, access_t type, int part_id);

    bool TraverseToTarget( Stack<StaticConfig> *stack, uint64_t key,
                           uint32_t key_size, AggressiveRowHead<StaticConfig> ** messge_upt,
                           LeafNode<StaticConfig> **l_node,
                           InternalNode<StaticConfig> **i_node,
                           bool le_child = true);
    LeafNode<StaticConfig> *TraverseToLeaf(Stack<StaticConfig> *stack,
                                           uint64_t key, uint32_t key_size,
                                           bool le_child = true);

    bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr);

    bool ChooseChildToPushdown(BaseNode<StaticConfig> *node_i, LeafNode<StaticConfig> **node_l);
    ReturnCode Insert(uint64_t key, uint32_t key_size,
                      AggressiveRowHead<StaticConfig>* &row_head,
                      uint64_t payload_ptr, uint32_t payload_size,
                      Conflicts<StaticConfig> *conflicts);
    ReturnCode Insert_Append(uint64_t key, uint32_t key_size,
                            AggressiveRowHead<StaticConfig>* &row_head,
                            uint32_t payload_size,
                            Conflicts<StaticConfig> *conflicts);

    inline BaseNode<StaticConfig> *GetRootNodeSafe() {
        auto root_node = root;
        return reinterpret_cast<BaseNode<StaticConfig>  *>(root_node);
    }

    static constexpr uint64_t kHaveToAbort = static_cast<uint64_t>(-1);
    static constexpr uint64_t kDataSize =  16;

private:
    BaseNode<StaticConfig> *root;
    uint32_t key_sz;
    ParameterSet parameters;
    tsl::hopscotch_set<uint64_t> conflicts_k ; // inserts and update conflicts
    tsl::hopscotch_set<uint64_t> conflicts_txn ;

    DB<StaticConfig>* db_;
    Table<StaticConfig>* main_tbl_;
    Table<StaticConfig>* idx_tbl_;
};

}
}

#include "cbtree_index_impl.h"

#endif