#pragma once

#include <cassert>
#include <global.h>

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
