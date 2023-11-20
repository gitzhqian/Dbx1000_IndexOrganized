#pragma once
#ifndef MICA_UTIL_TYPE_TRAITS_H_
#define MICA_UTIL_TYPE_TRAITS_H_

#include <type_traits>

namespace std {
// Fix compiliers that claim  is_trivially_copyable<std::pair<uint64_t, uint64_t>>::value == false.
template <typename T>
struct is_trivially_copyable<std::pair<T, uint64_t>> {
  static constexpr bool value = is_trivially_copyable<T>::value;
};

}

/************************************************/
// ASSERT Helper
/************************************************/
#define M_ASSERT(cond, ...) \
	if (!(cond)) {\
		printf("ASSERTION FAILURE [%s : %d] ", \
		__FILE__, __LINE__); \
		printf(__VA_ARGS__);\
		assert(false);\
	}

#define ASSERT(cond) assert(cond)

#endif