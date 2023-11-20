#pragma once
#ifndef MICA_UTIL_BARRIER_H_
#define MICA_UTIL_BARRIER_H_

#include "../common.h"

namespace mica {
namespace util {
static void memory_barrier() { asm volatile("" ::: "memory"); }

static void lfence() { asm volatile("lfence" ::: "memory"); }

static void sfence() { asm volatile("sfence" ::: "memory"); }

static void mfence() { asm volatile("mfence" ::: "memory"); }

static void pause() { asm volatile("pause"); }

static void clflush(volatile void* p) { asm volatile("clflush (%0)" ::"r"(p)); }

static void cpuid(unsigned int* eax, unsigned int* ebx, unsigned int* ecx,
                  unsigned int* edx) {
  asm volatile("cpuid"
               : "=a"(*eax), "=b"(*ebx), "=c"(*ecx), "=d"(*edx)
               : "0"(*eax), "2"(*ecx));
}

/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
	__sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
	__sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
	__sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
	__sync_sub_and_fetch(&(dest), value)

}
}

#endif