#pragma once
#ifndef MICA_TRANSACTION_LOGGING_H_
#define MICA_TRANSACTION_LOGGING_H_

#include "../common.h"
#include "db.h"
#include "row_mica.h"
#include "row_version_pool.h"
#include "context.h"
#include "transaction.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class LoggerInterface {
 public:
  bool log(const Transaction<StaticConfig>* tx);
};

template <class StaticConfig>
class NullLogger : public LoggerInterface<StaticConfig> {
 public:
  bool log(const Transaction<StaticConfig>* tx) {
    (void)tx;
    return true;
  }
};
}
}

#endif