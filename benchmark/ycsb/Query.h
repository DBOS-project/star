//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <vector>
#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"

namespace star {
namespace ycsb {

template <std::size_t N> struct YCSBQuery {
  int32_t Y_KEY[N];
  bool UPDATE[N];
  bool cross_partition;
  std::vector<int32_t> parts;
};

template <std::size_t N> class makeYCSBQuery {
public:
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random) const {
    std::vector<int32_t> crossParts;
    YCSBQuery<N> query;
    query.cross_partition = false;
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);
    for (auto i = 0u; i < N; i++) {
      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;

        if (context.isUniform) {
          key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          if (crossParts.empty()) {
            for (int j = 0; j < context.crossPartitionPartNum; ++j) {
              if (crossParts.size() >= context.partition_num)
                break;
              int32_t pid = random.uniform_dist(0, context.partition_num - 1);
              do {
                bool good = true;
                for (int k = 0; k < j; ++k) {
                  if (crossParts[k] == pid) {
                    good = false;
                  }
                }
                if (good == true)
                  break;
                pid =  random.uniform_dist(0, context.partition_num - 1);
              } while(true);
              crossParts.push_back(pid);
            }
          }
          auto newPartitionID = crossParts[i % crossParts.size()];
          while (newPartitionID == (int32_t)partitionID) {
            newPartitionID = crossParts[random.uniform_dist(0, crossParts.size() - 1)];
          }
          query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID);
          query.cross_partition = true;
        } else {
          query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
    if ((int)N > context.crossPartitionPartNum) {
      query.parts = crossParts;
    } else {
      query.parts = std::vector<int32_t>(crossParts.begin(), crossParts.begin() + N);
    }
    return query;
  }
};
} // namespace ycsb
} // namespace star
