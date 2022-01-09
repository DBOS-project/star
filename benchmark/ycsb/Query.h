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
  int parts[5];
  int granules[5];
  int num_parts = 0;

  int32_t get_part(int i) {
    DCHECK(i < num_parts);
    return parts[i];
  }

  int32_t get_granule(int i) {
    DCHECK(i < num_parts);
    return granules[i];
  }

  int number_of_parts() {
    return num_parts;
  }
};

template <std::size_t N> class makeYCSBQuery {
public:
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID, uint32_t granuleID,
                          Random &random, const Partitioner & partitioner) const {
    YCSBQuery<N> query;
    query.cross_partition = false;
    query.num_parts = 1;
    query.parts[0] = partitionID;
    query.granules[0] = granuleID;
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
              0, static_cast<int>(context.keysPerGranule) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          if (query.num_parts == 1) {
            query.num_parts = 1;
            for (int j = query.num_parts; j < context.crossPartitionPartNum; ++j) {
              if (query.num_parts >= (int)context.partition_num)
                break;
              int32_t pid = random.uniform_dist(0, context.partition_num - 1);
              int32_t gid = random.uniform_dist(0, context.granules_per_partition - 1);
              do {
                bool good = true;
                for (int k = 0; k < j; ++k) {
                  if (query.parts[k] == pid && query.granules[k] == gid) {
                    good = false;
                  }
                }
                // if (partitioner.has_master_partition(pid)) // We want a partition that is not on this node.
                //   good = false;
                if (good == true)
                  break;
                pid =  random.uniform_dist(0, context.partition_num - 1);
                gid = random.uniform_dist(0, context.granules_per_partition - 1);
              } while(true);
              query.parts[query.num_parts] = pid;
              query.granules[query.num_parts] = gid;
              query.num_parts++;
            }
          }
          auto newPartitionID = query.parts[i % query.num_parts];
          auto newGranuleID = query.granules[i % query.num_parts];
          // while (newPartitionID == (int32_t)partitionID) {
          //   newPartitionID = query.parts[random.uniform_dist(0, query.num_parts - 1)];
          // }
          query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID, newGranuleID);
          query.cross_partition = true;
        } else {
          query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID, granuleID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
    return query;
  }
};
} // namespace ycsb
} // namespace star
