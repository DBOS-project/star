#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"
#include "common/WALLogger.h"

DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");

DEFINE_int32(nop_prob, 0, "prob of transactions having nop, out of 10000");
DEFINE_int64(n_nop, 0, "total number of nop");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

bool do_tid_check = false;

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  star::ycsb::Context context;
  SETUP_CONTEXT(context);

  context.readWriteRatio = FLAGS_read_write_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.keysPerPartition = FLAGS_keys;

  context.nop_prob = FLAGS_nop_prob;
  context.n_nop = FLAGS_n_nop;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    star::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }

  if (context.log_path != "" && context.wal_group_commit_time != 0) {
    std::string redo_filename =
          context.log_path + "_group_commit.txt";
    context.logger = new star::GroupCommitLogger(redo_filename, context.group_commit_batch_size, context.wal_group_commit_time, context.emulated_persist_latency);
    LOG(INFO) << "WAL Group Commiting on[" << redo_filename << "]";
  } else {
    std::string redo_filename =
          context.log_path + "_non_group_commit.txt";
    context.logger = new star::SimpleWALLogger(redo_filename, context.emulated_persist_latency);
    LOG(INFO) << "WAL Group Commiting off";
  }

  star::ycsb::Database db;
  db.initialize(context);

  do_tid_check = false;
  star::Coordinator c(FLAGS_id, db, context);
  c.connectToPeers();
  c.start();
  return 0;
}