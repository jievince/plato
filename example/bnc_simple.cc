/*
  Tencent is pleased to support the open source community by making
  Plato available.
  Copyright (C) 2019 THL A29 Limited, a Tencent company.
  All rights reserved.

  Licensed under the BSD 3-Clause License (the "License"); you may
  not use this file except in compliance with the License. You may
  obtain a copy of the License at

  https://opensource.org/licenses/BSD-3-Clause

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" basis,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License.

  See the AUTHORS file for names of contributors.
*/

#include <cstdint>
#include <cstdlib>

#include "glog/logging.h"
#include "gflags/gflags.h"

#include "boost/format.hpp"
#include "boost/iostreams/stream.hpp"
#include "boost/iostreams/filter/gzip.hpp"
#include "boost/iostreams/filtering_stream.hpp"

#include "plato/util/nebula_writer.h"
#include "plato/graph/graph.hpp"
#include "plato/algo/bnc/betweenness.hpp"

DEFINE_string(input,       "",     "input file, in csv format, without edge data");
DEFINE_string(output,      "",     "output directory, store the closeness result");
DEFINE_string(vtype,       "uint32",                 "");
DEFINE_bool(is_directed,   false,  "is graph directed or not");
DEFINE_bool(need_encode,   false,                    "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_int32(alpha,        -1,     "alpha value used in sequence balance partition");
DEFINE_bool(part_by_in,    false,  "partition by in-degree");
DEFINE_int32(chosen,       -1,     "chosen vertex");
DEFINE_int32(max_iteration, 0,     "");
DEFINE_double(constant,     2,     "");

void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

template <typename VID_T>
void run_bnc_simple() {
  plato::stop_watch_t watch;
  auto& cluster_info = plato::cluster_info_t::get_instance();

  plato::vid_encoder_t<plato::empty_t, VID_T> single_data_encoder;
  plato::distributed_vid_encoder_t<plato::empty_t, VID_T> distributed_data_encoder;

  plato::vencoder_t<plato::empty_t, VID_T> encoder_ptr = nullptr;
  if (FLAGS_need_encode) {
    if (FLAGS_encoder == "single") {
      encoder_ptr = &single_data_encoder;
    } else {
      encoder_ptr = &distributed_data_encoder;
    }
  }

  plato::graph_info_t graph_info(FLAGS_is_directed);
  auto graph = plato::create_dualmode_seq_from_path<plato::empty_t, VID_T>(&graph_info, FLAGS_input,
      plato::edge_format_t::CSV, plato::dummy_decoder<plato::empty_t>,
      FLAGS_alpha, FLAGS_part_by_in, encoder_ptr);

  plato::algo::bader_opts_t opts;
  opts.chosen_ = FLAGS_chosen;
  opts.max_iteration_ = FLAGS_max_iteration;
  opts.constant_ = FLAGS_constant;
  watch.mark("t0");
  using bcsr_spec_t = plato::bcsr_t<plato::empty_t, plato::sequence_balanced_by_destination_t>;
  using dcsc_spec_t = plato::dcsc_t<plato::empty_t, plato::sequence_balanced_by_source_t>;

  plato::dualmode_engine_t<dcsc_spec_t, bcsr_spec_t> engine (
    std::shared_ptr<dcsc_spec_t>(&graph.second,  [](dcsc_spec_t*) { }),
    std::shared_ptr<bcsr_spec_t>(&graph.first, [](bcsr_spec_t*) { }),
    graph_info);

  plato::algo::bader_betweenness_t<dcsc_spec_t, bcsr_spec_t, double> bader(&engine, graph_info, opts);
  bader.compute();

  if (!boost::starts_with(FLAGS_output, "nebula:")) {
    plato::thread_local_fs_output os(FLAGS_output, (boost::format("%04d_") % cluster_info.partition_id_).str(), true);

    bader.save([&] (plato::vid_t v_i, double value) {
      auto& fs_output = os.local();
      if (encoder_ptr != nullptr) {
        fs_output << encoder_ptr->decode(v_i) << "," << value << "\n";
      } else {
        fs_output << v_i << "," << value << "\n";
      }
    });
  } else {
    if (encoder_ptr != nullptr) {
      struct Item {
        VID_T vid;
        double value;
        std::string toString() const {
          return std::to_string(value);
        }
      };
      plato::thread_local_nebula_writer<Item> writer(FLAGS_output);

      bader.save([&] (plato::vid_t v_i, double value) {
        auto& buffer = writer.local();
        buffer.add(Item{encoder_ptr->decode(v_i), value});
      });
    } else {
      struct Item {
        plato::vid_t vid;
        double value;
        std::string toString() const {
          return std::to_string(value);
        }
      };
      plato::thread_local_nebula_writer<Item> writer(FLAGS_output);

      bader.save([&] (plato::vid_t v_i, double value) {
        auto& buffer = writer.local();
        buffer.add(Item{v_i, value});
      });
    }
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "bnc done const: " << watch.show("t0") / 1000.0 << "s";
  }
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();

  init(argc, argv);
  cluster_info.initialize(&argc, &argv);
  LOG(INFO) << "partitions: " << cluster_info.partitions_ << " partition_id: " << cluster_info.partition_id_ << std::endl;

  if (FLAGS_vtype == "uint32") {
    run_bnc_simple<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_bnc_simple<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_bnc_simple<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_bnc_simple<int64_t>();
  } else if (FLAGS_vtype == "string") {
    run_bnc_simple<std::string>();
  }
  else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}

