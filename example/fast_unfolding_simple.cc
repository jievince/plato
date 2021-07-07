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
#include "plato/algo/fast_unfolding/fast_unfolding.hpp"


DEFINE_string(input,          "",     "input file, in csv format, without edge data");
DEFINE_string(output,         "",     "output directory, store the closeness result");
DEFINE_bool(is_directed,      false,  "is graph directed or not");
DEFINE_string(vtype,         "uint32",                 "");
DEFINE_bool(need_encode,     false,                    "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_int32(alpha,           -1,     "alpha value used in sequence balance partition");
DEFINE_bool(part_by_in,       false,  "partition by in-degree");
DEFINE_int32(outer_iteration, 3,      "outer iteration of algorithm");
DEFINE_int32(inner_iteration, 2,      "inner iteration of algorithm");

void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

template <typename VID_T>
void run_fast_unfolding_simple() {
  plato::stop_watch_t watch;
  auto& cluster_info = plato::cluster_info_t::get_instance();

  LOG(INFO) << "partitions: " << cluster_info.partitions_ << " partition_id: " << cluster_info.partition_id_ << std::endl;

  watch.mark("t0");

  using edge_value_t = float;

  plato::vid_encoder_t<edge_value_t, VID_T> single_data_encoder;
  plato::distributed_vid_encoder_t<edge_value_t, VID_T> distributed_data_encoder;

  plato::vencoder_t<edge_value_t, VID_T> encoder_ptr = nullptr;
  if (FLAGS_need_encode) {
    if (FLAGS_encoder == "single") {
      encoder_ptr = &single_data_encoder;
    } else {
      encoder_ptr = &distributed_data_encoder;
    }
  }

  plato::graph_info_t graph_info(FLAGS_is_directed);

  plato::decoder_with_default_t<edge_value_t> decoder((edge_value_t)1);
  auto graph = plato::create_bcsr_seqs_from_path<edge_value_t, VID_T>(&graph_info, 
      FLAGS_input, plato::edge_format_t::CSV, decoder,
      FLAGS_alpha, FLAGS_part_by_in, encoder_ptr);
  using BCSR = plato::bcsr_t<edge_value_t, plato::sequence_balanced_by_source_t>;
  plato::algo::louvain_opts_t opts;
  opts.outer_iteration_ = FLAGS_outer_iteration;
  opts.inner_iteration_ = FLAGS_inner_iteration;
  LOG(INFO) << "outer_iteraion: " << opts.outer_iteration_ << " inner_iteraion: " << opts.inner_iteration_;
  plato::algo::louvain_fast_unfolding_t<BCSR> louvain(graph, graph_info, opts);
  louvain.compute();

  if (!boost::starts_with(FLAGS_output, "nebula:")) {
    plato::thread_local_fs_output os(FLAGS_output, (boost::format("%04d_") % cluster_info.partition_id_).str(), true);
    louvain.save([&] (plato::vid_t src, plato::vid_t label) {
      auto& fs_output = os.local();
      if (encoder_ptr != nullptr) {
        fs_output << encoder_ptr->decode(src) << "," << encoder_ptr->decode(label) << "\n";
      } else {
        fs_output << src << "," << label << "\n";
      }
    });
  } else {
    if (encoder_ptr != nullptr) {
      struct Item {
        VID_T vid;
        VID_T label;
        std::string toString() const {
          return std::to_string(label);
        }
      };
      plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
      louvain.save([&] (plato::vid_t src, plato::vid_t label) {
        auto& buffer = writer.local();
        buffer.add(Item{encoder_ptr->decode(src), encoder_ptr->decode(label)});
      });
    } else {
      struct Item {
        plato::vid_t vid;
        plato::vid_t label;
        std::string toString() const {
          return std::to_string(label);
        }
      };
      plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
      louvain.save([&] (plato::vid_t src, plato::vid_t label) {
        auto& buffer = writer.local();
        buffer.add(Item{src, label});
      });
    }
  }

  LOG(INFO) << "total cost: " << watch.show("t0") / 1000.0;
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();
  init(argc, argv);
  cluster_info.initialize(&argc, &argv);

  if (FLAGS_vtype == "uint32") {
    run_fast_unfolding_simple<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_fast_unfolding_simple<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_fast_unfolding_simple<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_fast_unfolding_simple<int64_t>();
  } else if (FLAGS_vtype == "string") {
    run_fast_unfolding_simple<std::string>();
  } else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}
