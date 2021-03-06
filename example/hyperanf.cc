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

#include "plato/graph/graph.hpp"
#include "plato/graph/partition/sequence.hpp"
#include "plato/graph/structure/bcsr.hpp"
#include "plato/graph/structure/dcsc.hpp"
#include "plato/algo/hyperanf/hyperanf.hpp"

DEFINE_string(input,       "",      "input file, in csv format, without edge data");
DEFINE_bool(is_directed,   false,   "is graph directed or not");
DEFINE_string(vtype,         "uint32",                 "");
DEFINE_bool(need_encode,     false,                    "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_bool(part_by_in,    true,   "partition by in-degree");
DEFINE_int32(alpha,        -1,      "alpha value used in sequence balance partition");
DEFINE_uint32(iterations,  20,     "number of iterations");
DEFINE_uint32(bits,         6,      "hyperloglog bit width used for cardinality estimation");

bool string_not_empty(const char*, const std::string& value) {
  if (0 == value.length()) { return false; }
  return true;
}

DEFINE_validator(input, &string_not_empty);

void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

template <typename VID_T>
void run_hyperanf() {
  //using GRAGH_T = std::pair<bcsr_t<EDATA, sequence_balanced_by_destination_t>,dcsc_t<EDATA, sequence_balanced_by_source_t>>;
  plato::stop_watch_t watch;
  auto& cluster_info = plato::cluster_info_t::get_instance();

  using bcsr_spec_t = plato::bcsr_t<plato::empty_t, plato::sequence_balanced_by_destination_t>;
  using dcsc_spec_t = plato::dcsc_t<plato::empty_t, plato::sequence_balanced_by_source_t>;

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

  plato::algo::hyperanf_opts_t opts;
  opts.iteration_ = FLAGS_iterations;

  watch.mark("t0");
  /*
  double avg_distance = plato::algo::ComputerAvgDistanceWithWidth(graph.second, graph.first,
      graph_info, opts, FLAGS_bits);
  */
  double avg_distance;
    switch (FLAGS_bits) {
    case 6:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 6>(graph.second, graph.first, graph_info, opts);
      break;
    
    case 7:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 7>(graph.second, graph.first, graph_info, opts);
      break;
    case 8:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 8>(graph.second, graph.first, graph_info, opts);
      break;
    case 9:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 9>(graph.second, graph.first, graph_info, opts);
      break;
    case 10:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 10>(graph.second, graph.first, graph_info, opts);
      break;
    case 11:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 11>(graph.second, graph.first, graph_info, opts);
      break;
    case 12:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 12>(graph.second, graph.first, graph_info, opts);
      break;
    case 13:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 13>(graph.second, graph.first, graph_info, opts);
      break;
    case 14:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 14>(graph.second, graph.first, graph_info, opts);
      break;
    case 15:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 15>(graph.second, graph.first, graph_info, opts);
      break;
    case 16:
      avg_distance = plato::algo::hyperanf<dcsc_spec_t, bcsr_spec_t, 16>(graph.second, graph.first, graph_info, opts);
      break;
    default:
      CHECK(false) << "unsupport hyperloglog bit width: " << FLAGS_bits
        << ", supported range is in [6, 16]";
        
  }
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "hyperanf done, avg_distance: " << avg_distance << ", cost: "
      << watch.show("t0") / 1000.0 << "s";
  }
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();

  init(argc, argv);
  cluster_info.initialize(&argc, &argv);

  if (FLAGS_vtype == "uint32") {
    run_hyperanf<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_hyperanf<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_hyperanf<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_hyperanf<int64_t>();
  }
  // else if (FLAGS_vtype == "string") {
  //   run_hyperanf<std::string>();
  // }
  else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}
