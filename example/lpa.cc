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
#include "plato/algo/lpa/lpa.hpp"

DEFINE_string(input,       "",      "input file, in csv format, without edge data");
DEFINE_string(output,      "",      "output directory");
DEFINE_bool(is_directed,   false,   "is graph directed or not");
DEFINE_string(vtype,       "uint32",                 "");
DEFINE_bool(need_encode,   false,                    "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_bool(part_by_in,    true,   "partition by in-degree");
DEFINE_int32(alpha,        -1,      "alpha value used in sequence balance partition");
DEFINE_uint32(iterations,  20,     "number of iterations");

bool string_not_empty(const char*, const std::string& value) {
  if (0 == value.length()) { return false; }
  return true;
}

DEFINE_validator(input,  &string_not_empty);
DEFINE_validator(output, &string_not_empty);

void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

void print_flags(){
  LOG(INFO) << "input        : " << FLAGS_input;
  LOG(INFO) << "output       : " << FLAGS_output;
  LOG(INFO) << "is_directed  : " << FLAGS_is_directed;
  LOG(INFO) << "need_encode  : " << FLAGS_need_encode;
  LOG(INFO) << "part_by_in   : " << FLAGS_part_by_in;
  LOG(INFO) << "alpha        : " << FLAGS_alpha;
  LOG(INFO) << "iterations   : " << FLAGS_iterations;
}


template <typename VID_T>
void run_lpa() {
  plato::stop_watch_t watch;
  auto& cluster_info = plato::cluster_info_t::get_instance();

  watch.mark("t0");

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

  auto pdcsc = plato::create_dcsc_seqd_from_path<plato::empty_t, VID_T>(
    &graph_info, FLAGS_input, plato::edge_format_t::CSV,
    plato::dummy_decoder<plato::empty_t>, FLAGS_alpha, FLAGS_part_by_in, encoder_ptr
  );

  using graph_spec_t = typename std::remove_reference<decltype(*pdcsc)>::type;

  plato::algo::lpa_opts_t opts;
  opts.iteration_ = FLAGS_iterations;

  auto labels = plato::algo::lpa<graph_spec_t>(*pdcsc, graph_info, opts);

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "lpa calculation done: " << watch.show("t0") / 1000.0 << "s";
  }

  watch.mark("t0");
  {  // save result
    if (!boost::starts_with(FLAGS_output, "nebula:")) {
      plato::thread_local_fs_output os(FLAGS_output, (boost::format("%04d_") % cluster_info.partition_id_).str(), true);

      labels.template foreach<int> (
        [&](plato::vid_t v_i, plato::vid_t* pval) {
          auto& fs_output = os.local();
          if (encoder_ptr != nullptr) {
            fs_output << encoder_ptr->decode(v_i) << "," << encoder_ptr->decode(*pval) << "\n";
          } else {
            fs_output << v_i << "," << *pval << "\n";
          }
          return 0;
        }
      );
    } else {
      if (encoder_ptr != nullptr) {
        struct Item {
          VID_T vid;
          VID_T pval;
          std::string toString() const {
            return std::to_string(pval);
          }
        };
        plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
        labels.template foreach<int> (
          [&](plato::vid_t v_i, plato::vid_t* pval) {
            auto& buffer = writer.local();
            buffer.add(Item{encoder_ptr->decode(v_i), encoder_ptr->decode(*pval)});
            return 0;
          }
        );
      } else {
        struct Item {
          plato::vid_t vid;
          plato::vid_t pval;
          std::string toString() const {
            return std::to_string(pval);
          }
        };
        plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
        labels.template foreach<int> (
          [&](plato::vid_t v_i, plato::vid_t* pval) {
            auto& buffer = writer.local();
            buffer.add(Item{v_i, *pval});
            return 0;
          }
        );
      }
    }
  }
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "save result cost: " << watch.show("t1") / 1000.0 << "s";
  }
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();

  init(argc, argv);
  cluster_info.initialize(&argc, &argv);

  if (0 == cluster_info.partition_id_) {
    print_flags();
  }

  if (FLAGS_vtype == "uint32") {
    run_lpa<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_lpa<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_lpa<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_lpa<int64_t>();
  } else if (FLAGS_vtype == "string") {
    run_lpa<std::string>();
  } else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}

