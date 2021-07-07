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
#include <memory>
#include <limits>

#include "omp.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "boost/format.hpp"
#include "boost/iostreams/stream.hpp"
#include "boost/iostreams/filter/gzip.hpp"
#include "boost/iostreams/filtering_stream.hpp"
#include "boost/algorithm/string.hpp"

#include "plato/util/nebula_writer.h"
#include "plato/graph/graph.hpp"
#include "plato/algo/kcore/kcore.hpp"


DEFINE_string(input,     "",           "input edge file in csv format, every vertex must be indexed in range [0, #V)");
DEFINE_string(output,    "",           "result directory, when type is subgraph, \
                                        if output is set, save csv of each core with prefix [output]/[K]_core. \
                                        when type is vertex, if output is set, save each vertex's kcore in csv format");
DEFINE_string(type,      "subgraph",   "calculate k-core for each 'vertex' or 'subgraph', default: 'subgraph'");
DEFINE_uint64(vertices,  0,            "vertex count, if set to 0, system will count for you");
DEFINE_uint64(edges,     0,            "edge count, if set to 0, system will count for you");
DEFINE_uint32(kmin,      1,            "calculate the k-Core for k the range [kmin,kmax], only take effect when type is subgraph");
DEFINE_uint32(kmax,      1000000,      "calculate the k-Core for k the range [kmin,kmax], \
                                        only take effect when type is subgraph.");
DEFINE_bool(is_directed, true,         "if set to false, system will add reversed edges automatically");
DEFINE_bool(need_encode,   false,                    "");
DEFINE_string(vtype,       "uint32",                 "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_int32(alpha,      -1,           "alpha value used in sequence balance partition");
DEFINE_bool(part_by_in,  false,        "partition by in-degree");

/**
 * @brief string not empty validator
 * @param value
 * @return
 */
bool string_not_empty(const char*, const std::string& value) {
  if (0 == value.length()) { return false; }
  return true;
}

DEFINE_validator(input, &string_not_empty);

/**
 * @brief
 * @param argc
 * @param argv
 */
void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

template <typename VID_T>
void run_kcore_simple() {
  plato::stop_watch_t watch;
  auto& cluster_info = plato::cluster_info_t::get_instance();

  using namespace plato;
  using namespace plato::algo;

  kcore_calc_type_t type_c = kcore_calc_type_t::SUBGRAPH;
  if ("subgraph" == FLAGS_type) {
    type_c = kcore_calc_type_t::SUBGRAPH;
  } else {
    FLAGS_kmin = 0;
    FLAGS_kmax = 1000000;
    type_c = kcore_calc_type_t::VERTEX;
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "input:       " << FLAGS_input;
    LOG(INFO) << "output:      " << FLAGS_output;
    LOG(INFO) << "vs:          " << FLAGS_vertices;
    LOG(INFO) << "edges:       " << FLAGS_edges;
    LOG(INFO) << "kmin:        " << FLAGS_kmin;
    LOG(INFO) << "kmax:        " << FLAGS_kmax;
    LOG(INFO) << "is_directed: " << FLAGS_is_directed;
  }

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

  auto graph = create_bcsr_seqs_from_path<plato::empty_t, VID_T>(&graph_info, FLAGS_input, plato::edge_format_t::CSV,
      plato::dummy_decoder<plato::empty_t>, FLAGS_alpha, FLAGS_part_by_in, encoder_ptr, false);

  auto coreness = kcore_algo_t::compute_shell_index(graph_info, *graph);

  watch.mark("t0");
  if (kcore_calc_type_t::VERTEX == type_c) {
    if (!boost::starts_with(FLAGS_output, "nebula:")) {
      plato::thread_local_fs_output os(FLAGS_output, (boost::format("%04d_") % cluster_info.partition_id_).str(), true);
      coreness.template foreach<vid_t>(
        [&](vid_t v_i, vid_t* pcrns) {
          auto& fs_output = os.local();
          if (encoder_ptr != nullptr) {
            fs_output << encoder_ptr->decode(v_i) << "," << encoder_ptr->decode(*pcrns) << "\n";
          } else {
            fs_output << v_i << "," << *pcrns << "\n";
          }
          return 0;
        });
    } else {
      if (encoder_ptr != nullptr) {
        struct Item {
          VID_T vid;
          VID_T cur_k;
          std::string toString() const {
            return std::to_string(cur_k);
          }
        };
        plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
        coreness.template foreach<vid_t>(
          [&](vid_t v_i, vid_t* pcrns) {
            auto& buffer = writer.local();
            buffer.add(Item{encoder_ptr->decode(v_i), encoder_ptr->decode(*pcrns)});
            return 0;
          });
      } else {
        struct Item {
          plato::vid_t vid;
          plato::vid_t cur_k;
          std::string toString() const {
            return std::to_string(cur_k);
          }
        };
        plato::thread_local_nebula_writer<Item> writer(FLAGS_output);
        coreness.template foreach<vid_t>(
          [&](vid_t v_i, vid_t* pcrns) {
            auto& buffer = writer.local();
            buffer.add(Item{v_i, *pcrns});
            return 0;
          });
      }
    }
  } else {
    plato::bitmap_t<> lefted(graph_info.max_v_i_ + 1);
    lefted.fill();

    vid_t saved = 0;
    vid_t cur_k = 0;

    if (!boost::starts_with(FLAGS_output, "nebula:")) {
      while (saved < graph_info.vertices_) {
        plato::thread_local_fs_output os_sub((boost::format("%s/%u_core") % FLAGS_output % cur_k).str(), (boost::format("%04d_") % cluster_info.partition_id_).str(), true);

        auto save_kcore_subgraph =
          [&](vid_t src, vid_t dst, uint32_t cur_k) {
            auto& fs_output = os_sub.local();
            if (encoder_ptr != nullptr) {
              fs_output << encoder_ptr->decode(src) << "," << encoder_ptr->decode(dst) << "\n";
            } else {
              fs_output << src << "," << dst << "\n";
            }
          };

        coreness.template foreach<vid_t>(
          [&](vid_t v_i, vid_t* pcrns) {
            if (*pcrns == cur_k) {
              auto adjs = graph->neighbours(v_i);
              for (auto it = adjs.begin_; it != adjs.end_; ++it) {
                save_kcore_subgraph(v_i, it->neighbour_, cur_k);
              }
              lefted.clr_bit(v_i);
            }
            return 0;
          }, &lefted);
        saved = graph_info.max_v_i_ + 1 - lefted.count();
        MPI_Allreduce(MPI_IN_PLACE, &saved, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
        ++cur_k;
      }
    } else {
      if (encoder_ptr != nullptr) {
        struct Item {
          VID_T vid;
          VID_T dst;
          std::string toString() const {
            return std::to_string(dst);
          }
        };
        while (saved < graph_info.vertices_) {
          plato::thread_local_nebula_writer<Item> writer(FLAGS_output);

          auto save_kcore_subgraph =
            [&](vid_t src, vid_t dst, uint32_t cur_k) {
              auto& buffer = writer.local();
              buffer.add(Item{encoder_ptr->decode(src), encoder_ptr->decode(dst)});
            };

          coreness.template foreach<vid_t>(
            [&](vid_t v_i, vid_t* pcrns) {
              if (*pcrns == cur_k) {
                auto adjs = graph->neighbours(v_i);
                for (auto it = adjs.begin_; it != adjs.end_; ++it) {
                  save_kcore_subgraph(v_i, it->neighbour_, cur_k);
                }
                lefted.clr_bit(v_i);
              }
              return 0;
            }, &lefted);
          saved = graph_info.max_v_i_ + 1 - lefted.count();
          MPI_Allreduce(MPI_IN_PLACE, &saved, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
          ++cur_k;
        }
      } else {
        struct Item {
          plato::vid_t vid;
          plato::vid_t dst;
          std::string toString() const {
            return std::to_string(dst);
          }
        };
        while (saved < graph_info.vertices_) {
          plato::thread_local_nebula_writer<Item> writer(FLAGS_output);

          auto save_kcore_subgraph =
            [&](vid_t src, vid_t dst, uint32_t cur_k) {
              auto& buffer = writer.local();
              buffer.add(Item{src, dst});
            };

          coreness.template foreach<vid_t>(
            [&](vid_t v_i, vid_t* pcrns) {
              if (*pcrns == cur_k) {
                auto adjs = graph->neighbours(v_i);
                for (auto it = adjs.begin_; it != adjs.end_; ++it) {
                  save_kcore_subgraph(v_i, it->neighbour_, cur_k);
                }
                lefted.clr_bit(v_i);
              }
              return 0;
            }, &lefted);
          saved = graph_info.max_v_i_ + 1 - lefted.count();
          MPI_Allreduce(MPI_IN_PLACE, &saved, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
          ++cur_k;
        }
      }
    }
  }
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "all done, saving result cost: " << watch.showlit_seconds("t0");
  }
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();

  init(argc, argv);
  cluster_info.initialize(&argc, &argv);

  if (FLAGS_vtype == "uint32") {
    run_kcore_simple<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_kcore_simple<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_kcore_simple<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_kcore_simple<int64_t>();
  } else if (FLAGS_vtype == "string") {
    run_kcore_simple<std::string>();
  } else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}
