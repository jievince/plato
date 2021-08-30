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

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <type_traits>
#include <bitset>

#include "glog/logging.h"
#include "gflags/gflags.h"

#include "boost/format.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/iostreams/stream.hpp"
#include "boost/iostreams/filter/gzip.hpp"
#include "boost/iostreams/filtering_stream.hpp"
#include "nebula/client/Init.h"
#include "nebula/client/Config.h"
#include "nebula/client/ConnectionPool.h"

#include "plato/util/perf.hpp"
#include "plato/util/hdfs.hpp"
#include "plato/util/atomic.hpp"
#include "plato/util/nebula_writer.h"
#include "plato/graph/base.hpp"
#include "plato/graph/state.hpp"
#include "plato/graph/structure.hpp"
#include "plato/graph/message_passing.hpp"
#include "yas/types/std/unordered_set.hpp"

DEFINE_string(input,       "",      "input file, in csv format, without edge data");
DEFINE_string(output,      "",      "output directory");
DEFINE_bool(is_directed,   false,   "is graph directed or not");
DEFINE_bool(need_encode,   false,                    "");
DEFINE_string(vtype,       "uint32",                 "");
DEFINE_string(encoder,     "single","single or distributed vid encoder");
DEFINE_bool(part_by_in,    false,   "partition by in-degree");
DEFINE_int32(alpha,        -1,      "alpha value used in sequence balance partition");

DEFINE_string(ids1,      "",      "id set, split by `,`. Can not be empty.");
DEFINE_string(ids2,      "",      "id set, split by `,`. It means all vertices if empty");
DEFINE_uint32(iterations,   10,      "iterations");

using vid_set_t            = typename std::unordered_set<plato::vid_t>;
using nbr_t                = typename std::unordered_map<plato::vid_t, std::shared_ptr<vid_set_t>>;
using bitmap_spec_t        = plato::bitmap_t<>;

struct message_t {
    plato::vid_t v_i_;
    vid_set_t message_;
    
    template<typename Ar>
	void serialize(Ar &ar) {
		ar & v_i_ & message_;
	}
};

bool string_not_empty(const char*, const std::string& value) {
  if (0 == value.length()) { return false; }
  return true;
}

DEFINE_validator(input,  &string_not_empty);
DEFINE_validator(output, &string_not_empty);
DEFINE_validator(ids1, &string_not_empty);

template <typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value,
                               std::vector<VID_T>>::type
get_vids(const std::string& ids) {
    std::vector<VID_T> ret;
    if(!ids.empty()) {
        std::vector<std::string> strs;
        boost::split(strs, ids, boost::is_any_of(","));
        std::transform(
            strs.begin(), strs.end(), std::back_inserter(ret),
            [](std::string& item) { return std::strtoul(item.c_str(), nullptr, 0); });
    }
	return ret;
}

template <typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value,
                               std::vector<VID_T>>::type
get_vids(const std::string& ids) {
	std::vector<std::string> strs;
	boost::split(strs, ids, boost::is_any_of(","));
	return strs;
}

template <typename VID_T>
inline std::vector<plato::vid_t> to_vid_t(const std::vector<VID_T>& vids) {
	std::vector<plato::vid_t> ret;
    for(auto id : vids) {
      ret.emplace_back(id);
    }
    return ret;
}

inline std::vector<plato::vid_t> to_vid_t(const std::vector<std::string>& vids) {
    std::vector<plato::vid_t> ret;
    for(auto id : vids) {
      ret.emplace_back(std::strtoul(id.c_str(), nullptr, 0));
    }
    return ret;
}


void init(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
}

size_t intersection_num(const std::shared_ptr<vid_set_t> nbrs_id1, const std::shared_ptr<vid_set_t> nbrs_id2){
    size_t isc_num = 0;
    for(auto dst : *nbrs_id2) {
        if(nbrs_id1->find(dst) != nbrs_id1->end()) { 
            ++isc_num;
        }
    }
    return isc_num;
}

template <typename VID_T>
void run_jaccard() {
  plato::stop_watch_t watch;
  watch.mark("t0");
  auto& cluster_info = plato::cluster_info_t::get_instance();
  
  // parse input
  std::vector<VID_T> ids1 = get_vids<VID_T>(FLAGS_ids1);
  std::vector<VID_T> ids2 = get_vids<VID_T>(FLAGS_ids2);
  size_t ids1_num = ids1.size();
  size_t ids2_num = ids2.size();
  LOG(INFO) << "ids1_num:" << ids1_num << ", ids2:" << ids2_num;
  if(ids2_num > 0) {
    ids1.insert(ids1.end(), ids2.begin(), ids2.end());
  }

  // vid encoder
  plato::vid_encoder_t<plato::empty_t, VID_T> single_data_encoder;
  plato::distributed_vid_encoder_t<plato::empty_t, VID_T> distributed_data_encoder;
  plato::vencoder_t<plato::empty_t, VID_T> encoder_ptr = nullptr;
  if (FLAGS_need_encode) {
    if (FLAGS_encoder == "single") {
      encoder_ptr = &single_data_encoder;
    } else {
      encoder_ptr = &distributed_data_encoder;
    }
    encoder_ptr->set_vids(ids1);
  }
  
  // init graph
  plato::graph_info_t graph_info(FLAGS_is_directed);
  auto pbcsr = plato::create_bcsr_seqs_from_path<plato::empty_t, VID_T>(&graph_info, FLAGS_input,
      plato::edge_format_t::CSV, plato::dummy_decoder<plato::empty_t>,
      FLAGS_alpha, FLAGS_part_by_in, encoder_ptr);

  std::vector<plato::vid_t> encoded_vids;
  if (FLAGS_need_encode) {
    encoder_ptr->get_vids(encoded_vids);
  } else {
    encoded_vids = to_vid_t(ids1);
  }
  LOG(INFO) << "encoded_vids num:" << encoded_vids.size();
  LOG(INFO) << "vertices:" << graph_info.vertices_;

  const size_t mutices_num = 1024;
  std::mutex mutices[mutices_num];

  // init nbr_for_ids1
  auto nbr_for_ids1 = std::make_shared<nbr_t>();
  nbr_for_ids1->reserve(ids1_num);
  for(auto it_id1 = encoded_vids.begin(); it_id1 != encoded_vids.end() - ids2_num; ++it_id1){
      nbr_for_ids1->emplace(*it_id1, std::make_shared<vid_set_t>());
  }
  
  std::shared_ptr<bitmap_spec_t> active_bitmap(new bitmap_spec_t(graph_info.vertices_));
  // 1.fetch ids1's neighbours into nbr_for_ids1
  #pragma omp parallel for num_threads(cluster_info.threads_)
  for(auto it = encoded_vids.begin(); it != encoded_vids.end() - ids2_num; ++it){
      auto src = *it;
      active_bitmap->set_bit(src);
      auto neighbours = pbcsr->neighbours(src);
      for (auto it_nbr = neighbours.begin_; it_nbr != neighbours.end_; ++it_nbr) {
        auto dst = it_nbr->neighbour_;

        std::lock_guard<std::mutex> lg(mutices[src%mutices_num]);
        (*nbr_for_ids1)[src]->insert(dst);
      }
  }

  // 2.sync nbr_for_ids1 to all node
  using context_spec_t = plato::mepa_bc_context_t<message_t>;
  plato::bc_opts_t opts;
  opts.local_capacity_ = 4 * PAGESIZE;
  auto active_view = plato::create_active_v_view(pbcsr->partitioner()->self_v_view(), *active_bitmap);
  plato::broadcast_message<message_t, plato::vid_t> (active_view,
    [&](const context_spec_t& context, plato::vid_t v_i) {
        context.send(message_t{v_i, *(nbr_for_ids1->at(v_i))});
    },
    [&](int /* p_i */, const message_t& msg) {
        std::lock_guard<std::mutex> lg(mutices[msg.v_i_%mutices_num]);
        for(auto nbr: msg.message_) {
            (*nbr_for_ids1)[msg.v_i_]->insert(nbr);
        }
        return 1;
    }, opts);


  // 3. calculate the similarity between ids1 and ids2. And then save result.
  watch.mark("t1");
  if (!boost::starts_with(FLAGS_output, "nebula:")) {
      plato::thread_local_fs_output os(FLAGS_output, (boost::format("%04d_") % cluster_info.partition_id_).str(), true);
        auto do_similarity = [&](plato::vid_t id2){
            // get neighbours of id2
            auto nbrs_id2 = std::make_shared<vid_set_t>();
            auto neighbours = pbcsr->neighbours(id2);
            if(neighbours.begin_ == neighbours.end_) return;
            for (auto it_nbr = neighbours.begin_; it_nbr != neighbours.end_; ++it_nbr) {
                auto dst = it_nbr->neighbour_;
                nbrs_id2->insert(dst);
            }

            // for each id1
            #pragma omp parallel for num_threads(cluster_info.threads_)
            for(auto it_id1 = encoded_vids.begin(); it_id1 != encoded_vids.end() - ids2_num; ++it_id1){
                auto id1 = *it_id1;
                auto nbrs_id1 = nbr_for_ids1->at(id1);
            
                size_t isc_num = intersection_num(nbrs_id1, nbrs_id2);
                size_t nbrs_ids2_num = nbrs_id2->size();
                double similarity = double(isc_num)/(nbrs_id1->size() + nbrs_ids2_num - isc_num);
                auto& fs_output = os.local();
                if (encoder_ptr != nullptr) {
                    fs_output << encoder_ptr->decode(id1) << "," << encoder_ptr->decode(id2) << "," << similarity << "\n";
                } else {
                    fs_output << id1 << "," << id2 << "," << similarity << "\n";
                }

            } // end for each id1
        };

        if(ids2_num > 0) {
            #pragma omp parallel for num_threads(cluster_info.threads_)
            for(auto it = encoded_vids.begin() + ids1_num; it != encoded_vids.end(); ++it){
                do_similarity(*it);
            }
        } else {
            #pragma omp parallel for num_threads(cluster_info.threads_)
            for(plato::vid_t id = 0; id < graph_info.vertices_; id++){
                do_similarity(id);
            }
        }
      
  } else {
      CHECK(false) << "Does not support output to nebula database.";
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "save result cost: " << watch.show("t1") / 1000.0 << "s";
  }

  plato::mem_status_t mstatus;
  plato::self_mem_usage(&mstatus);
  LOG(INFO) << "memory usage: " << (double)mstatus.vm_rss / 1024.0 << " MBytes";
  LOG(INFO) << "pagerank done const: " << watch.show("t0") / 1000.0 << "s";
}

int main(int argc, char** argv) {
  auto& cluster_info = plato::cluster_info_t::get_instance();

  init(argc, argv);
  cluster_info.initialize(&argc, &argv);

  if (FLAGS_vtype == "uint32") {
    run_jaccard<uint32_t>();
  } else if (FLAGS_vtype == "int32")  {
    run_jaccard<int32_t>();
  } else if (FLAGS_vtype == "uint64") {
    run_jaccard<uint64_t>();
  } else if (FLAGS_vtype == "int64") {
    run_jaccard<int64_t>();
  } else if (FLAGS_vtype == "string") {
    run_jaccard<std::string>();
  } else {
    LOG(FATAL) << "unknown vtype: " << FLAGS_vtype;
  }

  return 0;
}

