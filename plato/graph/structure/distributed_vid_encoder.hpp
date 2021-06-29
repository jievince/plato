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

#pragma once

#include <vector>
#include <type_traits>

#include "plato/graph/base.hpp"
#include "plato/graph/structure/vid_encoder_base.hpp"
#include "plato/graph/structure/edge_cache.hpp"
#include "plato/graph/message_passing.hpp"
#include "libcuckoo/cuckoohash_map.hh"
#include "plato/util/perf.hpp"
#include "caches/cache.hpp"
#include "caches/lru_cache_policy.hpp"


namespace plato {

template <typename VID_T>
struct vid_to_encode_msg_t {
  VID_T v_i_;
  size_t idx_;
  bool is_src_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & idx_ & is_src_;
  }
};

template <typename VID_T>
struct vid_encoded_msg_t {
  VID_T v_i_;
  vid_t encoded_v_i_;
  size_t idx_;
  bool is_src_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & encoded_v_i_ & idx_ & is_src_;
  }
};

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, size_t>::type size(VID_T) { // could it marked as inline?
  return sizeof(VID_T);
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, size_t>::type size(VID_T vid) {
  return vid.size();
}

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, const VID_T*>::type addr(const VID_T& vid) { // could it marked as inline?
  return &vid;
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, const char*>::type addr(const VID_T& vid) {
  return vid.c_str();
}

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, uint32_t>::type hash(VID_T vid) { // could it marked as inline?
  return murmur_hash2(&vid, sizeof(VID_T));
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, uint32_t>::type hash(const VID_T& vid) {
  return murmur_hash2(vid.c_str(), vid.size());
}

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, void>::type mpi_recv(VID_T& vid, int part) { // could it marked as inline?
  MPI_Recv(&vid, sizeof(VID_T), MPI_CHAR, part, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, void>::type mpi_recv(VID_T& vid, int part) {
  MPI_Status status;
  int  recv_bytes  = 0;
  char *s = nullptr;
  MPI_Probe(part, Response, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
  s = new char[recv_bytes];
  MPI_Recv(s, recv_bytes, MPI_CHAR, part, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  vid = std::string(s, recv_bytes);
}


template <typename EDATA, typename VID_T = vid_t, template<typename, typename> class CACHE = edge_block_cache_t>
class distributed_vid_encoder_t : public vid_encoder_base_t<EDATA, VID_T, CACHE>{
public:
  template <typename Key, typename Value>
  using lru_cache_t = typename caches::fixed_sized_cache<Key, Value, caches::LRUCachePolicy<Key>>;

  using encoder_callback_t = std::function<bool(edge_unit_t<EDATA, vid_t>*, size_t)>;
  /**
   * @brief
   * @param opts
   */
  distributed_vid_encoder_t()
      : encoded_cache_(HUGESIZE), decoded_cache_(HUGESIZE) {}

  ~distributed_vid_encoder_t() {
    MPI_Barrier(MPI_COMM_WORLD);
    continue_serve_ = false;
    if (decoder_server_.joinable()) {
      decoder_server_.join();
    }
  }

  /**
   * @brief encode
   * @param cache
   * @param callback
   */
  void encode(CACHE<EDATA, VID_T>& cache, encoder_callback_t callback) override;

  /**
   * @brief decode
   * @param v_i
   * @return
   */
  VID_T decode(vid_t v_i) override {
    LOG(INFO) << "try to decode v_i: " << v_i;
    CHECK(v_i < local_vid_offset_.back())
        << "v: " << v_i << " exceed max value " << local_vid_offset_.back();

    auto &cluster_info = plato::cluster_info_t::get_instance();
    auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
    auto local_vid_end = local_vid_offset_[cluster_info.partition_id_ + 1];
    LOG(INFO) << "local_vid_start: " << local_vid_start << ", local_vid_end: " << local_vid_end;

    if (v_i >= local_vid_start && v_i < local_vid_end) {
      LOG(INFO) << "[" << cluster_info.partition_id_ << "]" << "decode() locally " << "v_i " << v_i << " --> " << local_ids_[v_i-local_vid_start];
      return local_ids_[v_i-local_vid_start];
    }
    // else if (decoded_cache_.Cached(v_i)) {
    //   LOG(INFO) << "hit decoded cache";
    //   return decoded_cache_.Get(v_i);
    // }
    else {
      LOG(INFO) << "decode cache not cached!!!!, decoded cache size: " << decoded_cache_.Size();
      VID_T decoded_v_i;
      auto send_to = get_part_id(v_i);
      LOG(INFO) << "send_to: " << send_to;
      // MPI_Sendrecv(&v_i, sizeof(vid_t), MPI_CHAR, send_to, Request,
      //              &decoded_v_i, 512, MPI_CHAR, send_to, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Send(&v_i, sizeof(vid_t), MPI_CHAR, send_to, Request, MPI_COMM_WORLD);
      LOG(INFO) << "has sendted";

      mpi_recv(decoded_v_i, send_to);

      // if (std::is_integral<VID_T>::value) {
      //   MPI_Recv(&decoded_v_i, sizeof(VID_T), MPI_CHAR, send_to, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      // } else {
      //   MPI_Status status;
      //   int  recv_bytes  = 0;
      //   char *a;
      //   MPI_Probe(send_to, Response, MPI_COMM_WORLD, &status);
      //   MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
      //   MPI_Recv(a, recv_bytes, MPI_CHAR, send_to, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      //   decoded_v_i = std::string(a, recv_bytes);
      // }
      //MPI_Recv(&decoded_v_i, 512, MPI_CHAR, send_to, Response, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      LOG(INFO) << "has recved";
      LOG(INFO) << "decoded v_i: " << decoded_v_i;
      LOG(INFO) << "[" << cluster_info.partition_id_ << "]" << "decode() on " << send_to << "v_i " << v_i << " --> " << decoded_v_i;
      //decoded_cache_.Put(v_i, decoded_v_i);
      return decoded_v_i;
    }
  }

  /**
   * @brief getter
   * @return
   */
  const std::vector<VID_T>& data() override {
    return local_ids_;
  }

private:
  int get_part_id(vid_t v_i) {
    CHECK(v_i < local_vid_offset_.back())
        << "v: " << v_i << " exceed max value " << local_vid_offset_.back();
    for (int p_i = 0; p_i < local_vid_offset_.size(); ++p_i) {
      if (v_i >= local_vid_offset_[p_i] && v_i < local_vid_offset_[p_i+1]) {
        return p_i;
      }
    }
    return -1;
  }

  int launch_decoder();

private:
  std::vector<VID_T> local_ids_;
  std::vector<vid_t> local_vid_offset_;

  lru_cache_t<VID_T, vid_t> encoded_cache_;
  lru_cache_t<vid_t, VID_T> decoded_cache_;
  std::thread decoder_server_;
  bool continue_serve_{true};
};

template <typename EDATA, typename VID_T, template<typename, typename> class CACHE>
void distributed_vid_encoder_t<EDATA, VID_T, CACHE>::encode(CACHE<EDATA, VID_T>& cache,
                                                            encoder_callback_t callback) {
  using cuckoomap_t = cuckoohash_map<VID_T, vid_t, std::hash<VID_T>, std::equal_to<VID_T>,
    std::allocator<std::pair<const VID_T, vid_t> > >;
  using locked_table_t = typename cuckoomap_t::locked_table;
  using iterator_t     = typename locked_table_t::iterator;
  stop_watch_t watch;
  auto& cluster_info = cluster_info_t::get_instance();

  watch.mark("t0");
  watch.mark("t1");
  std::unique_ptr<locked_table_t> lock_table;
  vid_t local_vertex_size;


  {
    cuckoomap_t table;
    cuckoomap_t used;
    using edge_unit_spec_t = edge_unit_t<EDATA, VID_T>;
    using push_context_t = plato::template mepa_sd_context_t<VID_T>;
    spread_message<VID_T, vid_t>(
      cache,
      [&](const push_context_t& context, size_t i, edge_unit_spec_t *edge) {
        bool upserted = used.upsert(edge->src_, [](vid_t&){}, 0);
        if (upserted) {
          auto send_to = hash(edge->src_) % cluster_info.partitions_;
          context.send(send_to, edge->src_);
        }

        upserted = used.upsert(edge->dst_, [](vid_t&){}, 0);
        if (upserted) {
          auto send_to = hash(edge->dst_) % cluster_info.partitions_;
          context.send(send_to, edge->dst_);
        }
      },
      [&](VID_T& msg) {
        table.upsert(msg, [](vid_t&){}, 0);
        return 0;
      }
    );

    local_vertex_size = table.size();
    local_ids_.resize(local_vertex_size);
    // get all vertex id from local hash table
    lock_table.reset(new locked_table_t(std::move(table.lock_table())));
    iterator_t it = lock_table->begin();
    for (size_t i = 0; lock_table->end() != it; ++i, ++it) {
      local_ids_[i] = it->first;
    }

    lock_table.reset(nullptr);
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "transfer bit cost: " << watch.show("t1") / 1000.0;
  }
  watch.mark("t1");
  LOG(INFO) << "[" << cluster_info.partition_id_ << "] local vertex size: " << local_vertex_size;
  std::vector<vid_t> local_sizes(cluster_info.partitions_);
  MPI_Allgather(&local_vertex_size, 1, get_mpi_data_type<vid_t>(), &local_sizes[0], 1, get_mpi_data_type<vid_t>(), MPI_COMM_WORLD);

  vid_t global_vertex_size;
  MPI_Allreduce(&local_vertex_size, &global_vertex_size, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "total vertex size: " << global_vertex_size;
  }

  local_vid_offset_.resize(cluster_info.partitions_+1, 0);
  for (int i = 0; i <= cluster_info.partitions_; ++i) {
    if (i > 0) local_vid_offset_[i] = local_sizes[i - 1] + local_vid_offset_[i - 1];
    //LOG(INFO) << "local_vid_offset_[" << i << "]=" << local_vid_offset_[i];
  }

  watch.mark("t1");
  cuckoomap_t id_table(local_vertex_size * 1.2);
  #pragma omp parallel for num_threads(cluster_info.threads_)
  for (vid_t i = 0; i < local_vertex_size; ++i) {
    auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
    id_table.upsert(local_ids_[i], [](vid_t&){ }, i + local_vid_start);
    //LOG(INFO) << "[" << cluster_info.partition_id_ << "]" << local_ids_[i] << " --> " << i + local_vid_start;
  }

  lock_table.reset(new locked_table_t(std::move(id_table.lock_table())));
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "get all id table cost: " << watch.show("t1") / 1000.0;
  }

  watch.mark("t1");

  std::vector<edge_unit_t<EDATA, vid_t>> items(cache.size()); /// shoule be resized to size of local edges
  std::atomic<size_t> k(0);
  std::atomic<size_t> sended(0);


  watch.mark("t1");
  LOG(INFO) << "---------------------- send/recv encode req start";
  {
    using edge_unit_spec_t = edge_unit_t<EDATA, VID_T>;
    using push_context_t = plato::template mepa_sd_context_t<vid_to_encode_msg_t<VID_T>>;

    traverse_opts_t traverse_opts;
    traverse_opts.auto_release_ = true;
    cache.reset_traversal(traverse_opts);

    bsp_opts_t bsp_opts;
    // bsp_opts.global_size_    = 64 * MBYTES;
    // bsp_opts.local_capacity_ = 32 * PAGESIZE;

    auto spread_task = [&](const push_context_t& context, size_t i, edge_unit_spec_t *edge) { /// 发送编码请求--> 发送读cache的ShuffleFin
      size_t idx = k.fetch_add(1, std::memory_order_relaxed);
      items[idx].edata_ = edge->edata_;
      {
        sended.fetch_add(1);
        // if (encoded_cache_.Cached(edge->src_)) {
        //   LOG(INFO) << "hit encoded cache";
        //   items[idx].src_ = encoded_cache_.Get(edge->src_);
        //   cache_hits.fetch_add(1);
        // } else {
          //LOG(INFO) << "encode cache not cached!!!!, encoded cache size: " << encoded_cache_.Size();
        auto send_to = hash(edge->src_) % cluster_info.partitions_;
        auto to_encode_msg = vid_to_encode_msg_t<VID_T>{edge->src_, idx, true};
        context.send(send_to, to_encode_msg);
        // }
      }
      {
        sended.fetch_add(1);
        // if (encoded_cache_.Cached(edge->dst_)) {
        //   LOG(INFO) << "hit encoded cache";
        //   items[idx].dst_ = encoded_cache_.Get(edge->dst_);
        //   cache_hits.fetch_add(1);
        // } else {
        auto send_to = hash(edge->dst_) % cluster_info.partitions_;
        auto to_encode_msg = vid_to_encode_msg_t<VID_T>{edge->dst_, idx, false};
        context.send(send_to, to_encode_msg);
        // }
      }
    };

    auto send_req = [&](bsp_send_callback_t<vid_to_encode_msg_t<VID_T>> send) {
      auto send_callback = [&](int node, const vid_to_encode_msg_t<VID_T>& message) {
        send(node, message);
        //LOG(INFO) << "---------------------send a msg";
      };

      mepa_sd_context_t<vid_to_encode_msg_t<VID_T>> context { send_callback };

      size_t chunk_size = bsp_opts.local_capacity_;
      auto rebind_traversal = bind_task_detail::bind_send_task(std::move(spread_task),
          std::move(context));
      while (cache.next_chunk(rebind_traversal, &chunk_size)) { LOG(INFO) << "---------------__send: in while"; }
      LOG(INFO) << "-------------__send: after while";
    };

    auto recv_req_callback = [&](int p_i, bsp_recv_pmsg_t<vid_to_encode_msg_t<VID_T>>& pmsg) {
      // LOG(INFO) << "------------------__recv: " << "v_i_: " << pmsg->v_i_ << "encoded: " << lock_table->at(pmsg->v_i_) << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
      vid_encoded_msg_t<VID_T> encoded_msg{ pmsg->v_i_, lock_table->at(pmsg->v_i_), pmsg->idx_, pmsg->is_src_ };
      return encoded_msg;
    };

    auto recv_resp = [&](int, bsp_recv_pmsg_t<vid_encoded_msg_t<VID_T>>& pmsg) {
      if (pmsg->is_src_) {
        items[pmsg->idx_].src_ = pmsg->encoded_v_i_;
      } else {
        items[pmsg->idx_].dst_ = pmsg->encoded_v_i_;
      }
    };

    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp start";
    int rc = fine_grain_bsp2<vid_to_encode_msg_t<VID_T>, vid_encoded_msg_t<VID_T>>(send_req, recv_req_callback, recv_resp, bsp_opts);
    CHECK(0 == rc);
    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp end";
  }

  callback(&items[0], items.size());
  lock_table.reset(nullptr);

  // start decode server
  MPI_Barrier(MPI_COMM_WORLD);
  int rc = launch_decoder();
  CHECK(0 == rc);
}

template <typename EDATA, typename VID_T, template<typename, typename> class CACHE>
int distributed_vid_encoder_t<EDATA, VID_T, CACHE>::launch_decoder() {

  auto &cluster_info = plato::cluster_info_t::get_instance();

  LOG(INFO) << "starting decoder server...";
  decoder_server_ = std::thread([&]() {
    LOG(INFO) << "decoder server in thread...";
    std::vector<vid_t> recv_buff(cluster_info.partitions_);
    std::vector<MPI_Request> recv_requests_vec(cluster_info.partitions_, MPI_REQUEST_NULL);
    for (size_t i = 0; i < recv_requests_vec.size(); ++i) {
      MPI_Irecv(&recv_buff[i], sizeof(vid_t), MPI_CHAR, MPI_ANY_SOURCE, Request, MPI_COMM_WORLD, &recv_requests_vec[i]);
    }

    auto probe_once =
      [&](bool continued) {
        //LOG(INFO) << cluster_info.partition_id_ << "***continue_serve_....";
        int  flag        = 0;
        int  index       = 0;
        int  recv_bytes  = 0;
        bool has_message = false;
        MPI_Status status;

        MPI_Testany(recv_requests_vec.size(), recv_requests_vec.data(), &index, &flag, &status);
        //LOG(INFO) << "MPI_Testanyed, flag: " << flag << ", index: " << index;
        while (flag && (MPI_UNDEFINED != index)) {
            //LOG(INFO) << "***catched a new req msg";
            MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
            CHECK(recv_bytes == sizeof(vid_t)) << "recv message's size != sizeof(vid_t): " << recv_bytes;

            auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
            auto local_vid_end = local_vid_offset_[cluster_info.partition_id_ + 1];
            auto v_i = recv_buff[index];
            CHECK(v_i >= local_vid_start && v_i < local_vid_end)
                << "v: " << v_i << ", vid cannot be decoded by"
                << " partition_id: " << cluster_info.partition_id_
                << ", valid local vid range: [" << local_vid_start << "," << local_vid_end
                << ")";
            VID_T decoded_v_i = local_ids_[v_i-local_vid_start];
            MPI_Irecv(&recv_buff[index], sizeof(vid_t), MPI_CHAR, MPI_ANY_SOURCE, Request, MPI_COMM_WORLD, &recv_requests_vec[index]);
            MPI_Send(addr(decoded_v_i), size(decoded_v_i), MPI_CHAR, status.MPI_SOURCE, Response, MPI_COMM_WORLD);

            has_message=true;
            if (false == continued) { break; }
            MPI_Testany(recv_requests_vec.size(), recv_requests_vec.data(), &index, &flag, &status);
        }

        return has_message;
      };

      uint32_t idle_times = 0;
      while (continue_serve_) {
        bool busy = probe_once(true);

        idle_times += (uint32_t)(false == busy);
        if (idle_times > 10) {
          poll(nullptr, 0, 1);
          idle_times = 0;
        } else if (false == busy) {
          pthread_yield();
        }
      }
      LOG(INFO) << cluster_info.partition_id_ << "decoder server thread exited....";
  });
  LOG(INFO) <<"decoder server starting...";

  return 0;
}

} // namespace plato

