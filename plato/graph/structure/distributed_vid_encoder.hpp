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

#include "plato/graph/base.hpp"
#include "plato/graph/structure/vid_encoder_base.hpp"
#include "plato/graph/structure/edge_cache.hpp"
#include "plato/graph/message_passing.hpp"
#include "libcuckoo/cuckoohash_map.hh"
#include "plato/util/perf.hpp"


namespace plato {

template <typename VID_T>
struct vid_encoder_message_t {
  VID_T v_i_;
  size_t idx_;
  bool is_src_;
  int from_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & idx_ & is_src_ & from_;
  }
};

template <>
struct vid_encoder_message_t<vid_t> {
  vid_t v_i_;
  size_t idx_;
  bool is_src_;
  int from_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & idx_ & is_src_ & from_;
  }
};

template <typename EDATA, typename VID_T = vid_t, template<typename, typename> class CACHE = edge_block_cache_t>
class distributed_vid_encoder_t : public vid_encoder_base_t<EDATA, VID_T, CACHE>{
public:
  using encoder_callback_t = std::function<bool(edge_unit_t<EDATA, vid_t>*, size_t)>;
  /**
   * @brief
   * @param opts
   */
  distributed_vid_encoder_t(const vid_encoder_opts_t& opts = vid_encoder_opts_t()): opts_(opts) {}

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
  inline VID_T decode(vid_t v_i) override {
    auto &cluster_info = plato::cluster_info_t::get_instance();
    auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
    auto local_vid_end = local_vid_offset_[cluster_info.partition_id_ + 1];
    CHECK(v_i >= local_vid_start && v_i < local_vid_end)
        << "v: " << v_i << ", vid cannot be decoded by"
        << " partition_id: " << cluster_info.partition_id_
        << ", valid vid range: [" << local_vid_start << "," << local_vid_end
        << ")";
    return local_ids_[v_i-local_vid_start];
  }

  /**
   * @brief getter
   * @return
   */
  const std::vector<VID_T>& data() override {
    return local_ids_;
  }

private:
  std::vector<VID_T> local_ids_;
  std::vector<vid_t> local_vid_offset_;
  vid_encoder_opts_t opts_;
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
        if (opts_.src_need_encode_) {
          bool upserted = used.upsert(edge->src_, [](vid_t&){}, 0);
          if (upserted) {
            auto send_to = murmur_hash2(&(edge->src_), sizeof(VID_T)) % cluster_info.partitions_;
            context.send(send_to, edge->src_);
          }
        }
        if (opts_.dst_need_encode_) {
          bool upserted = used.upsert(edge->dst_, [](vid_t&){}, 0);
          if (upserted) {
            auto send_to = murmur_hash2(&(edge->dst_), sizeof(VID_T)) % cluster_info.partitions_;
            context.send(send_to, edge->dst_);
          }
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
  LOG(INFO) << "pid: " << cluster_info.partition_id_ << " local vertex size: " << local_vertex_size;
  std::vector<vid_t> local_sizes(cluster_info.partitions_);
  MPI_Allgather(&local_vertex_size, 1, get_mpi_data_type<vid_t>(), &local_sizes[0], 1, get_mpi_data_type<vid_t>(), MPI_COMM_WORLD);

  vid_t global_vertex_size;
  MPI_Allreduce(&local_vertex_size, &global_vertex_size, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
  if (0 == cluster_info.partition_id_) {
    for (size_t i = 0; i < local_sizes.size(); ++i) {
      LOG(INFO) << "partition_" << i << ", local vertex size: " << local_sizes[i];
    }
    LOG(INFO) << "total vertex size: " << global_vertex_size;
  }

  local_vid_offset_.resize(cluster_info.partitions_+1, 0);
  for (int i = 0; i <= cluster_info.partitions_; ++i) {
    if (i > 0) local_vid_offset_[i] = local_sizes[i - 1] + local_vid_offset_[i - 1];
    LOG(INFO) << "local_vid_offset_[" << i << "] " << local_vid_offset_[i];
  }

  watch.mark("t1");
  cuckoomap_t id_table(local_vertex_size * 1.2);
  #pragma omp parallel for num_threads(cluster_info.threads_)
  for (vid_t i = 0; i < local_vertex_size; ++i) {
    id_table.upsert(local_ids_[i], [](vid_t&){ }, i + local_sizes[cluster_info.partition_id_]);
  }

  lock_table.reset(new locked_table_t(std::move(id_table.lock_table())));
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "get all id table cost: " << watch.show("t1") / 1000.0;
  }

  watch.mark("t1");
  
  using vid_to_encode_msg_t = vid_encoder_message_t<VID_T>;
  using vid_encoded_msg_t = vid_encoder_message_t<vid_t>;
  moodycamel::ConcurrentQueue<vid_encoded_msg_t>  encoded_msg_queue;
  std::atomic<bool> process_continue(true);

  std::vector<edge_unit_t<EDATA, vid_t>> items(cache.size()); /// shoule be resized to size of local edges
  std::atomic<size_t> k(0);

  LOG(INFO) << "------------------- assist_thread_start";
  std::thread assist_thread ([&] {
    // 发送编码结果, 接收编码结果
    // auto& cluster_info = cluster_info_t::get_instance();

    auto __send = [&](bsp_send_callback_t<vid_encoded_msg_t> send) { /// 发送编码结果
      vid_encoded_msg_t encoded_msg;
      LOG(INFO) << "-------------assist_thread, process_continue start: " << process_continue.load();
      while (process_continue.load()) {
        if (encoded_msg_queue.try_dequeue(encoded_msg)) {
          LOG(INFO) << "------------assist_thread, try dequeued a encded_msg" << "v_i_: " << encoded_msg.v_i_ << " idx_: " << encoded_msg.idx_ << " is_src_: " << encoded_msg.is_src_ << " from_: " << encoded_msg.from_;
          send(encoded_msg.from_, encoded_msg);
        } else {
          //LOG(INFO) << "------------assist_thread, try dequeued failed...";
        }
      }
      LOG(INFO) << "-------------assist_thread, process_continue end: " << process_continue.load();
    };

    auto __recv = [&](int, bsp_recv_pmsg_t<vid_encoded_msg_t>& pmsg) { /// 接收编码结果
      if (pmsg->is_src_) {
        LOG(INFO) << "-----------assist_thread, __recv src: " << "v_i_: " << pmsg->v_i_ << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
        items[pmsg->idx_].src_ = pmsg->v_i_;
      } else {
        LOG(INFO) << "------------assist_thread, __recv dst: " << "v_i_: " << pmsg->v_i_ << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
        items[pmsg->idx_].dst_ = pmsg->v_i_;
      }
    };

    bsp_opts_t bsp_opts;
    // bsp_opts.threads_ = 1;
    // bsp_opts.global_size_    = 64 * MBYTES;
    // bsp_opts.local_capacity_ = 32 * PAGESIZE;

    int rc = fine_grain_bsp<vid_encoded_msg_t>(__send, __recv, bsp_opts);
    CHECK(0 == rc);
  });

  LOG(INFO) << "---------------------- spread_task start";
  {
    using edge_unit_spec_t = edge_unit_t<EDATA, VID_T>;
    using push_context_t = plato::template mepa_sd_context_t<vid_to_encode_msg_t>;

    traverse_opts_t traverse_opts;
    traverse_opts.auto_release_ = true;
    cache.reset_traversal(traverse_opts);

    bsp_opts_t bsp_opts;
    // bsp_opts.global_size_    = 64 * MBYTES;
    // bsp_opts.local_capacity_ = 32 * PAGESIZE;
    
    auto spread_task = [&](const push_context_t& context, size_t i, edge_unit_spec_t *edge) { /// 发送编码请求--> 发送读cache的ShuffleFin
        size_t idx = k.fetch_add(1, std::memory_order_relaxed);
        items[idx].edata_ = edge->edata_;
        if (opts_.src_need_encode_) {
          auto send_to = murmur_hash2(&(edge->src_), sizeof(edge->src_)) %
                         cluster_info.partitions_;
          auto to_encode_msg = vid_to_encode_msg_t{edge->src_, idx, true, cluster_info.partition_id_};
          context.send(send_to, to_encode_msg);
        } else {
          items[idx].src_ = edge->src_;
        }
        if (opts_.dst_need_encode_) {
          auto send_to = murmur_hash2(&(edge->dst_), sizeof(edge->dst_)) %
                         cluster_info.partitions_;
          auto to_encode_msg = vid_to_encode_msg_t{edge->dst_, idx, false, cluster_info.partition_id_};
          context.send(send_to, to_encode_msg);
        } else {
          items[idx].dst_ = edge->dst_;
        }
    };

    auto __send = [&](bsp_send_callback_t<vid_to_encode_msg_t> send) {
      auto send_callback = [&](int node, const vid_to_encode_msg_t& message) {
        send(node, message);
        LOG(INFO) << "---------------------send a msg";
      };

      mepa_sd_context_t<vid_to_encode_msg_t> context { send_callback };

      size_t chunk_size = bsp_opts.local_capacity_;
      auto rebind_traversal = bind_task_detail::bind_send_task(std::move(spread_task),
          std::move(context));
      LOG(INFO) << "--------------------__send: before while";
      while (cache.next_chunk(rebind_traversal, &chunk_size)) { LOG(INFO) << "---------------__send: in while"; }
      LOG(INFO) << "--------------------__send: after while";
    };

    auto __recv = [&](int p_i, bsp_recv_pmsg_t<vid_to_encode_msg_t>& pmsg) {
        LOG(INFO) << "------------------__recv: " << "v_i_: " << pmsg->v_i_ << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_; 
        encoded_msg_queue.enqueue(vid_encoded_msg_t { lock_table->at(pmsg->v_i_), pmsg->idx_, pmsg->is_src_, pmsg->from_ });
    };

    auto __after_recv_task = [&](void) {
      process_continue.store(false);
      LOG(INFO) << "__after_recv_task executed, process_continue: " << process_continue.load();
    };

    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp start";
    int rc = fine_grain_bsp<vid_to_encode_msg_t>(__send, __recv, bsp_opts, bsp_detail::dummy_func, __after_recv_task);
    CHECK(0 == rc);
    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp end";
  }

  LOG(INFO) << "----------------assist_thread before join";

  assist_thread.join();
  LOG(INFO) << "-----------------assist_thread joined";
  callback(&items[0], items.size());
  lock_table.reset(nullptr);
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "get encode cache cost: " << watch.show("t1") / 1000.0;
    LOG(INFO) << "encode total cost: " << watch.show("t0") / 1000.0;
  }

}

}
