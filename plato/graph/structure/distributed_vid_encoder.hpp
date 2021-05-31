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
#include "caches/cache.hpp"
#include "caches/lru_cache_policy.hpp"


namespace plato {

template <typename VID_T>
struct vid_to_encode_msg_t {
  VID_T v_i_;
  size_t idx_;
  bool is_src_;
  int from_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & idx_ & is_src_ & from_;
  }

  static inline int size() {
    return sizeof(VID_T) + sizeof(size_t) + sizeof(bool) + sizeof(int);
  }
};

template <typename VID_T>
struct vid_encoded_msg_t {
  VID_T v_i_;
  vid_t encoded_v_i_;
  size_t idx_;
  bool is_src_;
  int from_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar & v_i_ & encoded_v_i_ & idx_ & is_src_ & from_;
  }

  static inline int size() {
    return sizeof(VID_T) + sizeof(vid_t) + sizeof(size_t) + sizeof(bool) + sizeof(int);
  }
};

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
  distributed_vid_encoder_t(const vid_encoder_opts_t& opts = vid_encoder_opts_t()): opts_(opts), encoded_cache_(HUGESIZE), decoded_cache_(HUGESIZE) {
    auto &cluster_info = plato::cluster_info_t::get_instance();
    
    if (!server_started_) {
      LOG(INFO) << "******start server";
      server_started_ = true;
      serve_thread_ = std::thread([&]() {
        LOG(INFO) << "******server starting";
        std::vector<vid_t> recv_buff(cluster_info.partitions_);
        std::vector<MPI_Request> recv_requests_vec(cluster_info.partitions_, MPI_REQUEST_NULL);
        for (size_t i = 0; i < recv_requests_vec.size(); ++i) {
          MPI_Irecv(&recv_buff[i], sizeof(vid_t), MPI_CHAR, i, Request, MPI_COMM_WORLD, &recv_requests_vec[i]);
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
                auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
                auto local_vid_end = local_vid_offset_[cluster_info.partition_id_ + 1];
                //LOG(INFO) << "***catched a new req msg";
                MPI_Get_count(&status, MPI_CHAR, &recv_bytes);

                CHECK(recv_bytes == sizeof(vid_t)) << "recv message's size != sizeof(vid_t): " << recv_bytes;
                auto v_i = recv_buff[index];
                CHECK(v_i >= local_vid_start && v_i < local_vid_end)
                    << "v: " << v_i << ", vid cannot be decoded by"
                    << " partition_id: " << cluster_info.partition_id_
                    << ", valid vid range: [" << local_vid_start << "," << local_vid_end
                    << ")";
                VID_T decoded_v_i = local_ids_[v_i-local_vid_start];
                MPI_Send(&decoded_v_i, sizeof(VID_T), MPI_CHAR, index, Response, MPI_COMM_WORLD);
                MPI_Irecv(&recv_buff[index], sizeof(vid_t), MPI_CHAR, index, Request, MPI_COMM_WORLD, &recv_requests_vec[index]);
                MPI_Testany(recv_requests_vec.size(), recv_requests_vec.data(), &index, &flag, &status);
            }

            return has_message;
          };

          uint32_t idle_times = 0;
          while (continue_serve_) {
            bool busy = probe_once(false);

            idle_times += (uint32_t)(false == busy);
            if (idle_times > 10) {
              poll(nullptr, 0, 1);
              idle_times = 0;
            } else if (false == busy) {
              pthread_yield();
            }
          }
          LOG(INFO) << cluster_info.partition_id_ << " serve thread exited..........................................................";
      });
      LOG(INFO) << "*** server started";
    }
  }

  ~distributed_vid_encoder_t() {
    MPI_Barrier(MPI_COMM_WORLD);
    continue_serve_ = false;
    serve_thread_.join();
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
    CHECK(v_i < local_vid_offset_.back())
        << "v: " << v_i << " exceed max value " << local_vid_offset_.back();

    auto &cluster_info = plato::cluster_info_t::get_instance();
    auto local_vid_start = local_vid_offset_[cluster_info.partition_id_];
    auto local_vid_end = local_vid_offset_[cluster_info.partition_id_ + 1];

    if (v_i >= local_vid_start && v_i < local_vid_end) {
      //LOG(INFO) << "[" << cluster_info.partition_id_ << "]" << "decode() locally " << "v_i " << v_i << " --> " << local_ids_[v_i-local_vid_start];
      return local_ids_[v_i-local_vid_start];
    }
    // else if (decoded_cache_.Cached(v_i)) {
    //   LOG(INFO) << "hit decoded cache";
    //   return decoded_cache_.Get(v_i);
    // }
    else {
      //LOG(INFO) << "decode cache not cached!!!!, decoded cache size: " << decoded_cache_.Size();
      VID_T decoded_v_i;
      MPI_Status status;
      auto send_to = get_part_id(v_i);
      MPI_Send(&v_i, sizeof(vid_t), MPI_CHAR, send_to, Request, MPI_COMM_WORLD);
      MPI_Recv(&decoded_v_i, 512, MPI_CHAR, send_to, Response, MPI_COMM_WORLD, &status);
      //LOG(INFO) << "[" << cluster_info.partition_id_ << "]" << "decode() on " << send_to << "v_i " << v_i << " --> " << decoded_v_i;
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

private:
  std::vector<VID_T> local_ids_;
  std::vector<vid_t> local_vid_offset_;
  vid_encoder_opts_t opts_;

  lru_cache_t<VID_T, vid_t> encoded_cache_;
  lru_cache_t<vid_t, VID_T> decoded_cache_;
  std::thread serve_thread_;
  bool server_started_{false};
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
  LOG(INFO) << "[" << cluster_info.partition_id_ << "] local vertex size: " << local_vertex_size;
  std::vector<vid_t> local_sizes(cluster_info.partitions_);
  MPI_Allgather(&local_vertex_size, 1, get_mpi_data_type<vid_t>(), &local_sizes[0], 1, get_mpi_data_type<vid_t>(), MPI_COMM_WORLD);

  vid_t global_vertex_size;
  MPI_Allreduce(&local_vertex_size, &global_vertex_size, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
  if (0 == cluster_info.partition_id_) {
    for (size_t i = 0; i < local_sizes.size(); ++i) {
      LOG(INFO) << "[" << i << "] local vertex size: " << local_sizes[i];
    }
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
  
  std::vector<moodycamel::ConcurrentQueue<std::vector<vid_encoded_msg_t<VID_T>>>>  encoded_msg_vec_queues(cluster_info.partitions_);
  std::atomic<bool> process_continue(true);

  std::vector<edge_unit_t<EDATA, vid_t>> items(cache.size()); /// shoule be resized to size of local edges
  std::atomic<size_t> k(0);

  // LOG(INFO) << "------------------- assist_thread_start";
  // std::thread assist_thread ([&] {
  //   std::atomic<int>         finished_count(0);
  //   std::vector<vid_encoded_msg_t<VID_T>> recv_buff(cluster_info.partitions_);
  //   std::vector<MPI_Request> recv_requests_vec(cluster_info.partitions_,
  //                                              MPI_REQUEST_NULL);
  //   for (size_t i = 0; i < recv_requests_vec.size(); ++i) {
  //     MPI_Irecv(&recv_buff[i], vid_encoded_msg_t<VID_T>::size(), MPI_CHAR, i,
  //               Response, MPI_COMM_WORLD, &recv_requests_vec[i]);
  //   }

  //   auto probe_once =
  //     [&](bool continued) {
  //       //LOG(INFO) << cluster_info.partition_id_ << "***continue_serve_....";
  //       int  flag        = 0;
  //       int  index       = 0;
  //       int  recv_bytes  = 0;
  //       bool has_message = false;
  //       MPI_Status status;

  //       MPI_Testany(recv_requests_vec.size(), recv_requests_vec.data(), &index, &flag, &status);
  //       //LOG(INFO) << "MPI_Testanyed, flag: " << flag << ", index: " << index;
  //       while (flag && (MPI_UNDEFINED != index)) {
  //         // LOG(INFO) << "assist_thread: catched a new res msg";
  //         MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
  //         if (0 == recv_bytes) {
  //           //LOG(INFO) << "assist_thread: cached a Response fin msg";
  //           ++finished_count;
  //           recv_requests_vec[index] = MPI_REQUEST_NULL;
  //         } else {
  //           CHECK(recv_bytes == vid_encoded_msg_t<VID_T>::size()) << "recv message's size != vid_encoded_msg_t<VID_T>::size(): " << recv_bytes;
  //           auto encoded_msg = recv_buff[index];
  //           if (encoded_msg.is_src_) {
  //             items[encoded_msg.idx_].src_ = encoded_msg.encoded_v_i_;
  //           } else {
  //             items[encoded_msg.idx_].dst_ = encoded_msg.encoded_v_i_;
  //           }
  //           MPI_Irecv(&recv_buff[index], vid_encoded_msg_t<VID_T>::size(), MPI_CHAR, index,
  //                     Response, MPI_COMM_WORLD, &recv_requests_vec[index]);
  //         }
  //         has_message = true;
  //         MPI_Testany(recv_requests_vec.size(), recv_requests_vec.data(), &index, &flag, &status);
  //       }

  //       return has_message;
  //     };

  //   uint32_t idle_times = 0;
  //   while (finished_count < cluster_info.partitions_) {
  //     bool busy = probe_once(false);

  //     idle_times += (uint32_t)(false == busy);
  //     if (idle_times > 10) {
  //       poll(nullptr, 0, 1);
  //       idle_times = 0;
  //     } else if (false == busy) {
  //       pthread_yield();
  //     }
  //   }
  //   probe_once(false);

  //   for (size_t r_i = 0; r_i < recv_requests_vec.size(); ++r_i) {
  //     if (MPI_REQUEST_NULL != recv_requests_vec[r_i]) {
  //       MPI_Cancel(&recv_requests_vec[r_i]);
  //       MPI_Wait(&recv_requests_vec[r_i], MPI_STATUS_IGNORE); /// 同步wait
  //     }
  //   }
  //   LOG(INFO) << cluster_info.partition_id_ << " assist thread exited..........................................................";
  // });
  // std::thread assist_thread ([&] {
  //   // 发送编码结果, 接收编码结果
  //   // auto& cluster_info = cluster_info_t::get_instance();

  //   auto __send = [&](bsp_send_callback_t<vid_encoded_msg_t<VID_T>> send) { /// 发送编码结果
  //     vid_encoded_msg_t<VID_T> encoded_msg;
  //     LOG(INFO) << "-------------assist_thread, process_continue start: " << process_continue.load();
  //     while (process_continue.load()) {
  //       if (encoded_msg_queue.try_dequeue(encoded_msg)) {
  //         LOG(INFO) << "------------assist_thread, try dequeued a encded_msg" << "v_i_: " << encoded_msg.v_i_ << " idx_: " << encoded_msg.idx_ << " is_src_: " << encoded_msg.is_src_ << " from_: " << encoded_msg.from_;
  //         send(encoded_msg.from_, encoded_msg);
  //       } else {
  //         //LOG(INFO) << "------------assist_thread, try dequeued failed...";
  //       }
  //     }
  //     LOG(INFO) << "-------------assist_thread, process_continue end: " << process_continue.load();
  //   };

  //   auto __recv = [&](int, bsp_recv_pmsg_t<vid_encoded_msg_t<VID_T>>& pmsg) { /// 接收编码结果
  //     if (pmsg->is_src_) {
  //       LOG(INFO) << "-----------assist_thread, __recv src: " << "v_i_: " << pmsg->v_i_ << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
  //       items[pmsg->idx_].src_ = pmsg->v_i_;
  //     } else {
  //       LOG(INFO) << "------------assist_thread, __recv dst: " << "v_i_: " << pmsg->v_i_ << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
  //       items[pmsg->idx_].dst_ = pmsg->v_i_;
  //     }
  //   };

  //   bsp_opts_t bsp_opts;
  //   // bsp_opts.threads_ = 1;
  //   // bsp_opts.global_size_    = 64 * MBYTES;
  //   // bsp_opts.local_capacity_ = 32 * PAGESIZE;

  //   int rc = fine_grain_bsp<vid_encoded_msg_t<VID_T>>(__send, __recv, bsp_opts);
  //   CHECK(0 == rc);
  // });

  std::thread assist_thread([&]() {
    std::atomic<size_t> cur(0);

    auto __send = [&](bsp_send_callback_t<vid_encoded_msg_t<VID_T>> send) { /// 发送编码结果
      std::vector<vid_encoded_msg_t<VID_T>> encoded_msg_vec;
      int p_i = cur.fetch_add(1, std::memory_order_relaxed);
      p_i %= cluster_info.partitions_;
      CHECK(p_i >= 0 && p_i < encoded_msg_vec_queues.size());
      while (process_continue.load()) {
        if (encoded_msg_vec_queues[p_i].try_dequeue(encoded_msg_vec)) {
          for (auto& encoded_msg : encoded_msg_vec) {
            send(p_i, encoded_msg);
          }
        }
      }
    };

    auto __recv = [&](int, bsp_recv_pmsg_t<vid_encoded_msg_t<VID_T>>& pmsg) { /// 接收编码结果
      if (pmsg->is_src_) {
        items[pmsg->idx_].src_ = pmsg->encoded_v_i_;
      } else {
        items[pmsg->idx_].dst_ = pmsg->encoded_v_i_;
      }
      // encoded_cache_.Put(pmsg->v_i_, pmsg->encoded_v_i_);
    };

    bsp_opts_t bsp_opts;
    // bsp_opts.threads_ = 1;
    // bsp_opts.global_size_    = 64 * MBYTES;
    // bsp_opts.local_capacity_ = 32 * PAGESIZE;
    bsp_opts.tag_ = Response;

    int rc = fine_grain_bsp<vid_encoded_msg_t<VID_T>>(__send, __recv, bsp_opts);
    CHECK(0 == rc);
  });

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

    thread_local std::vector<std::vector<vid_encoded_msg_t<VID_T>>> *encoded_msg_vecs;
    
    auto spread_task = [&](const push_context_t& context, size_t i, edge_unit_spec_t *edge) { /// 发送编码请求--> 发送读cache的ShuffleFin
        size_t idx = k.fetch_add(1, std::memory_order_relaxed);
        items[idx].edata_ = edge->edata_;
        if (opts_.src_need_encode_) {
          // if (encoded_cache_.Cached(edge->src_)) {
          //   LOG(INFO) << "hit encoded cache";
          //   items[idx].src_ = encoded_cache_.Get(edge->src_);
          // } else {
            //LOG(INFO) << "encode cache not cached!!!!, encoded cache size: " << encoded_cache_.Size();
            auto send_to = murmur_hash2(&(edge->src_), sizeof(edge->src_)) %
                          cluster_info.partitions_;
            auto to_encode_msg = vid_to_encode_msg_t<VID_T>{edge->src_, idx, true, cluster_info.partition_id_};
            context.send(send_to, to_encode_msg);
          // }
        } else {
          items[idx].src_ = edge->src_;
        }
        if (opts_.dst_need_encode_) {
          // if (encoded_cache_.Cached(edge->dst_)) {
          //   LOG(INFO) << "hit encoded cache";
          //   items[idx].dst_ = encoded_cache_.Get(edge->dst_);
          // } else {
            auto send_to = murmur_hash2(&(edge->dst_), sizeof(edge->dst_)) %
                          cluster_info.partitions_;
            auto to_encode_msg = vid_to_encode_msg_t<VID_T>{edge->dst_, idx, false, cluster_info.partition_id_};
            context.send(send_to, to_encode_msg);
          // }
        } else {
          items[idx].dst_ = edge->dst_;
        }
    };

    auto __send = [&](bsp_send_callback_t<vid_to_encode_msg_t<VID_T>> send) {
      auto send_callback = [&](int node, const vid_to_encode_msg_t<VID_T>& message) {
        send(node, message);
        // LOG(INFO) << "---------------------send a msg";
      };

      mepa_sd_context_t<vid_to_encode_msg_t<VID_T>> context { send_callback };

      size_t chunk_size = bsp_opts.local_capacity_;
      auto rebind_traversal = bind_task_detail::bind_send_task(std::move(spread_task),
          std::move(context));
      while (cache.next_chunk(rebind_traversal, &chunk_size)) { LOG(INFO) << "---------------__send: in while"; }
    };

    auto __recv = [&](int p_i, bsp_recv_pmsg_t<vid_to_encode_msg_t<VID_T>>& pmsg) {
        // LOG(INFO) << "------------------__recv: " << "v_i_: " << pmsg->v_i_ << "encoded: " << lock_table->at(pmsg->v_i_) << " idx_: " << pmsg->idx_ << " is_src_: " << pmsg->is_src_ << " from_: " << pmsg->from_;
        vid_encoded_msg_t<VID_T> encoded_msg{ pmsg->v_i_, lock_table->at(pmsg->v_i_), pmsg->idx_, pmsg->is_src_, pmsg->from_ };
        // encoded_msg_queue.enqueue(encoded_msg);
        // MPI_Send(&encoded_msg, vid_encoded_msg_t<VID_T>::size(), MPI_CHAR, p_i, Response, MPI_COMM_WORLD);
        auto &vec = (*encoded_msg_vecs)[p_i];
        vec.emplace_back(std::move(encoded_msg));
        if (vec.size() >= bsp_opts.local_capacity_) {
          encoded_msg_vec_queues[p_i].enqueue(std::vector<vid_encoded_msg_t<VID_T>>(vec));
          vec.clear();
        }
    };

    auto __before_recv_task = [&]() {
      encoded_msg_vecs = new std::vector<std::vector<vid_encoded_msg_t<VID_T>>>(cluster_info.partitions_);
      for (auto& vec : *encoded_msg_vecs) {
        vec.reserve(bsp_opts.local_capacity_);
      }
    };

    auto __after_recv_task = [&]() {
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto &vec = (*encoded_msg_vecs)[p_i];
        if (!vec.empty()) {
          encoded_msg_vec_queues[p_i].enqueue(std::vector<vid_encoded_msg_t<VID_T>>(vec));
        }
      }
    };

    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp start";
    int rc = fine_grain_bsp<vid_to_encode_msg_t<VID_T>>(__send, __recv, bsp_opts, __before_recv_task, __after_recv_task);
    CHECK(0 == rc);
    LOG(INFO) << "--------------- spread_task -> fine_grain_bsp end";
    process_continue.store(false);
    // for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
    //   LOG(INFO) << "before send Response Fin";
    //   MPI_Send(nullptr, 0, MPI_CHAR, p_i, Response, MPI_COMM_WORLD);
    //   LOG(INFO) << "after send Response Fin";
    // }
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "send/recv encode req cache cost: " << watch.show("t1") / 1000.0;
  }

  watch.mark("t1");
  LOG(INFO) << "---------------------- send/recv encode result start";
  assist_thread.join();

  // {
  //   std::atomic<size_t> cur(0);

  //   auto __send = [&](bsp_send_callback_t<vid_encoded_msg_t<VID_T>> send) { /// 发送编码结果
  //     std::vector<vid_encoded_msg_t<VID_T>> encoded_msg_vec;
  //     int p_i = cur.fetch_add(1, std::memory_order_relaxed);
  //     p_i %= cluster_info.partitions_;
  //     CHECK(p_i >= 0 && p_i < encoded_msg_vec_queues.size());
  //     while (encoded_msg_vec_queues[p_i].try_dequeue(encoded_msg_vec)) {
  //       for (auto& encoded_msg : encoded_msg_vec) {
  //         send(p_i, encoded_msg);
  //       }
  //     }
  //   };

  //   auto __recv = [&](int, bsp_recv_pmsg_t<vid_encoded_msg_t<VID_T>>& pmsg) { /// 接收编码结果
  //     if (pmsg->is_src_) {
  //       items[pmsg->idx_].src_ = pmsg->encoded_v_i_;
  //     } else {
  //       items[pmsg->idx_].dst_ = pmsg->encoded_v_i_;
  //     }
  //     // encoded_cache_.Put(pmsg->v_i_, pmsg->encoded_v_i_);
  //   };

  //   bsp_opts_t bsp_opts;
  //   // bsp_opts.threads_ = 1;
  //   // bsp_opts.global_size_    = 64 * MBYTES;
  //   // bsp_opts.local_capacity_ = 32 * PAGESIZE;

  //   int rc = fine_grain_bsp<vid_encoded_msg_t<VID_T>>(__send, __recv, bsp_opts);
  //   CHECK(0 == rc);
  // }
  //assist_thread.join();
  LOG(INFO) << "[" << cluster_info.partition_id_ << "]: send/recv encode result cache cost: " << watch.show("t1") / 1000.0;

  // LOG(INFO) << "-----------------assist_thread joined";
  // LOG(INFO) << "----after encoded, items:";
  // for (auto &item : items) {
  //   LOG(INFO) << item.src_ << "," << item.dst_;
  // }
  callback(&items[0], items.size());
  lock_table.reset(nullptr);
  LOG(INFO) << "[" << cluster_info.partition_id_ << "]: encode total cost: " << watch.show("t0") / 1000.0;

}

}
