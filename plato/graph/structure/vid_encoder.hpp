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

#include "plato/util/archive.hpp"
#include "yas/types/std/vector.hpp"

#include "plato/graph/base.hpp"
#include "plato/graph/structure/vid_encoder_base.hpp"
#include "plato/graph/structure/edge_cache.hpp"
#include "plato/graph/message_passing.hpp"
#include "libcuckoo/cuckoohash_map.hh"
#include "plato/util/perf.hpp"


namespace plato {

template <typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, void>::type
mpi_allgatherv(std::vector<VID_T>& local_ids, std::vector<VID_T>& global_ids, std::vector<int>& recvcounts, std::vector<int>& displs) {
  MPI_Allgatherv(
    &local_ids[0], local_ids.size(), get_mpi_data_type<VID_T>(), &global_ids[0],
    &recvcounts[0], &displs[0], get_mpi_data_type<VID_T>(), MPI_COMM_WORLD);
}

template <typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, void>::type
mpi_allgatherv(std::vector<VID_T>& local_ids, std::vector<VID_T>& global_ids, std::vector<int>&, std::vector<int>&) {
    LOG(INFO) << "mpi_allgatherv string start";
    using msg_type = std::string;
    using oarchive_spec_t = plato::oarchive_t<msg_type, plato::mem_ostream_t>;
    using iarchive_spec_t = plato::iarchive_t<msg_type, plato::mem_istream_t>;

    auto& cluster_info = cluster_info_t::get_instance();

    std::unique_ptr<oarchive_spec_t> oarchive_p;
    std::unique_ptr<iarchive_spec_t> iarchive_p;

    oarchive_p.reset(new oarchive_spec_t);

    for (size_t i = 0; i < local_ids.size(); ++i) {
      oarchive_p->emit(local_ids[i]);
      LOG(INFO) << "local_ids[" << i << "]" << local_ids[i];
    }

    auto local_ids_buff = oarchive_p->get_intrusive_buffer();
    LOG(INFO) << "local_ids_buff:";
    for (int i = 0; i < local_ids_buff.size_; ++i) {
      LOG(INFO) << local_ids_buff.data_[i];
    }

    vid_t ids_buff_size = local_ids_buff.size_;
    LOG(INFO) << "ids_buff_size: " << ids_buff_size;

    std::vector<vid_t> local_ids_buff_sizes(cluster_info.partitions_);

    MPI_Allgather(&ids_buff_size, 1, get_mpi_data_type<vid_t>(), &local_ids_buff_sizes[0], 1, get_mpi_data_type<vid_t>(), MPI_COMM_WORLD);

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      LOG(INFO) << "local_ids_buff_sizes[" << p_i << "]" << local_ids_buff_sizes[p_i];
    }

    std::vector<int> local_ids_buff_recvcounts(cluster_info.partitions_);
    std::vector<int> local_ids_buff_displs(cluster_info.partitions_, 0);

    for (int i = 0; i < cluster_info.partitions_; ++i) {
      local_ids_buff_recvcounts[i] = local_ids_buff_sizes[i];
      if (i > 0) local_ids_buff_displs[i] = local_ids_buff_sizes[i - 1] + local_ids_buff_displs[i - 1];
    }
    for (int i = 0; i < cluster_info.partitions_; ++i) {
      LOG(INFO) << "local_ids_buff_recvcounts[" << i << "]" << local_ids_buff_recvcounts[i];
    }
    for (int i = 0; i < cluster_info.partitions_; ++i) {
      LOG(INFO) << "local_ids_buff_displs[" << i << "]" << local_ids_buff_displs[i];
    }

    MPI_Allreduce(MPI_IN_PLACE, &ids_buff_size, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
    LOG(INFO) << "total ids_buff_size: " << ids_buff_size;

    char *global_ids_buff = new char[ids_buff_size];

    MPI_Allgatherv(
      local_ids_buff.data_, local_ids_buff.size_, MPI_CHAR, &global_ids_buff,
      &local_ids_buff_recvcounts[0], &local_ids_buff_displs[0], MPI_CHAR, MPI_COMM_WORLD);
    LOG(INFO) << "global_ids_buff";
    for (int i = 0; i < ids_buff_size; ++i) {
      LOG(INFO) << i;
      LOG(INFO) << global_ids_buff[i];
    }

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      iarchive_p.reset(new iarchive_spec_t(&global_ids_buff[local_ids_buff_displs[p_i]], local_ids_buff_sizes[p_i],
            local_ids.size()));
      for (int i = 0; i < local_ids_buff_sizes[p_i]; ++i) {
        auto val = *(iarchive_p->absorb());
        global_ids[i+local_ids_buff_displs[p_i]] = val;
      }
    }
}



template <typename EDATA, typename VID_T = vid_t, template<typename, typename> class CACHE = edge_block_cache_t>
class vid_encoder_t : public vid_encoder_base_t<EDATA, VID_T, CACHE>{
public:
  using encoder_callback_t = std::function<bool(edge_unit_t<EDATA, vid_t>*, size_t)>;
  /**
   * @brief
   * @param opts
   */
  vid_encoder_t() {}

  /**
   * @brief encode
   * @param cache
   * @param callback
   */
  void encode(CACHE<EDATA, VID_T>& cache, encoder_callback_t callback) override;


  void set_vids(const std::vector<VID_T>& vids) override {
      vids_ = vids;
  }

  void get_vids(std::vector<vid_t>& encoded_vids) override {
      encoded_vids = encoded_vids_;
  }

  /**
   * @brief decode
   * @param v_i
   * @return
   */
  inline VID_T decode(vid_t v_i) override {
    CHECK(v_i < (vid_t)global_ids_.size()) << "v: " <<  v_i << " global size: " << global_ids_.size() << " vid invalid";
    return global_ids_[v_i];
  }

  /**
   * @brief getter
   * @return
   */
  const std::vector<VID_T>& data() override {
    return global_ids_;
  }

private:
  std::vector<VID_T> global_ids_;
  std::vector<VID_T> vids_;
  std::vector<vid_t> encoded_vids_;
};

template <typename EDATA, typename VID_T, template<typename, typename> class CACHE>
void vid_encoder_t<EDATA, VID_T, CACHE>::encode(CACHE<EDATA, VID_T>& cache,
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
  std::vector<VID_T> local_ids;
  vid_t vertex_size;


  {
    cuckoomap_t table;
    cuckoomap_t used;
    using edge_unit_spec_t = edge_unit_t<EDATA, VID_T>;
    using push_context_t = plato::template mepa_sd_context_t<VID_T>;
    spread_message<VID_T, vid_t>(
      cache,
      [&](const push_context_t& context, size_t i, edge_unit_spec_t *edge) {
        {
          bool upserted = used.upsert(edge->src_, [](vid_t&){}, 0);
          if (upserted) {
            auto send_to = murmur_hash2(addr(edge->src_), size(edge->src_)) % cluster_info.partitions_;
            context.send(send_to, edge->src_);
          }
        }
        {
          bool upserted = used.upsert(edge->dst_, [](vid_t&){}, 0);
          if (upserted) {
            auto send_to = murmur_hash2(addr(edge->dst_), size(edge->dst_)) % cluster_info.partitions_;
            context.send(send_to, edge->dst_);
          }
        }
      },
      [&](VID_T& msg) {
        table.upsert(msg, [](vid_t&){}, 0);
        return 0;
      }
    );

    vertex_size = table.size();
    local_ids.resize(vertex_size);
    // get all vertex id from local hash table
    lock_table.reset(new locked_table_t(std::move(table.lock_table())));
    iterator_t it = lock_table->begin();
    for (size_t i = 0; lock_table->end() != it; ++i, ++it) {
      local_ids[i] = it->first;
    }

    lock_table.reset(nullptr);
  }

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "transfer bit cost: " << watch.show("t1") / 1000.0;
  }
  watch.mark("t1");
  LOG(INFO) << "pid: " << cluster_info.partition_id_ << " local vertex size: " << vertex_size;
  std::vector<vid_t> local_sizes(cluster_info.partitions_);
  MPI_Allgather(&vertex_size, 1, get_mpi_data_type<vid_t>(), &local_sizes[0], 1, get_mpi_data_type<vid_t>(), MPI_COMM_WORLD);

  MPI_Allreduce(MPI_IN_PLACE, &vertex_size, 1, get_mpi_data_type<vid_t>(), MPI_SUM, MPI_COMM_WORLD);
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "total vertex size: " << vertex_size;
  }

  global_ids_.resize(vertex_size);
  std::vector<int> recvcounts(cluster_info.partitions_);
  std::vector<int> displs(cluster_info.partitions_, 0);
  for (int i = 0; i < cluster_info.partitions_; ++i) {
    recvcounts[i] = local_sizes[i];
    if (i > 0) displs[i] = local_sizes[i - 1] + displs[i - 1];
    //LOG(INFO) << "partition: " << i << " count: " << local_sizes[i] << " pos: " << displs[i];
  }

  mpi_allgatherv(local_ids, global_ids_, recvcounts, displs);

  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "all gather cost: " << watch.show("t1") / 1000.0;
  }

  watch.mark("t1");
  cuckoomap_t id_table(vertex_size * 1.2);
  #pragma omp parallel for num_threads(cluster_info.threads_)
  for (vid_t i = 0; i < vertex_size; ++i) {
    id_table.upsert(global_ids_[i], [](vid_t&){ }, i);
  }

  lock_table.reset(new locked_table_t(std::move(id_table.lock_table())));
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "get all id table cost: " << watch.show("t1") / 1000.0;
  }

  watch.mark("t1");

  traverse_opts_t traverse_opts;
  traverse_opts.auto_release_ = true;
  cache.reset_traversal(traverse_opts);
  #pragma omp parallel num_threads(cluster_info.threads_)
  {
    using edge_unit_spec_t = edge_unit_t<EDATA, VID_T>;
    std::vector<edge_unit_t<EDATA, vid_t>> items(HUGESIZE);
    size_t k = 0;
    auto traversal =
      [&](size_t /*idx*/, edge_unit_spec_t* edge) {
        items[k].edata_ = edge->edata_;
        //LOG(INFO) << "pid: " << cluster_info.partition_id_ << " src: "  << item.src_ << " dst: " << item.dst_;
        items[k].src_ = lock_table->at(edge->src_);
        items[k].dst_ = lock_table->at(edge->dst_);

        k++;
        if (k == HUGESIZE) {
          callback(&items[0], k);
          k = 0;
        }
        return true;
      };

    size_t chunk_size = 64;
    while (cache.next_chunk(traversal, &chunk_size)) { }

    if (k > 0) {
      callback(&items[0], k);
    }
  }

  if (!vids_.empty()) {
    encoded_vids_.resize(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      encoded_vids_[i] = lock_table->at(vids_[i]);
    }
  }
  lock_table.reset(nullptr);
  if (0 == cluster_info.partition_id_) {
    LOG(INFO) << "get encode cache cost: " << watch.show("t1") / 1000.0;
    LOG(INFO) << "encode total cost: " << watch.show("t0") / 1000.0;
  }

}

}
