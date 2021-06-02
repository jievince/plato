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

#ifndef __PLATO_PARALLEL_BSP_HPP__
#define __PLATO_PARALLEL_BSP_HPP__

#include <poll.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>

#include <ctime>
#include <cstdint>
#include <cstdlib>

#include <list>
#include <mutex>
#include <atomic>
#include <chrono>
#include <string>
#include <memory>
#include <utility>
#include <algorithm>
#include <functional>
#include <condition_variable>

#include "omp.h"
#include "mpi.h"
#include "glog/logging.h"

#include "plato/graph/base.hpp"
#include "plato/util/atomic.hpp"
#include "plato/util/stream.hpp"
#include "plato/util/archive.hpp"
#include "plato/util/spinlock.hpp"
#include "plato/util/concurrentqueue.h"
#include "plato/parallel/mpi.hpp"

namespace plato {

// ******************************************************************************* //

struct bsp_opts_t {
  int      threads_               = -1;           // -1 means all available threads
  int      flying_send_per_node_  = 3; /// 同时最多向某个node发flying_send_per_node_个请求
  int      flying_recv_           = -1;
  uint32_t global_size_           = 16 * MBYTES;  // at most global_size_ bytes will be cached
                                                  // per one request

  uint32_t local_capacity_        = 4 * PAGESIZE; // message count
  uint32_t batch_size_            = 1;            // batch process #batch_size_ messages
  int      tag_                   = Shuffle; // MPI tag
  MPI_Comm comm_                  = MPI_COMM_WORLD;
};

template <typename MSG>
using bsp_send_callback_t = std::function<bool(int, const MSG&)>;

template <typename MSG>
using bsp_send_task_t = std::function<void(bsp_send_callback_t<MSG>)>;

// std::unique_ptr<MSG>
template <typename MSG>
using bsp_recv_pmsg_t = typename iarchive_t<MSG, mem_istream_t>::pmsg_t;

// partition-id, send-callback => void
template <typename MSG>
using bsp_recv_task_t = std::function<void(int, bsp_recv_pmsg_t<MSG>&)>;

namespace bsp_detail {

struct chunk_tail_t {
  uint32_t count_;
  uint32_t size_;
} __attribute__((packed));

/**
 * @brief
 * @tparam OARCHIVE_T
 * @param poarchive
 * @return
 */
template <typename OARCHIVE_T>
int append_chunk_tail(OARCHIVE_T* poarchive) {
  chunk_tail_t tail {
    (uint32_t)poarchive->count(),
    (uint32_t)poarchive->size() + (uint32_t)sizeof(chunk_tail_t)
  };
  poarchive->get_stream()->write(&tail, sizeof(tail));
  return 0;
}

struct chunk_desc_t {
  char*    data_;
  uint32_t size_;
  uint32_t count_;
  int      index_;
  int      from_;
};

/**
 * @brief void
 */
void dummy_func(void) { }

}

using bsp_buffer_t = std::vector<std::pair<std::shared_ptr<char>, size_t>>;

class bsp_chunk_vistor_t {
public:

  /*
   * process a chunk of received bsp message, generally used in send_task within bsp process
   *
   * \tparam MSG     message type
   * \tparam FUNC    callback message process function, should implement
   *                 <tt>void operator()(int, const MSG&)<\tt>
   *
   * \param  process message process callback function
   *
   * \return
   *    true  -- process at lease one edge
   *    false -- no more message to process
   **/
  template <typename MSG, typename FUNC>
  bool next_chunk(FUNC&& recv_task) {
    using iarchive_spec_t = iarchive_t<MSG, mem_istream_t>;
    using namespace bsp_detail;

    char*   precv_buff  = nullptr;
    size_t* precv_bytes = nullptr;

    chunk_tail_t* tail = nullptr;
    char*         data = nullptr;

    {
      std::lock_guard<decltype(lck4buff_)> lock(lck4buff_);

      do {
        if (buff_idx_ >= recv_buff_.size()) { return false; }

        precv_buff  = recv_buff_[buff_idx_].first.get();
        precv_bytes = &recv_buff_[buff_idx_].second;

        if ((0 == *precv_bytes) && (++buff_idx_ >= recv_buff_.size())) { return false; }  // can not move forward
      } while (0 == *precv_bytes);

      tail = (chunk_tail_t*)(&precv_buff[*precv_bytes - sizeof(chunk_tail_t)]);
      CHECK(tail->size_ >= sizeof(chunk_tail_t)) << "tail->size_: " << tail->size_
        << ", recv_bytes: " << *precv_bytes;

      data = &precv_buff[*precv_bytes - tail->size_];
      *precv_bytes -= tail->size_;
    }

    iarchive_spec_t iarchive(data, tail->size_ - (uint32_t)sizeof(chunk_tail_t), tail->count_);
    for (auto msg = iarchive.absorb(); nullptr != msg; msg = iarchive.absorb()) {
      recv_task(buff_idx_, msg);
    }

    return true;
  }

  /**
   * @brief
   * @param recv_buff
   */
  explicit bsp_chunk_vistor_t(bsp_buffer_t& recv_buff)
    : buff_idx_(0), recv_buff_(recv_buff) { }

  /**
   * @brief no copy constructor
   */
  bsp_chunk_vistor_t(const bsp_chunk_vistor_t&) = delete;

  /**
   * @brief no copy constructor
   * @return
   */
  bsp_chunk_vistor_t& operator=(const bsp_chunk_vistor_t&) = delete;

protected:
  spinlock_t     lck4buff_;
  size_t         buff_idx_;
  bsp_buffer_t&  recv_buff_;
};

/**
 * @brief original bulk synchronous parallel computation model
 * All received message will be cached
 * @tparam MSG
 * @param precv_buff
 * @param send_task
 * @param opts
 * @return
 */
template <typename MSG>
int bsp (
    bsp_buffer_t* precv_buff, /// no recv_task, all received msg will be cached to precv_buff, and will be used in bsp() caller.
    bsp_send_task_t<MSG> send_task,
    bsp_opts_t opts = bsp_opts_t()) {

  using namespace bsp_detail;
  using oarchive_spec_t = oarchive_t<MSG, mem_ostream_t>;

  auto& cluster_info = cluster_info_t::get_instance();
  if (opts.threads_ <= 0) { opts.threads_ = cluster_info.threads_; }
  if (opts.flying_recv_ <= 0) { opts.flying_recv_ = cluster_info.partitions_; }

  const size_t cache_size = ((size_t)sysconf(_SC_PAGESIZE) * (size_t)sysconf(_SC_PHYS_PAGES) / 2 + 1)
    * PAGESIZE / PAGESIZE;
  auto& recv_buff = *precv_buff;

  if (0 == recv_buff.size()) {
    recv_buff.resize(cluster_info.partitions_);
    for (int i = 0; i < cluster_info.partitions_; ++i) {
      char* buff = (char*)mmap(nullptr, cache_size, PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
      CHECK(nullptr != buff) << "unable to mmap size: " << cache_size << ", with errno: " << errno;
      madvise(buff, cache_size, MADV_SEQUENTIAL | MADV_HUGEPAGE);
      recv_buff[i].first.reset(buff, [cache_size](char* p) { munmap(p, cache_size); });
      recv_buff[i].second = 0;
    }
  } else {
    for (auto& buf: recv_buff) {
      buf.second = 0;
    }
  }

  std::thread recv_thread ([&](void) {
    const size_t max_recv_size = (2UL * 1024UL * (size_t)MBYTES - 1);

    std::atomic<int>         finished_count(0);
    std::vector<MPI_Request> requests_vec(cluster_info.partitions_, MPI_REQUEST_NULL);

    for (size_t i = 0; i < requests_vec.size(); ++i) {
      int recv_bytes = (int)std::min(max_recv_size, cache_size - recv_buff[i].second); /// cache是缓存区最大容量, max_recv_size是一次最多接收的消息大小.
      MPI_Irecv(recv_buff[i].first.get(), recv_bytes, MPI_CHAR, i, MPI_ANY_TAG, opts.comm_, &requests_vec[i]);
    }

    // to avoid gcc compiler bug: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86969
    auto probe_once = [&requests_vec, &recv_buff, &finished_count, &opts, max_recv_size, cache_size](void) {
      int  flag        = 0;
      int  index       = 0;
      int  recv_bytes  = 0;
      bool has_message = false;
      MPI_Status status;

      MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
      while (flag && (MPI_UNDEFINED != index)) {
        if (ShuffleFin == status.MPI_TAG) { /// ShuffleFin: 消息传输结束标志
          ++finished_count;
          requests_vec[index] = MPI_REQUEST_NULL;
        } else {  // append to cache
          MPI_Get_count(&status, MPI_CHAR, &recv_bytes);

          CHECK(recv_bytes >= static_cast<int>(sizeof(chunk_tail_t))) << "recv message too small: " << recv_bytes;
          recv_buff[index].second += (size_t)recv_bytes;

          recv_bytes = (int)std::min(max_recv_size, cache_size - recv_buff[index].second);
          CHECK(recv_bytes > 0) << "recv_buff is full with cache_size: " << cache_size << "bytes";
          MPI_Irecv(&(recv_buff[index].first.get()[recv_buff[index].second]), recv_bytes, MPI_CHAR, /// 持续发请求,知道读完所有数据, 直到ShuffleFin
              index, MPI_ANY_TAG, opts.comm_, &requests_vec[index]);
        }
        MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
      }

      return has_message;
    };

    uint32_t idle_times = 0;
    while (finished_count < cluster_info.partitions_) {
      bool busy = probe_once();
      idle_times += (uint32_t)(false == busy);

      if (idle_times > 1024) {
        poll(nullptr, 0, 1); /// sleep 1ms
        idle_times = 0;
      } else if (false == busy) {
        /// pthread_yield() causes the calling thread to relinquish the CPU. The thread is placed at the end of the run queue for its static priority and another thread is scheduled to run
        pthread_yield();
      }
    }
    probe_once();

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      if (MPI_REQUEST_NULL != requests_vec[r_i]) {
        MPI_Cancel(&requests_vec[r_i]);
        MPI_Wait(&requests_vec[r_i], MPI_STATUS_IGNORE); /// 同步wait
      }
    }
  });

  std::vector<spinlock_t> buflck_vec(cluster_info.partitions_);
  std::vector<std::list<mem_ostream_t>> global_sndbuf_vec(cluster_info.partitions_);

  std::vector<spinlock_t> reqlck_vec(cluster_info.partitions_);
  std::vector<std::list<std::pair<MPI_Request, mem_ostream_t>>> flying_requests_vec(cluster_info.partitions_);

  for (size_t p_i = 0; p_i < global_sndbuf_vec.size(); ++p_i) {
    for (int r_i = 0; r_i < opts.flying_send_per_node_; ++r_i) {
      global_sndbuf_vec[p_i].emplace_back((opts.global_size_ / PAGESIZE + 1) * PAGESIZE);
    }
  }

  std::atomic<bool> continued(true);
  /// 辅助后面的多线程并行域
  std::thread send_assist_thread ([&](void) {  // move complete request to buffer
    uint32_t idle_times = 0;
    while (continued.load()) {
      bool busy = false;
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        std::vector<MPI_Request> requests_vec;
        std::vector<decltype(reqlist.begin())> requests_it_vec;

        reqlck.lock();
        for (auto it = reqlist.begin(); reqlist.end() != it; ++it) {
          requests_vec.emplace_back(it->first);
          requests_it_vec.emplace_back(it);
        }
        reqlck.unlock();

        if (0 == requests_vec.size()) { continue; }

        int idx  = -1;
        int flag = 0;
        CHECK(MPI_SUCCESS == MPI_Testany(requests_vec.size(), requests_vec.data(), &idx, &flag, MPI_STATUSES_IGNORE));

        if (flag) {
          auto& it = requests_it_vec[idx];
          mem_ostream_t oss(std::move(it->second)); /// 异步请求发送完毕, 存储消息的oss可以释放了

          reqlck.lock();
          reqlist.erase(it);
          reqlck.unlock();

          oss.reset(); /// 重置数据(相当于清除数据了), beg=cur=0, 下次append数据时就从0开始了

          auto& buflck = buflck_vec[p_i];
          auto& blist  = global_sndbuf_vec[p_i];

          buflck.lock();
          blist.emplace_back(std::move(oss)); /// 同时最多往同一台机器发送flying_send_per_node_个请求
          buflck.unlock();

          busy = true;
        }
      }

      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }
  });

  #pragma omp parallel num_threads(opts.threads_) /// 下面block中代码被多线程并行执行(任务并没有分解), 每个线程维护自己的内存
  {
    std::vector<std::shared_ptr<oarchive_spec_t>> oarchives_vec(cluster_info.partitions_); // oarchives是各个线程内部的，blist是线程间汇总的(共享的)
    for (size_t p_i = 0; p_i < oarchives_vec.size(); ++p_i) {
      oarchives_vec[p_i].reset(new oarchive_spec_t(16 * PAGESIZE));
    }

    auto flush_local = [&](int p_i) { /// local指的是thread的buffer
      auto& poarchive = oarchives_vec[p_i];
      auto& buflck    = buflck_vec[p_i];
      auto& blist     = global_sndbuf_vec[p_i];

      uint32_t idles = 0;

      buflck.lock();
      while (0 == blist.size()) {  // wait for available slots
        buflck.unlock();
        if (++idles > 100) {
#ifdef __BSP_DEBUG__
          LOG(INFO) << "bsp send_task blocked with small 'flying_send_per_node_'" << opts.flying_send_per_node_;
#endif
          poll(nullptr, 0, 1);
          idles = 0;
        } else {
          pthread_yield();
        }
        buflck.lock();
      }

      append_chunk_tail(poarchive.get());
      auto chunk_buff = poarchive->get_intrusive_buffer();
      blist.back().write(chunk_buff.data_, chunk_buff.size_); /// 将某个线程的消息buffer汇总(flush)到partition的消息buffer

      if (blist.back().size() > opts.global_size_) {  // start a new ISend /// 消息已满,可以发送
        mem_ostream_t oss(std::move(blist.back()));
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        auto buff     = oss.get_intrusive_buffer();

        blist.pop_back();
        buflck.unlock();

        reqlck.lock();
        reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
        MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Shuffle, opts.comm_,
            &reqlist.back().first);
        reqlck.unlock();
      } else {
        buflck.unlock(); /// 消息未满,无事可做,释放🔒
      }

      poarchive->reset(); /// thread的msg buffer已经追加到partition的buffer里了
    };

    auto send_callback = [&](int p_i, const MSG& msg) { /// 将msg暂存到thread的buffer
      oarchives_vec[p_i]->emit(msg);
      if (oarchives_vec[p_i]->count() >= opts.local_capacity_) {  // flush oarchive, use size() will hurt performance
        flush_local(p_i);
      }
      return true;
    };

    send_task(send_callback); /// 各个线程同时开始send_task, while (actives.next_chunk(rebind_traversal, &chunk_size)) 知道actives被遍历完

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) { /// 收尾工作
      if (oarchives_vec[p_i]->count()) {
        flush_local(p_i);
      }
    }
  }

  continued.store(false); /// send_assist_thread停止工作!
  send_assist_thread.join();

  {  // flush global
    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) { /// 进程flush buffer
      auto& reqlist = flying_requests_vec[p_i];

      for (auto& oss: global_sndbuf_vec[p_i]) {
        if (oss.size()) {
          auto buff = oss.get_intrusive_buffer();

          reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
          MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Shuffle, opts.comm_,
              &reqlist.back().first);
        }
      }
    }

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      auto& reqlist = flying_requests_vec[p_i];
      std::vector<MPI_Request> requests_vec;

      for (auto& request: reqlist) {
        requests_vec.emplace_back(request.first);
      }
      /// 同步wait!!!
      CHECK(MPI_SUCCESS == MPI_Waitall(requests_vec.size(), requests_vec.data(), MPI_STATUSES_IGNORE)); /// block until all msg send success
    }
  }

  for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {  //  broadcast finish signal
    MPI_Send(nullptr, 0, MPI_CHAR, p_i, ShuffleFin, opts.comm_);
  }

  recv_thread.join();
  MPI_Barrier(opts.comm_);

  return 0;
} /// 并行域结束的地方应该会有同步屏障barriar

/*
 * resource-limited bulk synchronous parallel computation model
 *
 * Usually, a BSP superstep is composed of 'Computation' and 'Communication'.
 * We separate 'Computation' to 'Produce' and 'Consume' 2 steps, mixing messages
 * generating and consuming together to avoid caching all messages.
 *
 * \param  send_task         produce task, run in parallel
 * \param  recv_task         consume task, run in parallel
 * \param  opts              bsp options
 * \param  before_recv_task  run before recv task, multi-times, one per thread
 * \param  after_recv_task   run after recv task, multi-times, one per thread
 *
 * \return  0 -- success, else -- failed
 **/
template <typename MSG>
int fine_grain_bsp (
  bsp_send_task_t<MSG> send_task, /// std::function<void(bsp_send_callback_t<MSG>)>;
  bsp_recv_task_t<MSG> recv_task, /// std::function<void(int, bsp_recv_pmsg_t<MSG>&)>;
  bsp_opts_t opts = bsp_opts_t(),
  std::function<void(void)> before_recv_task = bsp_detail::dummy_func,
  std::function<void(void)> after_recv_task  = bsp_detail::dummy_func) {

  using namespace bsp_detail;
  using oarchive_spec_t = oarchive_t<MSG, mem_ostream_t>;
  using iarchive_spec_t = iarchive_t<MSG, mem_istream_t>;

  auto& cluster_info = cluster_info_t::get_instance();
  if (opts.threads_ <= 0) { opts.threads_ = cluster_info.threads_; }
  if (opts.flying_recv_ <= 0) { opts.flying_recv_ = cluster_info.partitions_; }

  std::atomic<bool> process_continue(true);
  // pin all these to numa node ??
  moodycamel::ConcurrentQueue<chunk_desc_t> chunk_queue;
  std::vector<std::shared_ptr<char>>        buffs_vec(opts.flying_recv_);
  std::unique_ptr<std::atomic<size_t>[]>    chunk_left(new std::atomic<size_t>[opts.flying_recv_]);

  const uint64_t buff_size = 2UL * 1024UL * (uint64_t)MBYTES - 1;
  for (size_t r_i = 0; r_i < buffs_vec.size(); ++r_i) {
    char* buff = (char*)mmap(nullptr, buff_size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    buffs_vec[r_i].reset(buff, [](char* p) { munmap(p, buff_size); });
    chunk_left[r_i].store(0);
  }

  /// produce msg
  std::thread recv_assist_thread([&](void) {
    std::atomic<int>         finished_count(0);
    std::vector<bool>        processing(opts.flying_recv_, false);
    std::vector<MPI_Request> requests_vec(opts.flying_recv_, MPI_REQUEST_NULL);

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      MPI_Irecv(buffs_vec[r_i].get(), buff_size, MPI_CHAR, MPI_ANY_SOURCE, opts.tag_, opts.comm_, &requests_vec[r_i]);
    }

    auto probe_once =
      [&](bool continued) {
        int  flag        = 0;
        int  index       = 0;
        int  recv_bytes  = 0;
        bool has_message = false;
        MPI_Status status;

        MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        while (flag && (MPI_UNDEFINED != index)) {
          MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
          if (0 == recv_bytes) {
            ++finished_count;
          } else {  // call recv_task
            char* buff = buffs_vec[index].get();
            CHECK(recv_bytes >= static_cast<int>(sizeof(chunk_tail_t))) << "recv message too small: " << recv_bytes;

            while (recv_bytes > 0) {  // push task in queue
              chunk_tail_t* tail = (chunk_tail_t*)(&buff[recv_bytes - sizeof(chunk_tail_t)]); /// chunk是首尾相接的
              CHECK(tail->size_ >= sizeof(chunk_tail_t));
              char* data = &buff[recv_bytes] - tail->size_;

              ++chunk_left[index];
              chunk_queue.enqueue(chunk_desc_t { data, tail->size_ - (uint32_t)sizeof(chunk_tail_t),
                                                 tail->count_, index, status.MPI_SOURCE });
              recv_bytes -= (int)tail->size_;
            }
            CHECK(0 == recv_bytes);
          }
          requests_vec[index] = MPI_REQUEST_NULL;
          processing[index] = true; /// processing记录是否还有msg没有被consume, 有新消息到来说明就还需要被process

          has_message = true;
          if (false == continued) { break; }

          MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        }

        return has_message;
      };

    auto restart_once = [&](void) {
      bool found = false;
      for (size_t i = 0; i < processing.size(); ++i) {
        if (processing[i] && (0 == chunk_left[i].load())) {
          found = true;
          processing[i] = false;
          MPI_Irecv(buffs_vec[i].get(), buff_size, MPI_CHAR, MPI_ANY_SOURCE, /// 上一个请求的msg已经被consume了,可以接收下一个请求了
              opts.tag_, opts.comm_, &requests_vec[i]);
        }
      }
      return found;
    };

    uint32_t idle_times = 0;
    while (finished_count < cluster_info.partitions_) {
      bool busy = probe_once(false);
      busy = restart_once() || busy; /// 有新消息到来或者有处理完的请求

      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }

    // we must probe here when use multi-threads. when one thread start irecv and release cpu for a while
    // another thread may receive finished signal and made finished_count bigger than cluster_info.partitions_,
    // then when first thread wake up, it will not process last received messages.
    probe_once(true);

    bool busy = false;
    do {  // wait all tasks finished
      busy = false;
      for (size_t i = 0; i < processing.size(); ++i) {
        if (processing[i] && (0 != chunk_left[i].load())) { /// 等待所有produce的消息都被consume掉
          busy = true;
        }
      }
      if (busy) { poll(nullptr, 0, 1); } /// why not pthread_yield()?
    } while (busy); /// busy==false: 没有新消息到来或者已有的请求都被处理完

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      if (MPI_REQUEST_NULL != requests_vec[r_i]) {
        MPI_Cancel(&requests_vec[r_i]);
        MPI_Wait(&requests_vec[r_i], MPI_STATUS_IGNORE);
      }
    }

    process_continue.store(false);
  });

  std::atomic<int> cpus(0);

  /// consume msg
  std::thread recv_thread ([&](void) {
    #pragma omp parallel num_threads(opts.threads_)
    {
      auto yeild = [&](bool inc, bool should_sleep) {
        if (inc) { ++cpus; }

        if (should_sleep) {
          poll(nullptr, 0, 1);
        } else {
          pthread_yield();
        }

        int times = 0;
        while (cpus.fetch_sub(1) <= 0) {
          ++cpus;
          if (++times > 10) {
            poll(nullptr, 0, 1);
            times = 0;
          } else {
            pthread_yield();
          }
        }
      };

      auto probe_once =
        [&](uint32_t batch_size) {
          bool has_message = false;
          chunk_desc_t chunk;

          uint32_t processed = 0;
          while (chunk_queue.try_dequeue(chunk)) {
            iarchive_spec_t iarchive(chunk.data_, chunk.size_, chunk.count_);
            for (auto msg = iarchive.absorb(); nullptr != msg; msg = iarchive.absorb()) {
              recv_task(chunk.from_, msg); /// consume msg
            }
            --chunk_left[chunk.index_];

            has_message = true;
            if (++processed >= batch_size) { break; }
          }

          return has_message;
        };

      uint32_t idles = 0;

      yeild(false, true);
      before_recv_task();
      while (process_continue) {
        idles += (uint32_t)(false == probe_once(opts.batch_size_));

        if (idles > 3) {
          yeild(true, true); /// sleep
          idles = 0;
        } else {
          yeild(true, false); /// pthread_yield
        }
      }
      after_recv_task();
    }
  });

  std::vector<spinlock_t> buflck_vec(cluster_info.partitions_);
  std::vector<std::list<mem_ostream_t>> global_sndbuf_vec(cluster_info.partitions_);

  std::vector<spinlock_t> reqlck_vec(cluster_info.partitions_);
  std::vector<std::list<std::pair<MPI_Request, mem_ostream_t>>> flying_requests_vec(cluster_info.partitions_);

#ifdef __BSP_DEBUG__
  volatile bool perf_continue = true;
  std::thread perf_thread([&](void) {
    time_t __stump = 0;
    while (perf_continue) {
      if (time(nullptr) - __stump < 10) { poll(nullptr, 0, 1); continue; }
      __stump = time(nullptr);

      LOG(INFO) << "[PERF] cpus: " << cpus.load();

      __asm volatile ("pause" ::: "memory");

      for (size_t i = 0; i < cluster_info.partitions_; ++i) {
        LOG(INFO) << "[PERF] sndbuf: " << i << " - " << global_sndbuf_vec[i].size();
        LOG(INFO) << "[PERF] flying: " << i << " - " << flying_requests_vec[i].size();
      }
    }
  });
#endif

  for (size_t p_i = 0; p_i < global_sndbuf_vec.size(); ++p_i) {
    for (int r_i = 0; r_i < opts.flying_send_per_node_; ++r_i) {
      global_sndbuf_vec[p_i].emplace_back((opts.global_size_ / PAGESIZE + 1) * PAGESIZE);
    }
  }

  std::atomic<bool> continued(true);
  std::thread send_assist_thread ([&](void) {  // move complete request to buffer
    uint32_t idle_times = 0;
    while (continued.load()) {
      bool busy = false;
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        std::vector<MPI_Request> requests_vec;
        std::vector<decltype(reqlist.begin())> requests_it_vec;

        reqlck.lock();
        for (auto it = reqlist.begin(); reqlist.end() != it; ++it) {
          requests_vec.emplace_back(it->first);
          requests_it_vec.emplace_back(it);
        }
        reqlck.unlock();

        if (0 == requests_vec.size()) { continue; }

        int idx  = -1;
        int flag = 0;
        CHECK(MPI_SUCCESS == MPI_Testany(requests_vec.size(), requests_vec.data(), &idx, &flag, MPI_STATUSES_IGNORE));

        if (flag) {
          auto& it = requests_it_vec[idx];
          mem_ostream_t oss(std::move(it->second));

          reqlck.lock();
          reqlist.erase(it);
          reqlck.unlock();

          oss.reset();

          auto& buflck = buflck_vec[p_i];
          auto& blist  = global_sndbuf_vec[p_i];

          buflck.lock();
          blist.emplace_back(std::move(oss));
          buflck.unlock();

          busy = true;
        }
      }

      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }
  });

  #pragma omp parallel num_threads(opts.threads_)
  {
    std::vector<std::shared_ptr<oarchive_spec_t>> oarchives_vec(cluster_info.partitions_);
    for (size_t p_i = 0; p_i < oarchives_vec.size(); ++p_i) {
      oarchives_vec[p_i].reset(new oarchive_spec_t(16 * PAGESIZE));
    }

    auto yeild = [&](bool should_sleep) {
      ++cpus;

      if (should_sleep) {
        poll(nullptr, 0, 1); /// sleep 1ms
      } else {
        pthread_yield();
      }

      int times = 0;
      while (cpus.fetch_sub(1) <= 0) {
        ++cpus;

        if (++times > 10) {
          poll(nullptr, 0, 1);
          times = 0;
        } else {
          pthread_yield();
        }
      }
    };

    auto flush_local = [&](int p_i) {
      auto& poarchive = oarchives_vec[p_i];
      auto& buflck    = buflck_vec[p_i];
      auto& blist     = global_sndbuf_vec[p_i];

      uint32_t idles = 0;

      buflck.lock();
      while (0 == blist.size()) {  // wait for available slots
        buflck.unlock();
        if (++idles > 3) {
          yeild(true);
          idles = 0;
        } else {
          yeild(false);
        }
        buflck.lock();
      }

      append_chunk_tail(poarchive.get());
      auto chunk_buff = poarchive->get_intrusive_buffer();
      blist.back().write(chunk_buff.data_, chunk_buff.size_);

      if (blist.back().size() > opts.global_size_) {  // start a new ISend
        mem_ostream_t oss(std::move(blist.back()));
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        auto buff     = oss.get_intrusive_buffer();

        blist.pop_back();
        buflck.unlock();

        reqlck.lock();
        reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
        CHECK(buff.size_ > 0);
        MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, opts.tag_, opts.comm_,
            &reqlist.back().first);
        reqlck.unlock();
      } else {
        buflck.unlock();
      }

      poarchive->reset();
    };

    auto send_callback = [&](int p_i, const MSG& msg) {
      oarchives_vec[p_i]->emit(msg);
      if (oarchives_vec[p_i]->count() >= opts.local_capacity_) {  // flush oarchive, use size() will hurt performance
        flush_local(p_i);
      }
      return true;
    };

    send_task(send_callback);

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      if (oarchives_vec[p_i]->count()) {
        flush_local(p_i);
      }
    }

    ++cpus;
  }

#ifdef __BSP_DEBUG__
  LOG(INFO) << cluster_info.partition_id_ << " - send finished";
#endif

  continued.store(false);
  send_assist_thread.join();

  {  // flush global
    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      auto& reqlist = flying_requests_vec[p_i];

      for (auto& oss: global_sndbuf_vec[p_i]) {
        if (oss.size()) {
          auto buff = oss.get_intrusive_buffer();

          reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
          CHECK(buff.size_ > 0);
          MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, opts.tag_, opts.comm_,
              &reqlist.back().first);
        }
      }
    }

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      auto& reqlist = flying_requests_vec[p_i];
      std::vector<MPI_Request> requests_vec;

      for (auto& request: reqlist) {
        requests_vec.emplace_back(request.first);
      }

      CHECK(MPI_SUCCESS == MPI_Waitall(requests_vec.size(), requests_vec.data(), MPI_STATUSES_IGNORE));
    }
  }

  for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {  //  broadcast finish signal
    MPI_Send(nullptr, 0, MPI_CHAR, p_i, opts.tag_, opts.comm_);
  }

  recv_assist_thread.join();
  recv_thread.join();
  MPI_Barrier(opts.comm_);

#ifdef __BSP_DEBUG__
  perf_continue = false;
  perf_thread.join();
#endif

  return 0;
}


// partition-id, send-callback => void
template <typename REQ_MSG, typename RESP_MSG>
using bsp_produce_resp_callback_t = std::function<RESP_MSG(int, bsp_recv_pmsg_t<REQ_MSG>&)>;

/*
 * resource-limited bulk synchronous parallel computation model
 *
 * Usually, a BSP superstep is composed of 'Computation' and 'Communication'.
 * We separate 'Computation' to 'Produce' and 'Consume' 2 steps, mixing messages
 * generating and consuming together to avoid caching all messages.
 *
 * \param  send_task         produce task, run in parallel
 * \param  recv_task         consume task, run in parallel
 * \param  opts              bsp options
 * \param  before_recv_task  run before recv task, multi-times, one per thread
 * \param  after_recv_task   run after recv task, multi-times, one per thread
 *
 * \return  0 -- success, else -- failed
 **/
template <typename REQ_MSG, typename RESP_MSG>
int fine_grain_bsp2 (
  bsp_send_task_t<REQ_MSG> send_req_task,
  bsp_produce_resp_callback_t<REQ_MSG, RESP_MSG> recv_req_callback,
  bsp_recv_task_t<RESP_MSG> recv_resp_task,
  bsp_opts_t opts = bsp_opts_t(),
  std::function<void(void)> before_recv_task = bsp_detail::dummy_func,
  std::function<void(void)> after_recv_task  = bsp_detail::dummy_func) {

  using namespace bsp_detail;
  using oarchive_req_spec_t = oarchive_t<REQ_MSG, mem_ostream_t>;
  using oarchive_resp_spec_t = oarchive_t<RESP_MSG, mem_ostream_t>;
  using iarchive_req_spec_t = iarchive_t<REQ_MSG, mem_istream_t>;
  using iarchive_resp_spec_t = iarchive_t<RESP_MSG, mem_istream_t>;

  auto& cluster_info = cluster_info_t::get_instance();
  if (opts.threads_ <= 0) { opts.threads_ = cluster_info.threads_; }
  if (opts.flying_recv_ <= 0) { opts.flying_recv_ = cluster_info.partitions_; }

  // pin all these to numa node ??
  std::vector<std::shared_ptr<char>>        resp_buffs_vec(opts.flying_recv_);

  const uint64_t resp_buff_size = 2UL * 1024UL * (uint64_t)MBYTES - 1;
  for (size_t r_i = 0; r_i < resp_buffs_vec.size(); ++r_i) {
    char* buff = (char*)mmap(nullptr, resp_buff_size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    resp_buffs_vec[r_i].reset(buff, [](char* p) { munmap(p, resp_buff_size); });
  }

  LOG(INFO) << "mark 0";
  std::thread recv_resp_thread([&](void) {
    LOG(INFO) << "recv_resp_thread start working...";
    std::atomic<int>         finished_count(0);
    std::vector<MPI_Request> requests_vec(opts.flying_recv_, MPI_REQUEST_NULL);

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      MPI_Irecv(resp_buffs_vec[r_i].get(), resp_buff_size, MPI_CHAR, MPI_ANY_SOURCE, Response, opts.comm_, &requests_vec[r_i]);
    }

    auto probe_once =
      [&](bool continued) {
        int  flag        = 0;
        int  index       = 0;
        int  recv_bytes  = 0;
        bool has_message = false;
        MPI_Status status;

        MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        while (flag && (MPI_UNDEFINED != index)) {
          MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
          if (0 == recv_bytes) {
            //LOG(INFO) << "[" << cluster_info.partition_id_ << "]: recved a resp fin";
            ++finished_count;
          } else {  // call recv_task
            //LOG(INFO) << "[" << cluster_info.partition_id_ << "]: recved a resp msg";
            char* buff = resp_buffs_vec[index].get();
            CHECK(recv_bytes >= static_cast<int>(sizeof(chunk_tail_t))) << "recv message too small: " << recv_bytes;

            while (recv_bytes > 0) {
              chunk_tail_t* tail = (chunk_tail_t*)(&buff[recv_bytes - sizeof(chunk_tail_t)]);
              CHECK(tail->size_ >= sizeof(chunk_tail_t));
              char* data = &buff[recv_bytes] - tail->size_;

              iarchive_resp_spec_t iarchive(data, tail->size_ - (uint32_t)sizeof(chunk_tail_t),
                                                 tail->count_);
              int from = status.MPI_SOURCE;
              for (auto msg = iarchive.absorb(); nullptr != msg; msg = iarchive.absorb()) {
                recv_resp_task(from, msg);
              }
              recv_bytes -= (int)tail->size_;
            }
            CHECK(0 == recv_bytes);
            requests_vec[index] = MPI_REQUEST_NULL;
            MPI_Irecv(resp_buffs_vec[index].get(), resp_buff_size, MPI_CHAR, MPI_ANY_SOURCE,
                Response, opts.comm_, &requests_vec[index]);
          }

          has_message = true;
          if (false == continued) { break; }

          MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        }

        return has_message;
      };

    uint32_t idle_times = 0;
    while (finished_count < cluster_info.partitions_) {
      bool busy = probe_once(false);
      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }
    LOG(INFO) << "mark 1";

    // we must probe here when use multi-threads. when one thread start irecv and release cpu for a while
    // another thread may receive finished signal and made finished_count bigger than cluster_info.partitions_,
    // then when first thread wake up, it will not process last received messages.
    probe_once(true);

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      if (MPI_REQUEST_NULL != requests_vec[r_i]) {
        MPI_Cancel(&requests_vec[r_i]);
        MPI_Wait(&requests_vec[r_i], MPI_STATUS_IGNORE);
      }
    }
    LOG(INFO) << "recv_resp_thread exit working...";
  });


  LOG(INFO) << "makr 2";
  std::atomic<bool> process_continue(true);
  // pin all these to numa node ??
  moodycamel::ConcurrentQueue<chunk_desc_t> chunk_queue;
  std::vector<std::shared_ptr<char>>        buffs_vec(opts.flying_recv_);
  std::unique_ptr<std::atomic<size_t>[]>    chunk_left(new std::atomic<size_t>[opts.flying_recv_]);

  const uint64_t buff_size = 2UL * 1024UL * (uint64_t)MBYTES - 1;
  for (size_t r_i = 0; r_i < buffs_vec.size(); ++r_i) {
    char* buff = (char*)mmap(nullptr, buff_size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    buffs_vec[r_i].reset(buff, [](char* p) { munmap(p, buff_size); });
    chunk_left[r_i].store(0);
  }

  std::thread recv_req_assist_thread([&](void) {
    LOG(INFO) << "recv_req_assist_thread start working...";
    std::atomic<int>         finished_count(0);
    std::vector<bool>        processing(opts.flying_recv_, false);
    std::vector<MPI_Request> requests_vec(opts.flying_recv_, MPI_REQUEST_NULL);

    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      MPI_Irecv(buffs_vec[r_i].get(), buff_size, MPI_CHAR, MPI_ANY_SOURCE, Request, opts.comm_, &requests_vec[r_i]);
    }

    auto probe_once =
      [&](bool continued) {
        int  flag        = 0;
        int  index       = 0;
        int  recv_bytes  = 0;
        bool has_message = false;
        MPI_Status status;

        MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        while (flag && (MPI_UNDEFINED != index)) {
          MPI_Get_count(&status, MPI_CHAR, &recv_bytes);
          if (0 == recv_bytes) {
            //LOG(INFO) << "[" << cluster_info.partition_id_ << "]: recved a req fin";
            ++finished_count;
          } else {  // call recv_task
            //LOG(INFO) << "[" << cluster_info.partition_id_ << "]: recved a req msg";
            char* buff = buffs_vec[index].get();
            CHECK(recv_bytes >= static_cast<int>(sizeof(chunk_tail_t))) << "recv message too small: " << recv_bytes;

            while (recv_bytes > 0) {  // push task in queue
              chunk_tail_t* tail = (chunk_tail_t*)(&buff[recv_bytes - sizeof(chunk_tail_t)]); /// chunk是首尾相接的
              CHECK(tail->size_ >= sizeof(chunk_tail_t));
              char* data = &buff[recv_bytes] - tail->size_;

              ++chunk_left[index];
              chunk_queue.enqueue(chunk_desc_t { data, tail->size_ - (uint32_t)sizeof(chunk_tail_t),
                                                 tail->count_, index, status.MPI_SOURCE });
              recv_bytes -= (int)tail->size_;
            }
            CHECK(0 == recv_bytes);
          }
          requests_vec[index] = MPI_REQUEST_NULL;
          processing[index] = true; /// processing记录是否还有msg没有被consume, 有新消息到来说明就还需要被process

          has_message = true;
          if (false == continued) { break; }

          MPI_Testany(requests_vec.size(), requests_vec.data(), &index, &flag, &status);
        }

        return has_message;
      };

    auto restart_once = [&](void) {
      bool found = false;
      for (size_t i = 0; i < processing.size(); ++i) {
        if (processing[i] && (0 == chunk_left[i].load())) {
          found = true;
          processing[i] = false;
          MPI_Irecv(buffs_vec[i].get(), buff_size, MPI_CHAR, MPI_ANY_SOURCE, /// 上一个请求的msg已经被consume了,可以接收下一个请求了
              Request, opts.comm_, &requests_vec[i]);
        }
      }
      return found;
    };

    LOG(INFO) << "mark 3";
    uint32_t idle_times = 0;
    while (finished_count < cluster_info.partitions_) {
      bool busy = probe_once(false);
      busy = restart_once() || busy; /// 有新消息到来或者有处理完的请求

      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }
    LOG(INFO) << "mark 4";

    // we must probe here when use multi-threads. when one thread start irecv and release cpu for a while
    // another thread may receive finished signal and made finished_count bigger than cluster_info.partitions_,
    // then when first thread wake up, it will not process last received messages.
    probe_once(true);

    bool busy = false;
    do {  // wait all tasks finished
      busy = false;
      for (size_t i = 0; i < processing.size(); ++i) {
        if (processing[i] && (0 != chunk_left[i].load())) { /// 等待所有produce的消息都被consume掉
          busy = true;
        }
      }
      if (busy) { poll(nullptr, 0, 1); } /// why not pthread_yield()?
    } while (busy); /// busy==false: 没有新消息到来或者已有的请求都被处理完

    LOG(INFO) << "mark 5";
    for (size_t r_i = 0; r_i < requests_vec.size(); ++r_i) {
      if (MPI_REQUEST_NULL != requests_vec[r_i]) {
        MPI_Cancel(&requests_vec[r_i]);
        MPI_Wait(&requests_vec[r_i], MPI_STATUS_IGNORE);
      }
    }

    process_continue.store(false);
    LOG(INFO) << "recv_req_assist_thread exit working...";
  });


  std::atomic<int> cpus(0);

  LOG(INFO) << "mark 6";
  std::thread recv_req_thread ([&](void) {
    LOG(INFO) << "recv_req_thread start working...";
    std::atomic<int> cpus2(0);

    std::vector<spinlock_t> buflck_vec(cluster_info.partitions_);
    std::vector<std::list<mem_ostream_t>> global_sndbuf_vec(cluster_info.partitions_);

    std::vector<spinlock_t> reqlck_vec(cluster_info.partitions_);
    std::vector<std::list<std::pair<MPI_Request, mem_ostream_t>>> flying_requests_vec(cluster_info.partitions_);

    for (size_t p_i = 0; p_i < global_sndbuf_vec.size(); ++p_i) {
      for (int r_i = 0; r_i < opts.flying_send_per_node_; ++r_i) {
        global_sndbuf_vec[p_i].emplace_back((opts.global_size_ / PAGESIZE + 1) * PAGESIZE);
      }
    }

    std::atomic<bool> continued(true);
    std::thread send_resp_assist_thread ([&](void) {  // move complete request to buffer
      LOG(INFO) << "send_resp_assist_thread start working...";
      uint32_t idle_times = 0;
      while (continued.load()) {
        //LOG(INFO) << "send_resp_assist_thread contihnues...";
        bool busy = false;
        for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
          auto& reqlck  = reqlck_vec[p_i];
          auto& reqlist = flying_requests_vec[p_i];
          std::vector<MPI_Request> requests_vec;
          std::vector<decltype(reqlist.begin())> requests_it_vec;

          reqlck.lock();
          for (auto it = reqlist.begin(); reqlist.end() != it; ++it) {
            requests_vec.emplace_back(it->first);
            requests_it_vec.emplace_back(it);
          }
          reqlck.unlock();

          if (0 == requests_vec.size()) { continue; }

          int idx  = -1;
          int flag = 0;
          CHECK(MPI_SUCCESS == MPI_Testany(requests_vec.size(), requests_vec.data(), &idx, &flag, MPI_STATUSES_IGNORE));

          if (flag) {
            auto& it = requests_it_vec[idx];
            mem_ostream_t oss(std::move(it->second));

            reqlck.lock();
            reqlist.erase(it);
            reqlck.unlock();

            oss.reset();

            auto& buflck = buflck_vec[p_i];
            auto& blist  = global_sndbuf_vec[p_i];

            buflck.lock();
            blist.emplace_back(std::move(oss));
            buflck.unlock();

            busy = true;
          }
        }

        idle_times += (uint32_t)(false == busy);
        if (idle_times > 10) {
          poll(nullptr, 0, 1);
          idle_times = 0;
        } else if (false == busy) {
          pthread_yield();
        }
      }
      LOG(INFO) << "send_resp_thread exit working...";
    });

    LOG(INFO) << "mark 7";
    #pragma omp parallel num_threads(opts.threads_)
    {
      std::vector<std::shared_ptr<oarchive_resp_spec_t>> oarchives_vec(cluster_info.partitions_);
      for (size_t p_i = 0; p_i < oarchives_vec.size(); ++p_i) {
        oarchives_vec[p_i].reset(new oarchive_resp_spec_t(16 * PAGESIZE));
      }

      auto yeild_send_resp = [&](bool should_sleep) {
        ++cpus2;

        if (should_sleep) {
          poll(nullptr, 0, 1); /// sleep 1ms
        } else {
          pthread_yield();
        }

        int times = 0;
        while (cpus2.fetch_sub(1) <= 0) {
          ++cpus2;

          if (++times > 10) {
            poll(nullptr, 0, 1);
            times = 0;
          } else {
            pthread_yield();
          }
        }
      };

      auto flush_local = [&](int p_i) {
        //LOG(INFO) << "send_resp: flush_local...";
        auto& poarchive = oarchives_vec[p_i];
        auto& buflck    = buflck_vec[p_i];
        auto& blist     = global_sndbuf_vec[p_i];

        uint32_t idles = 0;

        buflck.lock();
        //LOG(INFO) << "send_resp: flus local, wait for avalible slots";
        while (0 == blist.size()) {  // wait for available slots
          buflck.unlock();
          if (++idles > 3) {
            yeild_send_resp(true);
            idles = 0;
          } else {
            yeild_send_resp(false);
          }
          buflck.lock();
        }
        //LOG(INFO) << "send_resp: flus local, found avalible slots: " << blist.size();

        append_chunk_tail(poarchive.get());
        auto chunk_buff = poarchive->get_intrusive_buffer();
        blist.back().write(chunk_buff.data_, chunk_buff.size_);

        if (blist.back().size() > opts.global_size_) {  // start a new ISend
          //LOG(INFO) << "send_resp:flus local, start a new Isend";
          mem_ostream_t oss(std::move(blist.back()));
          auto& reqlck  = reqlck_vec[p_i];
          auto& reqlist = flying_requests_vec[p_i];
          auto buff     = oss.get_intrusive_buffer();

          blist.pop_back();
          buflck.unlock();

          reqlck.lock();
          reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
          CHECK(buff.size_ > 0);
          MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Response, opts.comm_,
              &reqlist.back().first);
          reqlck.unlock();
          //LOG(INFO) << "send_resp:flus local, end a new Isend";
        } else {
          //LOG(INFO) << "send_resp:flus local, Do not start a new Isend";
          buflck.unlock();
        }

        poarchive->reset();
        //LOG(INFO) << "send_resp: flush_local ended...";
      };

      auto send_resp_callback = [&](int p_i, const RESP_MSG& msg) {
        oarchives_vec[p_i]->emit(msg);
        if (oarchives_vec[p_i]->count() >= opts.local_capacity_) {  // flush oarchive, use size() will hurt performance
          flush_local(p_i);
        }
        return true;
      };



      auto yeild = [&](bool inc, bool should_sleep) {
        if (inc) { ++cpus; }

        if (should_sleep) {
          poll(nullptr, 0, 1);
        } else {
          pthread_yield();
        }

        int times = 0;
        while (cpus.fetch_sub(1) <= 0) {
          ++cpus;
          if (++times > 10) {
            poll(nullptr, 0, 1);
            times = 0;
          } else {
            pthread_yield();
          }
        }
      };

      auto probe_once =
        [&](uint32_t batch_size) {
          bool has_message = false;
          chunk_desc_t chunk;

          uint32_t processed = 0;
          while (chunk_queue.try_dequeue(chunk)) {
            iarchive_req_spec_t iarchive(chunk.data_, chunk.size_, chunk.count_);
            for (auto msg = iarchive.absorb(); nullptr != msg; msg = iarchive.absorb()) {
              auto resp_msg = recv_req_callback(chunk.from_, msg);
              send_resp_callback(chunk.from_, resp_msg); // 必须在这里才能阻塞住recv_req
            }
            --chunk_left[chunk.index_];

            has_message = true;
            if (++processed >= batch_size) { break; }
          }

          return has_message;
        };

      uint32_t idles = 0;

      yeild(false, true);
      before_recv_task();
      LOG(INFO) << "mark 8";
      while (process_continue) {
        idles += (uint32_t)(false == probe_once(opts.batch_size_));

        if (idles > 3) {
          yeild(true, true); /// sleep
          idles = 0;
        } else {
          yeild(true, false); /// pthread_yield
        }
      }
      LOG(INFO) << "mark 9";
      after_recv_task(); //?

      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        if (oarchives_vec[p_i]->count()) {
          flush_local(p_i);
        }
      }
      LOG(INFO) << "mark 10";

      ++cpus2;
      // after_recv_task(); //?
    }

    LOG(INFO) << "mark 11";
    continued.store(false);
    send_resp_assist_thread.join();
    LOG(INFO) << "makr 12";
    LOG(INFO) << "send_resp_assist_thread joined..";

    {  // flush global
      LOG(INFO) << "mark 101";
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto& reqlist = flying_requests_vec[p_i];

        for (auto& oss: global_sndbuf_vec[p_i]) {
          if (oss.size()) {
            auto buff = oss.get_intrusive_buffer();

            reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
            CHECK(buff.size_ > 0);
            MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Response, opts.comm_,
                &reqlist.back().first);
          }
        }
      }

      LOG(INFO) << "mark 102";
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto& reqlist = flying_requests_vec[p_i];
        std::vector<MPI_Request> requests_vec;

        for (auto& request: reqlist) {
          requests_vec.emplace_back(request.first);
        }

        CHECK(MPI_SUCCESS == MPI_Waitall(requests_vec.size(), requests_vec.data(), MPI_STATUSES_IGNORE));
        LOG(INFO) << "mark 13";
      }
      LOG(INFO) << "mark 103";
    }
    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {  //  broadcast finish signal
      MPI_Send(nullptr, 0, MPI_CHAR, p_i, Response, opts.comm_);
    }
    LOG(INFO) << "mark 14";

  });

  std::vector<spinlock_t> buflck_vec(cluster_info.partitions_);
  std::vector<std::list<mem_ostream_t>> global_sndbuf_vec(cluster_info.partitions_);

  std::vector<spinlock_t> reqlck_vec(cluster_info.partitions_);
  std::vector<std::list<std::pair<MPI_Request, mem_ostream_t>>> flying_requests_vec(cluster_info.partitions_);

#ifdef __BSP_DEBUG__
  volatile bool perf_continue = true;
  std::thread perf_thread([&](void) {
    time_t __stump = 0;
    while (perf_continue) {
      if (time(nullptr) - __stump < 10) { poll(nullptr, 0, 1); continue; }
      __stump = time(nullptr);

      LOG(INFO) << "[PERF] cpus: " << cpus.load();

      __asm volatile ("pause" ::: "memory");

      for (size_t i = 0; i < cluster_info.partitions_; ++i) {
        LOG(INFO) << "[PERF] sndbuf: " << i << " - " << global_sndbuf_vec[i].size();
        LOG(INFO) << "[PERF] flying: " << i << " - " << flying_requests_vec[i].size();
      }
    }
  });
#endif

  for (size_t p_i = 0; p_i < global_sndbuf_vec.size(); ++p_i) {
    for (int r_i = 0; r_i < opts.flying_send_per_node_; ++r_i) {
      global_sndbuf_vec[p_i].emplace_back((opts.global_size_ / PAGESIZE + 1) * PAGESIZE);
    }
  }

  LOG(INFO) << "mark 15";
  std::atomic<bool> continued(true);
  std::thread send_req_assist_thread ([&](void) {  // move complete request to buffer
    LOG(INFO) << "send_req_assist_thread start working...";
    uint32_t idle_times = 0;
    while (continued.load()) {
      //LOG(INFO) << "sennd_req_assist_thread continued...";
      bool busy = false;
      for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        std::vector<MPI_Request> requests_vec;
        std::vector<decltype(reqlist.begin())> requests_it_vec;

        reqlck.lock();
        for (auto it = reqlist.begin(); reqlist.end() != it; ++it) {
          requests_vec.emplace_back(it->first);
          requests_it_vec.emplace_back(it);
        }
        reqlck.unlock();

        if (0 == requests_vec.size()) { continue; }

        int idx  = -1;
        int flag = 0;
        CHECK(MPI_SUCCESS == MPI_Testany(requests_vec.size(), requests_vec.data(), &idx, &flag, MPI_STATUSES_IGNORE));

        if (flag) {
          auto& it = requests_it_vec[idx];
          mem_ostream_t oss(std::move(it->second));

          reqlck.lock();
          reqlist.erase(it);
          reqlck.unlock();

          oss.reset();

          auto& buflck = buflck_vec[p_i];
          auto& blist  = global_sndbuf_vec[p_i];

          buflck.lock();
          blist.emplace_back(std::move(oss));
          buflck.unlock();

          busy = true;
        }
      }

      idle_times += (uint32_t)(false == busy);
      if (idle_times > 10) {
        poll(nullptr, 0, 1);
        idle_times = 0;
      } else if (false == busy) {
        pthread_yield();
      }
    }
    LOG(INFO) << "send_req_assist_thread exit workin...";
  });

  LOG(INFO) << "mark 16";
  #pragma omp parallel num_threads(opts.threads_)
  {
    std::vector<std::shared_ptr<oarchive_req_spec_t>> oarchives_vec(cluster_info.partitions_);
    for (size_t p_i = 0; p_i < oarchives_vec.size(); ++p_i) {
      oarchives_vec[p_i].reset(new oarchive_req_spec_t(16 * PAGESIZE));
    }

    auto yeild = [&](bool should_sleep) {
      ++cpus;

      if (should_sleep) {
        poll(nullptr, 0, 1); /// sleep 1ms
      } else {
        pthread_yield();
      }

      int times = 0;
      while (cpus.fetch_sub(1) <= 0) {
        ++cpus;

        if (++times > 10) {
          poll(nullptr, 0, 1);
          times = 0;
        } else {
          pthread_yield();
        }
      }
    };

    auto flush_local = [&](int p_i) {
      auto& poarchive = oarchives_vec[p_i];
      auto& buflck    = buflck_vec[p_i];
      auto& blist     = global_sndbuf_vec[p_i];

      uint32_t idles = 0;

      buflck.lock();
      while (0 == blist.size()) {  // wait for available slots
        buflck.unlock();
        if (++idles > 3) {
          yeild(true);
          idles = 0;
        } else {
          yeild(false);
        }
        buflck.lock();
      }

      append_chunk_tail(poarchive.get());
      auto chunk_buff = poarchive->get_intrusive_buffer();
      blist.back().write(chunk_buff.data_, chunk_buff.size_);

      if (blist.back().size() > opts.global_size_) {  // start a new ISend
        mem_ostream_t oss(std::move(blist.back()));
        auto& reqlck  = reqlck_vec[p_i];
        auto& reqlist = flying_requests_vec[p_i];
        auto buff     = oss.get_intrusive_buffer();

        blist.pop_back();
        buflck.unlock();

        reqlck.lock();
        reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
        CHECK(buff.size_ > 0);
        MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Request, opts.comm_,
            &reqlist.back().first);
        reqlck.unlock();
      } else {
        buflck.unlock();
      }

      poarchive->reset();
    };

    auto send_req_callback = [&](int p_i, const REQ_MSG& msg) {
      oarchives_vec[p_i]->emit(msg);
      if (oarchives_vec[p_i]->count() >= opts.local_capacity_) {  // flush oarchive, use size() will hurt performance
        flush_local(p_i);
      }
      return true;
    };

    LOG(INFO) << "mark 17";
    send_req_task(send_req_callback);
    LOG(INFO) << "mark 18";

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      if (oarchives_vec[p_i]->count()) {
        flush_local(p_i);
      }
    }
    LOG(INFO) << "mark 19";

    ++cpus;
  }
  LOG(INFO) << "mark 20";

#ifdef __BSP_DEBUG__
  LOG(INFO) << cluster_info.partition_id_ << " - send finished";
#endif

  continued.store(false);
  send_req_assist_thread.join();
  LOG(INFO) << "mark 21";
  LOG(INFO) << "send_req_assist_thread joined";

  {  // flush global
    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      auto& reqlist = flying_requests_vec[p_i];

      for (auto& oss: global_sndbuf_vec[p_i]) {
        if (oss.size()) {
          auto buff = oss.get_intrusive_buffer();

          reqlist.emplace_back(std::move(std::make_pair(MPI_Request(), std::move(oss))));
          CHECK(buff.size_ > 0);
          MPI_Isend(buff.data_, buff.size_, MPI_CHAR, p_i, Request, opts.comm_,
              &reqlist.back().first);
        }
      }
    }

    for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {
      auto& reqlist = flying_requests_vec[p_i];
      std::vector<MPI_Request> requests_vec;

      for (auto& request: reqlist) {
        requests_vec.emplace_back(request.first);
      }

      CHECK(MPI_SUCCESS == MPI_Waitall(requests_vec.size(), requests_vec.data(), MPI_STATUSES_IGNORE));
    }
  }
  LOG(INFO) << "mark 22";

  for (int p_i = 0; p_i < cluster_info.partitions_; ++p_i) {  //  broadcast finish signal
    MPI_Send(nullptr, 0, MPI_CHAR, p_i, Request, opts.comm_);
  }
  LOG(INFO) << "mark 23";

  recv_req_assist_thread.join();
  LOG(INFO) << "mark 24";
  recv_req_thread.join();
  LOG(INFO) << "mark 25";
  recv_resp_thread.join();
  LOG(INFO) << "mark 26";
  MPI_Barrier(opts.comm_);
  LOG(INFO) << "mark 99";
#ifdef __BSP_DEBUG__
  perf_continue = false;
  perf_thread.join();
#endif

  return 0;
}

// ******************************************************************************* //

}

#endif

