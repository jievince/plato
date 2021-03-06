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

#include "libcuckoo/cuckoohash_map.hh"
#include "plato/graph/base.hpp"
#include "plato/graph/message_passing.hpp"
#include "plato/graph/structure/edge_cache.hpp"
#include "plato/util/perf.hpp"

namespace plato {

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, size_t>::type size(VID_T) {
  return sizeof(VID_T);
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, size_t>::type size(const VID_T& vid) { // std::string
  return vid.size();
}

template<typename VID_T>
inline typename std::enable_if<std::is_integral<VID_T>::value, const VID_T*>::type addr(const VID_T& vid) {
  return &vid;
}

template<typename VID_T>
inline typename std::enable_if<!std::is_integral<VID_T>::value, const char*>::type addr(const VID_T& vid) { // std:string
  return vid.c_str();
}

template <typename EDATA, typename VID_T = vid_t,
          template <typename, typename> class CACHE = edge_block_cache_t>
class vid_encoder_base_t {
public:
  using encoder_callback_t =
      std::function<bool(edge_unit_t<EDATA, vid_t> *, size_t)>;
  /**
   * @brief
   * @param opts
   */
  vid_encoder_base_t() = default;

  /**
   * @brief encode
   * @param cache
   * @param callback
   */
  virtual void encode(CACHE<EDATA, VID_T> &cache,
                      encoder_callback_t callback) = 0;

  virtual void set_vids(const std::vector<VID_T>& vids) = 0;

  virtual void get_vids(std::vector<vid_t>& encoded_vids) = 0;

  /**
   * @brief decode
   * @param v_i
   * @return
   */
  virtual VID_T decode(vid_t v_i) = 0;

  /**
   * @brief getter
   * @return
   */
  virtual const std::vector<VID_T> &data() = 0;
};

} // namespace plato
