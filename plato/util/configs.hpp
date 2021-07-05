//-----------------------------------------------------------------------------
// MurmurHash2 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

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

#include <fstream>
#include <string>
#include <unordered_map>

#include "boost/algorithm/string.hpp"
#include "glog/logging.h"

namespace plato {

struct Configs {
  Configs(const std::string &path, const std::string &pathPrefix = "") {
    CHECK(boost::istarts_with(path, pathPrefix))
        << "config path doesn't starts with pathPrefix, path: " << path
        << ", pathPrefix: " << pathPrefix;
    const std::string &realPath = path.substr(pathPrefix.size());
    std::ifstream fin(realPath.c_str());
    if (!fin.good()) {
      LOG(FATAL) << "open config file " << realPath << " failed";
    }

    std::string line, key, val;
    size_t posEqual, posComment;
    LOG(INFO) << "Configs: " << realPath;
    while (fin.good() && (false == fin.eof())) {
      std::getline(fin, line);
      //LOG(INFO) << "line: " << line;
      if (boost::starts_with(line, "--")) {
        posEqual = line.find('=');
        if (posEqual == std::string::npos || posEqual == line.size() - 1) {
          continue;
        }
        posComment = line.find('#');
        if (posComment != std::string::npos) {
          line = line.substr(0, posComment);
        }
        key = trim(line.substr(2, posEqual - 2));
        val = trim(line.substr(posEqual + 1));
        configs_[key] = val;
        //LOG(INFO) << "[" << key << "]=" << val;
      } else if (boost::starts_with(line, "#")) {
        ;
      }
    }
  }

  bool has(const std::string &key) { return configs_.count(key); }

  std::string get(const std::string &key) {
    if (has(key)) {
      return configs_[key];
    }
    return "";
  }

private:
  std::string trim(std::string const &source, char const *delims = " \t\r\n") {
    std::string result(source);
    std::string::size_type index = result.find_last_not_of(delims);
    if (index != std::string::npos)
      result.erase(++index);

    index = result.find_first_not_of(delims);
    if (index != std::string::npos)
      result.erase(0, index);
    else
      result.erase();
    return result;
  }

  std::unordered_map<std::string, std::string> configs_;
};

} // namespace plato
