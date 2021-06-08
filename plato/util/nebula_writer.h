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

#ifndef __PLATO_UTIL_NEBULA_WRITER_HPP__
#define __PLATO_UTIL_NEBULA_WRITER_HPP__

#include <cstdint>
#include <cstdlib>
#include <string>
#include <memory>

#include "glog/logging.h"
#include "boost/format.hpp"
#include "boost/algorithm/string.hpp"
#include "nebula/client/Config.h"
#include "nebula/client/ConnectionPool.h"
#include "nebula/client/Init.h"

#include "plato/graph/base.hpp"
#include "plato/util/configs.hpp"
#include "thread_local_object.h"

namespace plato {


template<typename ITEM>
struct buffer {
  size_t capacity_;
  nebula::Session* session_;
  const std::string& prefix_;
  std::function<nebula::ErrorCode()> write_batch_;
  std::vector<ITEM> items_;
  void add(const ITEM& item) {
    items_.emplace_back(item);
    if (items_.size() > capacity_) {
      write_batch_();
      items_.clear();
    }
  }
  buffer(size_t capacity, nebula::Session* session, const std::string& prefix) : capacity_(capacity), session_(session), prefix_(prefix) {
    items_.reserve(capacity);
    write_batch_ = [&]() {
      auto stmt = genStmt();
      auto result = session_->execute(stmt);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
          LOG(INFO) << "session execute failed, errorcode: " << static_cast<int>(result.errorCode) << " statment: " << prefix;
      }
      return result.errorCode;
    };
  }
  std::string genStmt() {
    std::string stmt(prefix_);
    for (auto &item : items_) {
      stmt += item.toString();
      stmt += ',';
    }
    stmt.back() = ';';

    return stmt;
  };
};

class thread_local_nebula_writer {
public:
  thread_local_nebula_writer(const thread_local_nebula_writer&) = delete;
  thread_local_nebula_writer& operator=(const thread_local_nebula_writer&) = delete;
  thread_local_nebula_writer(thread_local_nebula_writer&& x) noexcept : id_(x.id_) {
    x.id_ = -1;
  }
  thread_local_nebula_writer& operator=(thread_local_nebula_writer &&x) noexcept {
    if (this != &x) {
      this->~thread_local_nebula_writer();
      new(this) thread_local_nebula_writer(std::move(x));
    }
    return *this;
  }

  thread_local_nebula_writer(const std::string& path) {
    CHECK(boost::starts_with(path, "nebula:")) << "it's not a nebula config file";
    Configs configs(path, "nebula:");
    std::string graph_server_addrs_val, user_val, password_val, mode_val, tag_val, props_val;
    CHECK((graph_server_addrs_val = configs.get("graph_server_addrs")) != "") << "graph_server_addrs doesn't exist.";
    CHECK((user_val = configs.get("user")) != "") << "user doesn't exist.";
    CHECK((password_val = configs.get("password")) != "") << "password doesn't exist.";
    CHECK((mode_val = configs.get("mode")) != "") << "props doesn't exist.";
    CHECK((tag_val = configs.get("tag")) != "") << "tag doesn't exist.";
    CHECK((props_val = configs.get("props")) != "") << "props doesn't exist.";
  
    std::vector<std::string> graphServers;
    boost::split(graphServers, graph_server_addrs_val, boost::is_any_of(","), boost::token_compress_on);

    std::vector<std::string> props;
    boost::split(props, props_val, boost::is_any_of(","), boost::token_compress_on);

    //if (mode == Mode::INSERT) {
    stmtPrefix_ = genInsertStmtPrefix(tag_val, props);
    //}

    auto& cluster_info = cluster_info_t::get_instance();

    nebula::ConnectionPool pool;
    nebula::Config poolConfig;
    poolConfig.maxConnectionPoolSize_ = cluster_info.threads_;
    pool.init(graphServers, poolConfig);

    std::function<void*()> construction([&pool, &user_val, &password_val] {
      nebula::Session *session_ = new nebula::Session(pool.getSession(user_val, password_val));
      if (!session_->valid()) {
          return (void*)nullptr;
      }
      if (!session_->ping()) {
        return (void*)nullptr;
      }

      return (void*)session_;
    });

    std::function<void(void *)> destruction([] (void* p) {
      auto *session_ = (nebula::Session*)p;
      session_->release();
      CHECK(!session_->valid()) << "release nebula session failed";
      delete session_;
    });

    id_ = thread_local_object_detail::create_object(std::move(construction), std::move(destruction));
    if (-1 == id_) throw std::runtime_error("thread_local_object_detail::create_object failed.");
  }

  ~thread_local_nebula_writer() {
    if (id_ != -1) {
      thread_local_object_detail::delete_object(id_);
      id_ = -1;
    }
  }

  void foreach(std::function<void(const std::string& filename, boost::iostreams::filtering_ostream& os)> reducer) {};

  template<typename ITEM>
  [[gnu::always_inline]] [[gnu::hot]]
  buffer<ITEM>& local() {
    nebula::Session* session_ = (nebula::Session*)thread_local_object_detail::get_local_object(id_);
    CHECK(session_ && session_->valid()) << "session is not valid";
    thread_local buffer<ITEM> buff_(1000, session_, stmtPrefix_);

    return buff_;
  }

private:
  std::string genInsertStmtPrefix(const std::string& tag, const std::vector<std::string>& props) {
    std::string prefix = "INSERT VERTEX %s" + tag + "(";
    for (auto &prop : props) {
      prefix += prop;
      prefix += ",";
    }
    prefix.back() = ')';
    prefix += " VALUES ";

    return prefix;
  }

protected:

  enum class Mode {
    INSERT,
    UPDATE
  };

  int id_;

  std::string stmtPrefix_;
};

}  // namespace plato

#endif

