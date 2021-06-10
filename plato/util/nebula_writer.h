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
  std::string prefix_;
  std::function<nebula::ErrorCode()> flush_;
  std::vector<ITEM> items_;

  buffer(size_t capacity, nebula::Session* session, const std::string& prefix) : capacity_(capacity), session_(session), prefix_(prefix) {
    LOG(INFO) << "buffer(), prefix_: " << prefix_;
    items_.reserve(capacity);
    flush_ = [&]() {
      auto stmt = genStmt();
      LOG(INFO) << "stmt: " << stmt;
      CHECK(session_->valid()) << "session_ not valid";
      CHECK(session_->ping()) << "session ping failed";
      auto result = session_->execute(stmt);
      if (result.errorCode != nebula::ErrorCode::SUCCEEDED) {
          LOG(INFO) << "session execute failed, statment: " << stmt << "\nerrorCode: " << static_cast<int>(result.errorCode) << ", errorMsg: " << *result.errorMsg;
      }
      // flush succeeded
      items_.clear();
      return result.errorCode;
    };
  }

  ~buffer() {
    delete session_;
    CHECK(!session_->valid()) << "release nebula session failed";
    session_ = nullptr;

    CHECK(items_.empty()) << "items_ is not empty when destroying buffer";
  }

  void add(const ITEM& item) {
    LOG(INFO) << "Buffer added an item: " << item.toString();
    items_.emplace_back(item);
    if (items_.size() > capacity_) {
      LOG(INFO) << "write batch...";
      flush_();
      items_.clear();
    }
  }

  std::string genStmt() {
    std::string stmt(prefix_);
    LOG(INFO) << "genstmt: " << stmt;
    for (auto &item : items_) {
      stmt += item.toString();
      stmt += ',';
    }
    stmt.back() = ';';

    return stmt;
  };
};

template<typename ITEM>
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
    std::string graph_server_addrs_val, user_val, password_val, mode_val, space_val, tag_val, props_val;
    CHECK((graph_server_addrs_val = configs.get("graph_server_addrs")) != "") << "graph_server_addrs doesn't exist.";
    CHECK((user_val = configs.get("user")) != "") << "user doesn't exist.";
    CHECK((password_val = configs.get("password")) != "") << "password doesn't exist.";
    CHECK((mode_val = configs.get("mode")) != "") << "props doesn't exist.";
    CHECK((space_val = configs.get("space")) != "") << "space doesn't exist";
    CHECK((tag_val = configs.get("tag")) != "") << "tag doesn't exist.";
    CHECK((props_val = configs.get("props")) != "") << "props doesn't exist.";
  
    std::vector<std::string> graphServers;
    boost::split(graphServers, graph_server_addrs_val, boost::is_any_of(","), boost::token_compress_on);

    std::vector<std::string> props;
    boost::split(props, props_val, boost::is_any_of(","), boost::token_compress_on);

    //if (mode == Mode::INSERT) {
    genInsertStmtPrefix(tag_val, props);
    //}

    auto& cluster_info = cluster_info_t::get_instance();

    pool_ = new nebula::ConnectionPool();
    nebula::Config poolConfig;
    poolConfig.maxConnectionPoolSize_ = cluster_info.threads_;
    pool_->init(graphServers, poolConfig);

    std::function<void*()> construction([this, user_val, password_val, space_val] {
      nebula::Session *session = new nebula::Session(this->pool_->getSession(user_val, password_val));
      CHECK(session) << "session is nullptr";
      CHECK(session->valid()) << "session is not valid";
      CHECK(session->ping()) << "session ping failed";
      session->execute("USE " + space_val);

      auto *buff_ = new buffer<ITEM>(1000, session, this->stmtPrefix_);

      return (void*)buff_;
    });

    std::function<void(void *)> destruction([] (void* p) {
      auto *buff_ = (buffer<ITEM>*)p;
      if (!buff_->items_.empty()) {
        LOG(INFO) << "flush buff_...";
        buff_->flush_();
      }
      delete buff_;
    });

    id_ = thread_local_object_detail::create_object(std::move(construction), std::move(destruction));
    LOG(INFO) << "thread_local_object_detail::create_object, id_: " << id_;
    if (-1 == id_) throw std::runtime_error("thread_local_object_detail::create_object failed.");
  }

  ~thread_local_nebula_writer() {
    LOG(INFO) << "start call ~thread_local_nebula_writer";
    if (id_ != -1) {
      thread_local_object_detail::delete_object(id_);
      id_ = -1;
    }
    if (pool_) {
      delete pool_;
      pool_ = nullptr;
    }
    LOG(INFO) << "finish call ~thread_local_nebula_writer";
  }

  void foreach(std::function<void(const std::string& filename, boost::iostreams::filtering_ostream& os)> reducer) {};

  [[gnu::always_inline]] [[gnu::hot]]
  buffer<ITEM>& local() {
    return *((buffer<ITEM>*)thread_local_object_detail::get_local_object(id_));
  }

private:
  void genInsertStmtPrefix(const std::string& tag, const std::vector<std::string>& props) {
    stmtPrefix_ = "INSERT VERTEX " + tag + "(";
    for (auto &prop : props) {
      stmtPrefix_ += prop;
      stmtPrefix_ += ",";
    }
    stmtPrefix_.back() = ')';
    stmtPrefix_ += " VALUES ";
    LOG(INFO) << "stmtPrefix_: " << stmtPrefix_;
  }

protected:

  enum class Mode {
    INSERT,
    UPDATE
  };

  int id_;

  nebula::ConnectionPool *pool_;

  std::string stmtPrefix_;
};

}  // namespace plato

#endif

