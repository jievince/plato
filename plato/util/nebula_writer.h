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
#include <memory>
#include <string>

#include "glog/logging.h"
#include "boost/algorithm/string.hpp"
#include "boost/format.hpp"
#include "nebula/client/Config.h"
#include "nebula/client/ConnectionPool.h"
#include "nebula/client/Init.h"

#include "plato/graph/base.hpp"
#include "plato/util/configs.hpp"
#include "thread_local_object.h"

namespace plato {

namespace nebula_writer_configs_detail {

static std::string user_;
static std::string password_;
static std::string space_;
static std::string mode_;
static int retry_;
static std::string err_file_;
static std::string tag_;
static std::vector<std::string> props_;

} // namespace nebula_writer_configs_detail

bool check_response(const nebula::ExecutionResponse &resp,
                    const std::string &stmt) {
  if (resp.errorCode != nebula::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "session execute failed, statment: " << stmt
              << "\nerrorCode: " << static_cast<int>(resp.errorCode)
              << ", errorMsg: " << *resp.errorMsg;
    return false;
  }

  return true;
}

template <typename ITEM>
struct Buffer {
  size_t capacity_;
  nebula::Session *session_;
  std::function<int()> flush_;
  std::vector<ITEM> items_;

  Buffer(size_t capacity, nebula::Session *session)
      : capacity_(capacity), session_(session) {
    items_.reserve(capacity);
    flush_ = [&]() {
      LOG(INFO) << "flush_, items_.size()=" << items_.size();
      auto stmt = genStmt();
      CHECK(!!session_) << "session_ is nullptr";
      int retry = nebula_writer_configs_detail::retry_;
      while (retry--) {
        auto result = session_->execute(stmt);
        if (check_response(result, stmt)) {
          break;
        }
      }
      if (retry < 0) {
        // write to err_file
        ;
      }
      // flush succeeded
      items_.clear();
      return 0;
    };
  }

  ~Buffer() {
    delete session_;
    session_ = nullptr;
    CHECK(items_.empty()) << "items_ is not empty when destroying Buffer";
  }

  void add(const ITEM &item) {
    items_.emplace_back(item);
    if (items_.size() >= capacity_) {
      flush_();
      items_.clear();
    }
  }

  std::string genStmt() {
    if (items_.empty()) {
      return "";
    }

    if (boost::iequals(nebula_writer_configs_detail::mode_, "insert")) {
      std::string stmt = "INSERT VERTEX " + nebula_writer_configs_detail::tag_ + "(";
      for (auto &prop : nebula_writer_configs_detail::props_) {
        stmt += prop;
        stmt += ",";
      }
      stmt.back() = ')';
      stmt += " VALUES ";

      for (auto &item : items_) {
        stmt += std::to_string(item.vid);
        stmt += ":(";
        stmt += item.toString();
        stmt += "),";
      }
      stmt.back() = ';';

      return stmt;
    } else if (boost::iequals(nebula_writer_configs_detail::mode_, "update")) {
      std::string stmt;
      for (auto &item : items_) {
        stmt += "UPDATE VERTEX ON " + nebula_writer_configs_detail::tag_ + " ";
        stmt += std::to_string(item.vid);
        stmt += " SET ";
        for (auto &prop: nebula_writer_configs_detail::props_) {
          stmt += prop;
          stmt += " = ";
          stmt += item.toString();
          stmt += ",";
        }
        stmt.back() = ';';
      }

      return stmt;
    } else {
      LOG(FATAL) << "invalid mode: " << nebula_writer_configs_detail::mode_;
    }

    return "";
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

  thread_local_nebula_writer(const std::string &path) {
    Configs configs(path, "nebula:");
    std::string graph_server_addrs, user, password, mode, space, tag, props, retry, err_file;
    CHECK((graph_server_addrs = configs.get("graph_server_addrs")) != "")
        << "graph_server_addrs doesn't exist.";
    CHECK((user = configs.get("user")) != "") << "user doesn't exist.";
    CHECK((password = configs.get("password")) != "")
        << "password doesn't exist.";
    CHECK((space = configs.get("space")) != "") << "space doesn't exist";
    CHECK((mode = configs.get("mode")) != "") << "props doesn't exist.";
    CHECK((tag = configs.get("tag")) != "") << "tag doesn't exist.";
    CHECK((props = configs.get("props")) != "") << "props doesn't exist.";
    CHECK((retry = configs.get("retry")) != "") << "retry doesn't exist.";
    CHECK((err_file = configs.get("err_file")) != "") << "err_file doesn't exist.";

    std::vector<std::string> graphServers;
    boost::split(graphServers, graph_server_addrs, boost::is_any_of(","),
                 boost::token_compress_on);

    std::vector<std::string> tagProps;
    boost::split(tagProps, props, boost::is_any_of(","),
                 boost::token_compress_on);

    auto &cluster_info = cluster_info_t::get_instance();

    pool_ = new nebula::ConnectionPool();
    nebula::Config poolConfig;
    poolConfig.maxConnectionPoolSize_ = cluster_info.threads_;
    pool_->init(graphServers, poolConfig);

    nebula_writer_configs_detail::user_ = user;
    nebula_writer_configs_detail::password_ = password;
    nebula_writer_configs_detail::space_ = space;
    nebula_writer_configs_detail::mode_ = mode;
    nebula_writer_configs_detail::tag_ = tag;
    nebula_writer_configs_detail::props_ = tagProps;
    nebula_writer_configs_detail::retry_ = stoi(retry);
    nebula_writer_configs_detail::err_file_ = err_file;

    std::function<void *()> construction(
        [this] {
          nebula::Session *session =
              new nebula::Session(this->pool_->getSession(nebula_writer_configs_detail::user_, nebula_writer_configs_detail::password_));
          CHECK(session) << "session is nullptr";
          CHECK(session->valid()) << "session is not valid";
          CHECK(session->ping()) << "session ping failed";
          std::string stmt;
          nebula::ExecutionResponse result;

          stmt = "USE " + nebula_writer_configs_detail::space_;
          result = session->execute(stmt);
          CHECK(check_response(result, stmt));

          stmt = "DESC TAG " + nebula_writer_configs_detail::tag_;
          result = session->execute(stmt);
          CHECK(check_response(result, stmt));

          auto *buff_ = new Buffer<ITEM>(1000, session);

          return (void *)buff_;
        });

    std::function<void(void *)> destruction([](void *p) {
      auto *buff_ = (Buffer<ITEM> *)p;
      if (!buff_->items_.empty()) {
        LOG(INFO) << "flush_ in desctruction, items_.size()=" << buff_->items_.size();
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
  Buffer<ITEM> &local() {
    return *((Buffer<ITEM> *)thread_local_object_detail::get_local_object(id_));
  }

protected:
  int id_;

  nebula::ConnectionPool *pool_;
};

} // namespace plato

#endif
