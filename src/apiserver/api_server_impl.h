/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <base/texttable.h>  // TEST

#include "interface_provider.h"
#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API
#include "proto/http.pb.h"
#include "sdk/sql_cluster_router.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);

namespace fedb {
namespace http {

class APIServiceImpl : public APIService {
 public:
    APIServiceImpl() = default;
    ~APIServiceImpl() override;
    bool Init(const sdk::ClusterOptions& options);
    void Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                 google::protobuf::Closure* done) override;

 private:
    inline void SetOK(Response* resp) {
        resp->set_code(0);
        resp->set_msg("ok");
    }
    inline void SetErr(Response* resp, const std::string& msg) {
        resp->set_code(-1);
        resp->set_msg(msg);
    }

    static bool Json2SQLRequestRow(const butil::rapidjson::Value& input, const butil::rapidjson::Value& common_cols_v,
                                   std::shared_ptr<fedb::sdk::SQLRequestRow> row);
    static bool AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type,
                                std::shared_ptr<fedb::sdk::SQLRequestRow> row);

 private:
    std::unique_ptr<sdk::SQLRouter> sql_router_;
    InterfaceProvider provider_;
};

}  // namespace http
}  // namespace fedb