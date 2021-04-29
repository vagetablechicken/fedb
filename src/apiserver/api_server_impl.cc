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

#include "api_server_impl.h"

#include <memory>

#include "brpc/server.h"
#include "interface_provider.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

namespace fedb {
namespace http {

using butil::rapidjson::Document;
using butil::rapidjson::StringBuffer;
using butil::rapidjson::Value;
using butil::rapidjson::Writer;

APIServiceImpl::~APIServiceImpl() = default;

bool APIServiceImpl::Init(const sdk::ClusterOptions& options) {
    // If cluster sdk is needed, use ptr, don't own it. SQLClusterRouter owns it.
    auto cluster_sdk = new ::fedb::sdk::ClusterSDK(options);
    bool ok = cluster_sdk->Init();
    if (!ok) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }

    auto router = std::make_unique<::fedb::sdk::SQLClusterRouter>(cluster_sdk);
    if (!router->Init()) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }
    sql_router_ = std::move(router);

    provider_.put("/db/:db_name/table/:table_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, http::Response* resp) {
                      auto db_it = param.find("db_name");
                      auto table_it = param.find("table_name");
                      auto db = db_it->second;
                      auto table = table_it->second;

                      // json2doc, then use schema, generate a sql
                      Document document;
                      if (document.Parse(req_body.to_string().c_str()).HasParseError()) {
                          SetErr(resp, "json2doc failed");
                          return;
                      }

                      hybridse::sdk::Status status;

                      const auto& value = document["value"];
                      if (!value.IsArray() || value.Empty() || value.Size() > 1) {
                          SetErr(resp, "invalid value");
                          return;
                      }
                      const auto& obj = value[0];

                      StringBuffer buffer;
                      Writer<StringBuffer> writer(buffer);
                      std::ostringstream col_ss, value_ss;
                      for (auto it = obj.MemberBegin(); it != obj.MemberEnd(); it++) {
                          if (value_ss.tellp() > 0) {  // not first round
                                                       //                col_ss << ",";
                              value_ss << ",";
                          }
                          //            it->name.Accept(writer); // TODO col_name can't be enclosed in double quotes
                          //            col_ss << buffer.GetString();
                          //            buffer.Clear();
                          // TODO convert func
                          it->value.Accept(writer);
                          value_ss << buffer.GetString();
                          buffer.Clear();
                      }
                      std::string insert_sql = "insert into " + table + " values(" + value_ss.str() + ");";
                      LOG(INFO) << "generate sql: " << insert_sql;
                      sql_router_->ExecuteInsert(db, insert_sql, &status);
                      SetOK(resp);
                  });

    provider_.post("/db/:db_name/procedure/:sp_name", [this](const InterfaceProvider::Params& param,
                                                             const butil::IOBuf& req_body, http::Response* resp) {
        auto db_it = param.find("db_name");
        auto sp_it = param.find("sp_name");
        auto db = db_it->second;
        auto sp = sp_it->second;

        Document document;
        if (document.Parse(req_body.to_string().c_str()).HasParseError()) {
            SetErr(resp, "parse2json failed");
            return;
        }

        hybridse::sdk::Status status;
        auto sp_info = sql_router_->ShowProcedure(db, sp, &status);
        if (!sp_info) {
            SetErr(resp, status.msg);
            return;
        }
        auto& input_schema = sp_info->GetInputSchema();
        const auto& schema_impl =
            dynamic_cast<const ::hybridse::sdk::SchemaImpl&>(input_schema);  // TODO catch(std::bad_cast)
        auto schema_copied = new hybridse::sdk::SchemaImpl(schema_impl.GetSchema());
        std::shared_ptr<hybridse::sdk::Schema> schema_shared;
        schema_shared.reset(schema_copied);

        auto common_column_indices = std::make_shared<fedb::sdk::ColumnIndicesSet>(schema_shared);
        for (int i = 0; i < input_schema.GetColumnCnt(); ++i) {
            if (input_schema.IsConstant(i)) {
                common_column_indices->AddCommonColumnIdx(i);
            }
        }

        auto common_cols_v = document.FindMember("common_cols");
        if (common_cols_v == document.MemberEnd()) {
            SetErr(resp, "no common_cols");
            return;
        }

        auto input = document.FindMember("input");
        if (input == document.MemberEnd() || !input->value.IsArray()) {
            resp->set_code(-1);
            return;
        }

        const auto& rows = input->value;
        auto row_batch = std::make_shared<fedb::sdk::SQLRequestRowBatch>(schema_shared, common_column_indices);
        std::set<std::string> col_set;
        for (decltype(rows.Size()) i = 0; i < rows.Size(); ++i) {
            if (!rows[i].IsArray()) {
                SetErr(resp, "invalid input data row");
                return;
            }

            auto row = std::make_shared<fedb::sdk::SQLRequestRow>(schema_shared, col_set);
            if (!Json2SQLRequestRow(rows[i], common_cols_v->value, row)) {
                resp->set_code(-1);
                return;
            }
            row->Build();
            row_batch->AddRow(row);
        }

        auto rs = sql_router_->CallSQLBatchRequestProcedure(db, sp, row_batch, &status);
        auto res = resp->mutable_exec_sp_resp();
        if (document.HasMember("need_schema") && document["need_schema"].IsBool() &&
            document["need_schema"].GetBool()) {
            auto schema = rs->GetSchema();
            for (int i = 0; i < schema->GetColumnCnt(); ++i) {
                auto s = res->add_schema();
                s->set_name(schema->GetColumnName(i));
                s->set_type(schema->GetColumnType(i));
            }
        }

        rs->Reset();
        while (rs->Next()) {
            auto add = res->add_data();
            for (int idx = 0; idx < rs->GetSchema()->GetColumnCnt(); idx++) {
                if (!rs->GetSchema()->IsConstant(idx)) {
                    std::string str = rs->GetAsString(idx);
                    add->add_value(str);
                }
            }
        }
        rs->Reset();
        if (rs->Size() > 0) {
            for (int idx = 0; idx < rs->GetSchema()->GetColumnCnt(); idx++) {
                if (rs->GetSchema()->IsConstant(idx)) {
                    res->add_common_cols_data(rs->GetAsString(idx));
                }
            }
        }

        SetOK(resp);
    });

    provider_.get("/db/:db_name/procedure/:sp_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, http::Response* resp) {
                      auto db = param.find("db_name");
                      auto sp = param.find("sp_name");
                      hybridse::sdk::Status status;
                      auto sp_info = sql_router_->ShowProcedure(db->second, sp->second, &status);
                      if (!sp_info) {
                          SetErr(resp, status.msg);
                          return;
                      }

                      auto res = resp->mutable_get_sp_resp();
                      res->set_name(sp_info->GetSpName());
                      res->set_procedure(sp_info->GetSql());

                      auto& input_schema = sp_info->GetInputSchema();
                      auto& output_schema = sp_info->GetOutputSchema();
                      for (auto i = 0; i < input_schema.GetColumnCnt(); ++i) {
                          auto add = res->add_input_schema();
                          add->set_name(input_schema.GetColumnName(i));
                          add->set_type(input_schema.GetColumnType(i));
                          if (input_schema.IsConstant(i)) {
                              res->add_input_common_cols(input_schema.GetColumnName(i));
                          }
                      }
                      for (auto i = 0; i < output_schema.GetColumnCnt(); ++i) {
                          auto add = res->add_output_schema();
                          add->set_name(output_schema.GetColumnName(i));
                          add->set_type(output_schema.GetColumnType(i));
                          if (input_schema.IsConstant(i)) {
                              res->add_output_common_cols(output_schema.GetColumnName(i));
                          }
                      }

                      for (const auto& table : sp_info->GetTables()) {
                          res->add_tables(table);
                      }

                      SetOK(resp);
                  });

    return true;
}

void APIServiceImpl::Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = dynamic_cast<brpc::Controller*>(cntl_base);

    // The unresolved path has no slashes at the beginning, it's not good for url parsing
    auto unresolved_path = "/" + cntl->http_request().unresolved_path();
    auto method = cntl->http_request().method();
    LOG(INFO) << "unresolved path: " << unresolved_path << ", method: " << HttpMethod2Str(method);
    const butil::IOBuf& req_body = cntl->request_attachment();
    http::Response resp;
    if (!provider_.handle(unresolved_path, method, req_body, &resp)) {
        return;
    }

    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    std::string json_resp;
    // TODO ZeroCopyOutputStream
    std::string err;
    json2pb::ProtoMessageToJson(resp, &json_resp, json_options, &err);
    if (!err.empty()) {
        LOG(WARNING) << err;
    }
    cntl->response_attachment().append(json_resp);
}

bool APIServiceImpl::Json2SQLRequestRow(const butil::rapidjson::Value& input,
                                        const butil::rapidjson::Value& common_cols_v,
                                        std::shared_ptr<fedb::sdk::SQLRequestRow> row) {
    auto sch = row->GetSchema();
    int non_common_idx = 0, common_idx = 0;
    for (decltype(sch->GetColumnCnt()) i = 0; i < sch->GetColumnCnt(); ++i) {
        // TODO no need to append common cols
        if (sch->IsConstant(i)) {
            AppendJsonValue(common_cols_v[common_idx], row->GetSchema()->GetColumnType(i), row);
            common_idx++;
        } else {
            AppendJsonValue(input[non_common_idx], row->GetSchema()->GetColumnType(i), row);
            non_common_idx++;
        }
    }
    return true;
}
bool APIServiceImpl::AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type,
                                     std::shared_ptr<fedb::sdk::SQLRequestRow> row) {
    switch (type) {
        case hybridse::sdk::kTypeBool:
            row->AppendBool(v.GetBool());
            break;
        case hybridse::sdk::kTypeInt16:
            row->AppendInt16(v.GetInt());  // TODO cast
            break;
        case hybridse::sdk::kTypeInt32:
            row->AppendInt32(v.GetInt());
            break;
        case hybridse::sdk::kTypeInt64:
            row->AppendInt64(v.GetInt64());
            break;
        case hybridse::sdk::kTypeFloat:
            row->AppendFloat(v.GetDouble());  // TODO cast
            break;
        case hybridse::sdk::kTypeDouble:
            row->AppendDouble(v.GetDouble());
            break;
        case hybridse::sdk::kTypeString:
            row->Init(v.GetStringLength());  // TODO cast
            row->AppendString(v.GetString());
            break;
        case hybridse::sdk::kTypeDate:
            LOG(INFO) << v.GetString() << " convert not supported";
            row->AppendDate(0);
            break;
        case hybridse::sdk::kTypeTimestamp:
            row->AppendTimestamp(v.GetInt64());
            break;
        default:
            return false;
    }
    return true;
}
}  // namespace http
}  // namespace fedb
