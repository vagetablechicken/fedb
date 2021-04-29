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

#include <brpc/channel.h>
#include <brpc/restful.h>
#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "api_server_impl.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/rapidjson.h"
#include "sdk/mini_cluster.h"

namespace fedb {
namespace http {

class APIServerTest : public ::testing::Test {
 public:
    APIServerTest() : mc_(new sdk::MiniCluster(6181)), queue_svc_(new APIServiceImpl) {}
    ~APIServerTest() {}
#if 1
    void SetUp() {
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        FLAGS_zk_session_timeout = 100000;
        ASSERT_TRUE(mc_->SetUp()) << "Fail to set up mini cluster";

        sdk::ClusterOptions cluster_options;
        cluster_options.zk_cluster = mc_->GetZkCluster();
        cluster_options.zk_path = mc_->GetZkPath();

        ASSERT_TRUE(queue_svc_->Init(cluster_options));

        // Http server set up
        ASSERT_TRUE(server_.AddService(queue_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE, "/* => Process") == 0)
            << "Fail to add queue_svc";

        // Start the server.
        int api_server_port = 8010;
        brpc::ServerOptions server_options;
        // options.idle_timeout_sec = FLAGS_idle_timeout_s;
        ASSERT_TRUE(server_.Start(api_server_port, &server_options) == 0) << "Fail to start HttpServer";

        sdk::SQLRouterOptions sql_opt;
        sql_opt.session_timeout = 30000;
        sql_opt.zk_cluster = mc_->GetZkCluster();
        sql_opt.zk_path = mc_->GetZkPath();
        //        sql_opt.enable_debug = true;
        cluster_remote_ = sdk::NewClusterSQLRouter(sql_opt);
        hybridse::sdk::Status status;
        ASSERT_TRUE(cluster_remote_ != nullptr);
        db_ = "api_server_test";
        (cluster_remote_->DropDB(db_, &status));
        (cluster_remote_->CreateDB(db_, &status));
        std::vector<std::string> dbs;
        ASSERT_TRUE(cluster_remote_->ShowDB(&dbs, &status));
        ASSERT_TRUE(std::find(dbs.begin(), dbs.end(), db_) != dbs.end());

        brpc::ChannelOptions options;
        options.protocol = "http";
        options.timeout_ms = 2000 /*milliseconds*/;
        options.max_retry = 3;
        ASSERT_TRUE(http_channel_.Init("http://127.0.0.1:", api_server_port, &options) == 0)
            << "Fail to initialize http channel";
    }
    void TearDown() {
        hybridse::sdk::Status status;
        EXPECT_TRUE(cluster_remote_->DropDB(db_, &status));
        server_.Stop(0);
        server_.Join();
        queue_svc_.reset();
        mc_->Close();
    }
#endif
 public:
    std::string db_;
    std::unique_ptr<sdk::MiniCluster> mc_;
    std::unique_ptr<APIServiceImpl> queue_svc_;
    brpc::Server server_;
    brpc::Channel http_channel_;
    std::shared_ptr<sdk::SQLRouter> cluster_remote_;
};

TEST_F(APIServerTest, json) {
    butil::rapidjson::Document document;
    // req_body.to_string().c_str()
    if (document
            .Parse(R"(
{
    "value": [
        {
            "field1": "value1",
            "field2": 111,
            "field3": 1.4,
            "field4": "2021-04-27",
            "field5": 1620471840256,
            "field6": true
        }
    ]
}
)")
            .HasParseError()) {
        LOG(FATAL) << "";
    }

    hybridse::sdk::Status status;
    const auto& value = document["value"];
    ASSERT_TRUE(value.IsArray());
    ASSERT_EQ(1, value.Size());
    const auto& obj = value[0];
    static const char* kTypeNames[] = {"Null", "False", "True", "Object", "Array", "String", "Number"};

    for (auto it = obj.MemberBegin(); it != obj.MemberEnd(); ++it) {
        LOG(INFO) << "Type of member " << it->name.GetString() << " is " << kTypeNames[it->value.GetType()];
    }

    butil::rapidjson::StringBuffer buffer;
    butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(buffer);
    std::ostringstream col_ss, value_ss;
    for (auto it = obj.MemberBegin(); it != obj.MemberEnd(); it++) {
        if (col_ss.tellp() > 0) {  // not first round
            col_ss << ",";
            value_ss << ",";
        }
        it->name.Accept(writer);
        col_ss << buffer.GetString();
        buffer.Clear();
        it->value.Accept(writer);
        value_ss << buffer.GetString();
        buffer.Clear();
    }
    std::string insert_sql = "insert into t1 (" + col_ss.str() + ") values( " + value_ss.str() + ");";
    LOG(INFO) << insert_sql;
}

TEST_F(APIServerTest, put) {
    // create table
    std::string table = "put";
    std::string ddl = "create table " + table +
                      "(field1 string, "
                      "field2 timestamp, "
                      "field3 double, "
                      "field4 date, "
                      "field5 bigint, "
                      "field6 bool,"
                      "index(key=field1, ts=field2));";
    hybridse::sdk::Status status;
    EXPECT_TRUE(cluster_remote_->ExecuteDDL(db_, ddl, &status)) << status.msg;
    ASSERT_TRUE(cluster_remote_->RefreshCatalog());
    std::map<uint32_t, std::vector<std::string>> key_map;
    for (int i = 0; i < 10; i++) {
        std::string key = "value" + std::to_string(i);

        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/db/" + db_ + "/table/" + table;
        cntl.request_attachment().append(
            "{\n"
            "    \"value\": [{\n"
            "        \"field1\": \"" +
            key +
            "\",\n"
            "        \"field2\": 111,\n"
            "        \"field3\": 1.4,\n"
            "        \"field4\": \"2021-04-27\",\n"
            "        \"field5\": 1620471840256,\n"
            "        \"field6\": true\n"
            "    }]\n"
            "\n"
            "}");
        http_channel_.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        http::Response resp;
        json2pb::JsonToProtoMessage(cntl.response_attachment().to_string(), &resp);
        ASSERT_EQ(0, resp.code());
    }
    ASSERT_TRUE(cluster_remote_->ExecuteDDL(db_, "drop table " + table + ";", &status)) << status.msg;
}

TEST_F(APIServerTest, procedure) {
    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    hybridse::sdk::Status status;
    cluster_remote_->ExecuteDDL(db_, "drop table trans;", &status);
    ASSERT_TRUE(cluster_remote_->RefreshCatalog());
    ASSERT_TRUE(cluster_remote_->ExecuteDDL(db_, ddl, &status)) << "fail to create table";

    ASSERT_TRUE(cluster_remote_->RefreshCatalog());
    // insert
    std::string insert_sql = "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(cluster_remote_->ExecuteInsert(db_, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    ASSERT_TRUE(cluster_remote_->ExecuteDDL(db_, sp_ddl, &status)) << "fail to create procedure";
    ASSERT_TRUE(cluster_remote_->RefreshCatalog());

    http::Response resp;

    // show procedure
    brpc::Controller show_cntl;  // default is GET
    show_cntl.http_request().uri() = "http://127.0.0.1:8010/db/" + db_ + "/procedure/" + sp_name;
    http_channel_.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
    ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
    LOG(INFO) << "get sp resp: " << show_cntl.response_attachment();

    ASSERT_TRUE(json2pb::JsonToProtoMessage(show_cntl.response_attachment().to_string(), &resp));
    ASSERT_EQ(0, resp.code());
    ASSERT_TRUE(resp.has_get_sp_resp());
    auto sp_info = resp.get_sp_resp();
    ASSERT_EQ(3, sp_info.input_common_cols().size());

    // call procedure
    brpc::Controller cntl;
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.http_request().uri() = "http://127.0.0.1:8010/db/" + db_ + "/procedure/" + sp_name;
    cntl.request_attachment().append(R"(
{
    "common_cols":["bb", 23, 1590738994000],
    "input": [[123, 5.1, 6.1, "2021-08-01"],[234, 5.2, 6.2, "2021-08-02"]],
    "need_schema": true
})");
    http_channel_.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();
    ASSERT_TRUE(json2pb::JsonToProtoMessage(cntl.response_attachment().to_string(), &resp));

    ASSERT_EQ(0, resp.code());
    ASSERT_TRUE(resp.has_exec_sp_resp());

    // TODO check resp data

    // drop procedure
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(cluster_remote_->ExecuteDDL(db_, drop_sp_sql, &status));
    ASSERT_TRUE(cluster_remote_->ExecuteDDL(db_, "drop table trans;", &status));
}

}  // namespace http
}  // namespace fedb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
