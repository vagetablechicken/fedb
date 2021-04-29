#include <brpc/channel.h>
#include <brpc/restful.h>
#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "api_server_impl.h"
#include "sdk/mini_cluster.h"
namespace fedb {
namespace http {

class APIServerTest : public ::testing::Test {
 public:
    APIServerTest() : mc_(new sdk::MiniCluster(6181)) {}
    ~APIServerTest() {}
    void SetUp() {
        ASSERT_TRUE(mc_->SetUp()) << "Fail to set up mini cluster";
        sdk::ClusterOptions option;
        option.zk_cluster = mc_->GetZkCluster();
        option.zk_path = mc_->GetZkPath();

        fedb::http::APIServerImpl queue_svc;
        ASSERT_TRUE(queue_svc.Init(option));

        // Http server set up
        ASSERT_TRUE(server_.AddService(&queue_svc, brpc::SERVER_DOESNT_OWN_SERVICE, "/demo => demo,") == 0)
            << "Fail to add queue_svc";

        // Start the server.
        brpc::ServerOptions options;
        // options.idle_timeout_sec = FLAGS_idle_timeout_s;
        ASSERT_TRUE(server_.Start(8010, &options) == 0) << "Fail to start HttpServer";
    }
    void TearDown() {
        server_.Stop(0);
        server_.Join();

        mc_->Close();
    }

 public:
    brpc::Server server_;
    std::unique_ptr<sdk::MiniCluster> mc_;
};

TEST_F(APIServerTest, demo) {
    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "http";
    options.timeout_ms = 2000 /*milliseconds*/;
    options.max_retry = 3;

    // Initialize the channel, NULL means using default options.
    // options, see `brpc/channel.h'.
    std::string url = "http://127.0.0.1:8010/demo";
    ASSERT_TRUE(channel.Init(url.c_str(), &options) == 0) << "Fail to initialize channel";

    // We will receive response synchronously, safe to put variables
    // on stack.
    brpc::Controller cntl;

    cntl.http_request().uri() = url;
    // if (!FLAGS_d.empty()) {
    //     cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    //     cntl.request_attachment().append(FLAGS_d);
    // }

    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timedout).
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    LOG(INFO) << cntl.response_attachment();
}

}  // namespace http
}  // namespace fedb

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    // ::fedb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
