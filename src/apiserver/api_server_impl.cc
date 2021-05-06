#include "api_server_impl.h"

#include <brpc/server.h>

namespace fedb {
namespace http {
//APIServerImpl::APIServerImpl(const sdk::ClusterOptions& options): cluster_sdk_(options) {
//
//}
APIServerImpl::~APIServerImpl() {
    sql_router_.reset();
}

bool APIServerImpl::Init(const sdk::ClusterOptions& options) {
    cluster_sdk_.reset(new ::fedb::sdk::ClusterSDK(options));
    bool ok = cluster_sdk_->Init();
    if (!ok) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }

    auto router = std::make_unique<::fedb::sdk::SQLClusterRouter>(cluster_sdk_.get());
    if (!router->Init()) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }
    sql_router_ = std::move(router);
    return true;
}

void APIServerImpl::demo(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                         google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->response_attachment().append("nice");

    LOG(INFO) << "get path: " << cntl->http_request().uri().path();

}

}  // namespace http
}  // namespace fedb
