#include "api_server_impl.h"

namespace fedb {
namespace http {
void APIServerImpl::demo(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                         google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->response_attachment().append("nice");
}
}  // namespace http
}  // namespace fedb
