
#include "proto/http.pb.h"

namespace fedb {
namespace http {
class APIServerImpl : public QueueService {
    APIServerImpl(){};
    virtual ~APIServerImpl(){};
    void demo(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
              google::protobuf::Closure* done);
};
}  // namespace http
}  // namespace fedb