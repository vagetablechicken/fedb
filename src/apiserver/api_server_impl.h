
#include "proto/http.pb.h"
#include "sdk/sql_cluster_router.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);

namespace fedb {
namespace http {
class APIServerImpl : public QueueService {
 public:
    APIServerImpl(){};
    virtual ~APIServerImpl();
    bool Init(const sdk::ClusterOptions& options);
    void demo(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
              google::protobuf::Closure* done);

 private:
    std::unique_ptr<sdk::ClusterSDK> cluster_sdk_;
    std::unique_ptr<sdk::SQLRouter> sql_router_;
};
}  // namespace http
}  // namespace fedb