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

#include "bulk_load_mgr.h"

namespace openmldb::tablet {

bool BulkLoadMgr::DataAppend(uint32_t tid, uint32_t pid, int data_part_id,
                             const ::openmldb::api::BulkLoadRequest* request, const butil::IOBuf& data) {
    std::unique_lock<std::mutex> ul(catalog_mu_);
    std::shared_ptr<DataReceiver> data_receiver = nullptr;
    auto table_cat = catalog_.find(tid);
    if (table_cat == catalog_.end()) {
        if (data_part_id != 0) {
            return false;
        }
        table_cat = catalog_[tid];
    }

    return false;
}

bool BulkLoadMgr::WriteBinlogToReplicator(
    uint32_t tid, uint32_t pid, std::shared_ptr<replica::LogReplicator> replicator,
    const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
    // TODO(hw): check tid pid

    return false;
}
}  // namespace openmldb::tablet