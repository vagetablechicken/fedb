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

bool BulkLoadMgr::DataAppend(uint32_t tid, uint32_t pid, const ::openmldb::api::BulkLoadRequest* request,
                             const butil::IOBuf& data) {
    if (!request->has_data_part_id() || data.empty()) {
        return false;
    }
    auto data_part_id = request->data_part_id();
    auto data_receiver = GetDataReceiver(tid, pid, data_part_id == 0);
    if (!data_receiver) {
        return false;
    }

    return false;
}

bool BulkLoadMgr::WriteBinlogToReplicator(
    uint32_t tid, uint32_t pid, std::shared_ptr<replica::LogReplicator> replicator,
    const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
    auto data_receiver = GetDataReceiver(tid, pid, false);
    if (!data_receiver) {
        return false;
    }

    return false;
}

std::shared_ptr<DataReceiver> BulkLoadMgr::GetDataReceiver(uint32_t tid, uint32_t pid, bool create) {
    std::shared_ptr<DataReceiver> data_receiver = nullptr;
    do {
        std::unique_lock<std::mutex> ul(catalog_mu_);
        auto table_cat_iter = catalog_.find(tid);
        if (table_cat_iter == catalog_.end()) {
            if (!create) {
                break;
            }
            // tid-pid-DataReceiver
            data_receiver.reset(new DataReceiver(tid, pid));
            auto pid_cat = decltype(catalog_)::mapped_type();
            pid_cat[pid] = data_receiver;
            catalog_[tid] = pid_cat;
            break;
        }
        auto pid_cat = table_cat_iter->second;
        auto iter = pid_cat.find(pid);
        if (iter == pid_cat.end()) {
            if (!create) {
                break;
            }
            data_receiver.reset(new DataReceiver(tid, pid));
            pid_cat[pid] = data_receiver;
            break;
        }

        // catalog has the receiver for tid-pid, we treat it as error. // TODO(hw): or should treat it as covering?
        if (create) {
            DLOG(INFO) << "already has receiver for " << tid << "-" << pid << ", but want to create a new one";
            break;
        }
        data_receiver = iter->second;
    } while (false);
    return data_receiver;
}
}  // namespace openmldb::tablet