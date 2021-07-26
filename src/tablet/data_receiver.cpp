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

#include "data_receiver.h"

namespace openmldb {
namespace tablet {
bool DataReceiver::DataAppend(const ::openmldb::api::BulkLoadRequest* request,
                              const butil::IOBuf& data) {
    // TODO(hw): check part id in request
    // We must copy data from IOBuf, cuz the rows have different TTLs, it's not a good idea to keep them together.
    butil::IOBufBytesIterator iter(data);
    std::vector<DataBlock*> data_blocks(request->block_info_size());
    for (int i = 0; i < request->block_info_size(); ++i) {
        const auto& info = request->block_info(i);
        auto buf = new char[info.length()];
        iter.copy_and_forward(buf, info.length());
        data_blocks[i] = new DataBlock(info.ref_cnt(), buf, info.length(), true);
        DLOG(INFO) << "bulk load request(data block) len " << info.length();
    }
    if (iter.bytes_left() != 0) {
        // TODO(hw): error
        PDLOG(WARNING, "data info mismatch");
        response->set_code(::openmldb::base::ReturnCode::kReceiveDataError);
        return;
    }
    DLOG(INFO) << "data_blocks size: " << data_blocks.size();
    return false;
}

bool DataReceiver::WriteBinlogToReplicator(
    std::shared_ptr<replica::LogReplicator> replicator,
    const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
    const auto& indexes = request->index_region();
    for (int i = 0; i < indexes.size(); ++i) {
        const auto& inner_index = indexes.Get(i);
        for (int j = 0; j < inner_index.segment_size(); ++j) {
            const auto& segment_index = inner_index.segment(j);
            for (int key_idx = 0; key_idx < segment_index.key_entries_size(); ++key_idx) {
                const auto& key_entries = segment_index.key_entries(key_idx);
                const auto& pk = key_entries.key();
                for (int key_entry_idx = 0; key_entry_idx < key_entries.key_entry_size(); ++key_entry_idx) {
                    const auto& key_entry = key_entries.key_entry(key_entry_idx);
                    for (int time_idx = 0; time_idx < key_entry.time_entry_size(); ++time_idx) {
                        const auto& time_entry = key_entry.time_entry(time_idx);
                        auto* block =
                            time_entry.block_id() < data_blocks.size() ? data_blocks[time_entry.block_id()] : nullptr;
                        if (block == nullptr) {
                            // TODO(hw): valid?
                            continue;
                        }
                        ::openmldb::api::LogEntry entry;
                        entry.set_pk(pk);
                        entry.set_ts(time_entry.time());
                        entry.set_value(block->data, block->size);
                        entry.set_term(replicator->GetLeaderTerm());
                        replicator->AppendEntry(entry);
                        log_num++;
                    }
                }
            }
        }
    }
    return true;
}
}  // namespace tablet
}  // namespace openmldb