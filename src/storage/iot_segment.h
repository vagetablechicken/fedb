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

#ifndef SRC_STORAGE_IOT_SEGMENT_H_
#define SRC_STORAGE_IOT_SEGMENT_H_

#include "codec/row_codec.h"
#include "codec/row_iterator.h"
#include "storage/segment.h"
#include "storage/table.h"  // for storage::Schema
namespace openmldb::storage {

// secondary index iterator
// GetValue will lookup, and it may trigger rpc
class IOTIterator : public MemTableIterator {
 public:
    IOTIterator(TimeEntries::Iterator* it, type::CompressType compress_type,
                std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableIterator(it, compress_type), cidx_iter_(std::move(cidx_iter)) {}
    virtual ~IOTIterator() {}
    // TODO(hw): add schema for test, delete later
    void SetSchema(const std::shared_ptr<openmldb::storage::Schema>& schema,
                   const std::shared_ptr<openmldb::storage::IndexDef>& cidx) {
        schema_ = *schema;  // copy
        // pkeys idx
        std::map<std::string, int> col_idx_map;
        for (int i = 0; i < schema_.size(); i++) {
            col_idx_map[schema_[i].name()] = i;
        }
        pkeys_idx_.clear();
        for (auto pkey : cidx->GetColumns()) {
            pkeys_idx_.emplace_back(col_idx_map[pkey.GetName()]);
        }

        ts_idx_ = col_idx_map[cidx->GetTsColumn()->GetName()];
    }
    openmldb::base::Slice GetValue() const override {
        auto pkeys_pts = MemTableIterator::GetValue();
        // TODO(hw): secondary index is covering index for test, unpack the row and get pkeys+pts
        // fix this after secondary index value is pkeys+pts
        std::vector<std::string> vec;
        codec::RowCodec::DecodeRow(schema_, pkeys_pts, vec);
        std::string pkeys;
        for (auto pkey_idx : pkeys_idx_) {
            if (!pkeys.empty()) {
                pkeys += "|";
            }
            auto& key = vec[pkey_idx];
            pkeys += key.empty() ? hybridse::codec::EMPTY_STRING : key;
        }
        uint64_t ts =  std::stoull(vec[ts_idx_]);
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            auto ts_iter = cidx_iter_->GetValue();
            ts_iter->Seek(ts);
            if (ts_iter->Valid()){
                auto row = ts_iter->GetValue();
                return {(const char*)row.buf(), (size_t)row.size()};
            }
        }
        // TODO(hw): Valid() to check row data? what if only one entry invalid?
        return "";
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
    // test
    codec::Schema schema_;
    std::vector<int> pkeys_idx_;
    int ts_idx_;
};

}  // namespace openmldb::storage
#endif  // SRC_STORAGE_IOT_SEGMENT_H_
