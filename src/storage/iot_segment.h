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

#include "catalog/tablet_catalog.h"
#include "codec/row_codec.h"
#include "codec/row_iterator.h"
#include "codec/sql_rpc_row_codec.h"
#include "storage/mem_table_iterator.h"
#include "storage/segment.h"
#include "storage/table.h"  // for storage::Schema

namespace openmldb::storage {

base::Slice RowToSlice(const ::hybridse::codec::Row& row) {
    butil::IOBuf buf;
    size_t size;
    if (codec::EncodeRpcRow(row, &buf, &size)) {
        auto r = new char[buf.size()];
        buf.copy_to(r);  // TODO(hw): don't copy, move it to slice
        // slice own the new r
        return {r, size, true};
    }
    LOG(WARNING) << "convert row to slice failed";
    return {};
}
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
        uint64_t ts = std::stoull(vec[ts_idx_]);
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            auto ts_iter = cidx_iter_->GetValue();
            ts_iter->Seek(ts);
            if (ts_iter->Valid()) {
                return RowToSlice(ts_iter->GetValue());
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

class IOTTraverseIterator : public MemTableTraverseIterator {
 public:
    IOTTraverseIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                        uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index, type::CompressType compress_type,
                        std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableTraverseIterator(segments, seg_cnt, ttl_type, expire_time, expire_cnt, ts_index, compress_type),
          cidx_iter_(std::move(cidx_iter)) {}
    ~IOTTraverseIterator() override {}
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
        auto pkeys_pts = MemTableTraverseIterator::GetValue();
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
        uint64_t ts = std::stoull(vec[ts_idx_]);
        // distribute cidx iter should seek to (key, ts)
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            auto ts_iter = cidx_iter_->GetValue();
            ts_iter->Seek(ts);
            if (ts_iter->Valid()) {
                return RowToSlice(ts_iter->GetValue());
            }
        }
        LOG(WARNING) << "no suitable iter";
        return "";  // won't core, just no row for select
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
    // test
    codec::Schema schema_;
    std::vector<int> pkeys_idx_;
    int ts_idx_;
};

class IOTWindowIterator : public MemTableWindowIterator {
 public:
    IOTWindowIterator(TimeEntries::Iterator* it, ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                      uint64_t expire_cnt, type::CompressType compress_type,
                      std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableWindowIterator(it, ttl_type, expire_time, expire_cnt, compress_type),
          cidx_iter_(std::move(cidx_iter)) {}
    void SetSchema(const codec::Schema& schema, const std::vector<int>& pkeys_idx, int ts_idx) {
        schema_ = schema;
        pkeys_idx_ = pkeys_idx;
        ts_idx_ = ts_idx;
        if (ts_idx_ != -1) {
            ts_type_ = schema_.Get(ts_idx_).data_type();
        }
    }
    const ::hybridse::codec::Row& GetValue() override {
        auto pkeys_pts = MemTableWindowIterator::GetValue();
        // unpack the row and get pkeys+pts
        // Row -> cols
        codec::RowView row_view(schema_, pkeys_pts.buf(), pkeys_pts.size());
        std::string pkeys, key;  // RowView Get will assign output, no need to clear
        for (auto pkey_idx : pkeys_idx_) {
            if (!pkeys.empty()) {
                pkeys += "|";
            }
            // TODO(hw): if null, append to key?
            auto ret = row_view.GetStrValue(pkey_idx, &key);
            if (ret == -1) {
                LOG(WARNING) << "get pkey failed";
                return dummy;
            }
            pkeys += key.empty() ? hybridse::codec::EMPTY_STRING : key;
            DLOG(INFO) << pkey_idx << "=" << key;
        }
        // TODO(hw): what if no ts?
        uint64_t ts = 0;
        if (ts_idx_ != -1) {
            int64_t tsv = 0;
            auto ret = row_view.GetInteger(pkeys_pts.buf(), ts_idx_, ts_type_, &tsv);
            if (ret == -1) {
                LOG(WARNING) << "get ts failed";
                return dummy;  // TODO(hw): right?
            } else if (ret == 1) {
                LOG(INFO) << "ts is null"; // TODO(hw): ts col can be null?
            } else {
                ts = tsv;
            }
        }
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            DLOG(INFO) << "seek to ts " << ts;
            // hold the row iterator to avoid invalidation
            cidx_ts_iter_ = std::move(cidx_iter_->GetValue());
            cidx_ts_iter_->Seek(ts);
            if (cidx_ts_iter_->Valid()) {
                return cidx_ts_iter_->GetValue();
            }
        }
        // Valid() to check row data? what if only one entry invalid?
        return dummy;
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
    std::unique_ptr<hybridse::codec::RowIterator> cidx_ts_iter_;
    // test
    codec::Schema schema_;
    std::vector<int> pkeys_idx_;
    int ts_idx_ = -1;
    type::DataType ts_type_;
    ::hybridse::codec::Row dummy;
};

class IOTKeyIterator : public MemTableKeyIterator {
 public:
    IOTKeyIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                   uint64_t expire_cnt, uint32_t ts_index, type::CompressType compress_type,
                   std::shared_ptr<catalog::TabletTableHandler> cidx_handler, const std::string& cidx_name)
        : MemTableKeyIterator(segments, seg_cnt, ttl_type, expire_time, expire_cnt, ts_index, compress_type) {
        // cidx_iter will be used by RowIterator but it's unique, so create it when get RowIterator
        cidx_handler_ = cidx_handler;
        cidx_name_ = cidx_name;
    }

    ~IOTKeyIterator() override {}
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
    ::hybridse::vm::RowIterator* GetRawValue() override {
        // TODO(hw):
        TimeEntries::Iterator* it = GetTimeIter();
        auto cidx_iter = cidx_handler_->GetWindowIterator(cidx_name_);
        auto iter =
            new IOTWindowIterator(it, ttl_type_, expire_time_, expire_cnt_, compress_type_, std::move(cidx_iter));
        iter->SetSchema(schema_, pkeys_idx_, ts_idx_);
        return iter;
    }

 private:
    std::shared_ptr<catalog::TabletTableHandler> cidx_handler_;
    std::string cidx_name_;
    // test
    codec::Schema schema_;
    std::vector<int> pkeys_idx_;
    int ts_idx_;
};

}  // namespace openmldb::storage
#endif  // SRC_STORAGE_IOT_SEGMENT_H_
