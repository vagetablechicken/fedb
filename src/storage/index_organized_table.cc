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

#include "storage/index_organized_table.h"

#include <snappy.h>

#include "storage/iot_segment.h"

namespace openmldb::storage {

IOTIterator* NewNullIterator() {
    // if TimeEntries::Iterator is null, nothing will be used
    return new IOTIterator(nullptr, type::CompressType::kNoCompress, {});
}

// TODO(hw): temp func to create iot iterator
IOTIterator* NewIOTIterator(Segment* segment, const Slice& key, Ticket& ticket, type::CompressType compress_type,
                            std::unique_ptr<hybridse::codec::WindowIterator> cidx_iter) {
    void* entry = nullptr;
    auto entries = segment->GetKeyEntries();
    if (entries == nullptr || segment->GetTsCnt() > 1 || entries->Get(key, entry) < 0 || entry == nullptr) {
        return NewNullIterator();
    }
    ticket.Push(reinterpret_cast<KeyEntry*>(entry));
    return new IOTIterator(reinterpret_cast<KeyEntry*>(entry)->entries.NewIterator(), compress_type,
                           std::move(cidx_iter));
}

IOTIterator* NewIOTIterator(Segment* segment, const Slice& key, uint32_t idx, Ticket& ticket,
                            type::CompressType compress_type,
                            std::unique_ptr<hybridse::codec::WindowIterator> cidx_iter) {
    auto ts_idx_map = segment->GetTsIdxMap();
    auto pos = ts_idx_map.find(idx);
    if (pos == ts_idx_map.end()) {
        return NewNullIterator();
    }
    auto entries = segment->GetKeyEntries();
    if (segment->GetTsCnt() == 1) {
        return NewIOTIterator(segment, key, ticket, compress_type, std::move(cidx_iter));
    }
    void* entry_arr = nullptr;
    if (entries->Get(key, entry_arr) < 0 || entry_arr == nullptr) {
        return NewNullIterator();
    }
    auto entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
    ticket.Push(entry);
    return new IOTIterator(entry->entries.NewIterator(), compress_type, std::move(cidx_iter));
}

// TODO(hw): iot iterator needs schema for test, delete later
TableIterator* IndexOrganizedTable::NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        LOG(WARNING) << "index is invalid";
        return nullptr;
    }
    LOG(INFO) << "hw test new iter for index and pk " << index << " name " << index_def->GetName();
    uint32_t seg_idx = SegIdx(pk);
    Slice spk(pk);
    uint32_t real_idx = index_def->GetInnerPos();
    Segment* segment = GetSegment(real_idx, seg_idx);
    auto ts_col = index_def->GetTsColumn();
    // TODO(hw): no IOT Segment now, impl later
    if (ts_col) {
        // if secondary, use iot iterator
        if (index_def->IsSecondaryIndex()) {
            // get clustered index iter for secondary index
            auto handler = catalog_->GetTable(GetDB(), GetName());
            if (!handler) {
                LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
                return nullptr;
            }
            auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
            if (!tablet_table_handler) {
                LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
                return nullptr;
            }
            LOG(INFO) << "create iot iterator for pk";
            // TODO(hw): iter may be invalid if catalog updated
            auto iter =
                NewIOTIterator(segment, spk, ts_col->GetId(), ticket, GetCompressType(),
                               std::move(tablet_table_handler->GetWindowIterator(table_index_.GetIndex(0)->GetName())));
            // schema, pkeys cols, ts col
            iter->SetSchema(GetSchema(), table_index_.GetIndex(0));
            return iter;
        }
        // clsutered and covering still use old iterator
        return segment->NewIterator(spk, ts_col->GetId(), ticket, GetCompressType());
    }
    // TODO(hw): secondary index support no ts? ts is current time or const?
    return segment->NewIterator(spk, ticket, GetCompressType());
}

TraverseIterator* IndexOrganizedTable::NewTraverseIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "index %u not found. tid %u pid %u", index, id_, pid_);
        return nullptr;
    }
    LOG(INFO) << "hw test new traverse iter for index " << index << " name " << index_def->GetName();
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (GetExpireStatus()) {  // gc enabled
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        // if secondary, use iot iterator
        if (index_def->IsSecondaryIndex()) {
            // get clustered index iter for secondary index
            auto handler = catalog_->GetTable(GetDB(), GetName());
            if (!handler) {
                LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
                return nullptr;
            }
            auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
            if (!tablet_table_handler) {
                LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
                return nullptr;
            }
            LOG(INFO) << "create iot traverse iterator for traverse";
            // TODO(hw): iter may be invalid if catalog updated
            auto iter = new IOTTraverseIterator(
                GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_col->GetId(),
                GetCompressType(),
                std::move(tablet_table_handler->GetWindowIterator(table_index_.GetIndex(0)->GetName())));
            // schema, pkeys cols, ts col
            iter->SetSchema(GetSchema(), table_index_.GetIndex(0));
            return iter;
        }
        return new MemTableTraverseIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt,
                                            ts_col->GetId(), GetCompressType());
    }
    return new MemTableTraverseIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, 0,
                                        GetCompressType());
}

::hybridse::vm::WindowIterator* IndexOrganizedTable::NewWindowIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        LOG(WARNING) << "index id " << index << " not found. tid " << id_ << " pid " << pid_;
        return nullptr;
    }
    LOG(INFO) << "hw test new window iter for index " << index << " name " << index_def->GetName();
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (GetExpireStatus()) {
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    uint32_t ts_idx = 0;
    if (ts_col) {
        ts_idx = ts_col->GetId();
    }
    // if secondary, use iot iterator
    if (index_def->IsSecondaryIndex()) {
        // get clustered index iter for secondary index
        auto handler = catalog_->GetTable(GetDB(), GetName());
        if (!handler) {
            LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
            return nullptr;
        }
        auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
        if (!tablet_table_handler) {
            LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
            return nullptr;
        }
        LOG(INFO) << "create iot key traverse iterator for window";
        // TODO(hw): iter may be invalid if catalog updated
        auto iter =
            new IOTKeyIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_idx,
                               GetCompressType(), tablet_table_handler, table_index_.GetIndex(0)->GetName());
        // schema, pkeys cols, ts col
        iter->SetSchema(GetSchema(), table_index_.GetIndex(0));
        return iter;
    }
    return new MemTableKeyIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_idx,
                                   GetCompressType());
}

bool IndexOrganizedTable::Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) {
    uint32_t seg_idx = SegIdx(pk);
    Segment* segment = GetSegment(0, seg_idx);
    if (segment == nullptr) {
        return false;
    }
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    record_byte_size_.fetch_add(GetRecordSize(size));
    return true;
}

absl::Status IndexOrganizedTable::Put(uint64_t time, const std::string& value, const Dimensions& dimensions,
                                      bool put_if_absent) {
    if (dimensions.empty()) {
        PDLOG(WARNING, "empty dimension. tid %u pid %u", id_, pid_);
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": empty dimension"));
    }
    // inner index pos: -1 means invalid, so it's positive in inner_index_key_map
    std::map<int32_t, Slice> inner_index_key_map;
    std::pair<int32_t, std::string> inner_pkeys_pair;
    std::vector<int32_t> secondary_inners;

    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        int32_t inner_pos = table_index_.GetInnerIndexPos(iter->idx());
        if (inner_pos < 0) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid dimension idx ", iter->idx()));
        }
        if (iter->idx() == 0) {
            inner_pkeys_pair = {inner_pos, iter->key()};
        }
        inner_index_key_map.emplace(inner_pos, iter->key());
    }

    const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
    std::string uncompress_data;
    uint32_t data_length = value.length();
    if (GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(value.data(), value.size(), &uncompress_data);
        data = reinterpret_cast<const int8_t*>(uncompress_data.data());
        data_length = uncompress_data.length();
    }
    if (data_length < codec::HEADER_LENGTH) {
        PDLOG(WARNING, "invalid value. tid %u pid %u", id_, pid_);
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid value"));
    }
    uint8_t version = codec::RowView::GetSchemaVersion(data);
    auto decoder = GetVersionDecoder(version);
    if (decoder == nullptr) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid schema version ", version));
    }

    uint64_t clustered_tsv = 0;
    std::map<uint32_t, std::map<int32_t, uint64_t>> ts_value_map;
    // we need two ref cnt
    // 1. clustered and covering: put row ->DataBlock(i)
    // 2. secondary: put pkeys+pts -> DataBlock(j)
    uint32_t real_ref_cnt = 0, secondary_ref_cnt = 0;
    // inner_pkeys_pair can get the clustered index
    for (const auto& kv : inner_index_key_map) {
        auto inner_index = table_index_.GetInnerIndex(kv.first);
        if (!inner_index) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid inner index pos ", kv.first));
        }
        std::map<int32_t, uint64_t> ts_map;
        for (const auto& index_def : inner_index->GetIndex()) {
            if (!index_def->IsReady()) {
                continue;
            }
            auto ts_col = index_def->GetTsColumn();
            if (ts_col) {
                int64_t ts = 0;
                if (ts_col->IsAutoGenTs()) {
                    // clustered index don't use current time to be ts TODO(hw): current time to ttl?
                    if (kv.first == inner_pkeys_pair.first) {
                        ts = 0;
                    } else {
                        ts = time;
                    }
                } else if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                    return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": get ts failed"));
                }
                if (ts < 0) {
                    return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": ts is negative ", ts));
                }
                // TODO(hw): why uint32_t to int32_t?
                ts_map.emplace(ts_col->GetId(), ts);

                if (index_def->IsSecondaryIndex()) {
                    secondary_ref_cnt++;
                } else {
                    real_ref_cnt++;
                }
                if (index_def->IsClusteredIndex()) {
                    clustered_tsv = ts;
                }
            }
        }
        if (!ts_map.empty()) {
            ts_value_map.emplace(kv.first, std::move(ts_map));
        }
    }
    if (ts_value_map.empty()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": empty ts value map"));
    }
    // get pkeys+pts
    auto pts = ts_value_map.find(inner_pkeys_pair.first);
    if (pts == ts_value_map.end()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": no pts for clustered index"));
    }
    auto pkeys_pts = PackPkeysAndPts(inner_pkeys_pair.second, clustered_tsv);
    auto* row_block = new DataBlock(real_ref_cnt, value.c_str(), value.length());
    auto* pblock = new DataBlock(secondary_ref_cnt, pkeys_pts.c_str(), pkeys_pts.length());
    for (const auto& kv : inner_index_key_map) {
        auto iter = ts_value_map.find(kv.first);
        if (iter == ts_value_map.end()) {
            continue;
        }
        uint32_t seg_idx = SegIdx(kv.second.ToString());  // TODO(hw):
        Segment* segment = GetSegment(kv.first, seg_idx);
        // clustered index put row
        auto blk = kv.first == inner_pkeys_pair.first ? row_block : pblock;
        DLOG(INFO) << "don't support put if absent";
        // clustered segment should be dedup and update will trigger all index update like delete TODO
        if (!segment->Put(kv.second, iter->second, blk, false)) {
            return absl::AlreadyExistsError("data exists");  // let caller know exists
        }
    }
    record_byte_size_.fetch_add(GetRecordSize(value.length()));
    return absl::OkStatus();
}

}  // namespace openmldb::storage
