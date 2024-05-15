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

#include "storage/iot_segment.h"

#include "iot_segment.h"

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

std::string PackPkeysAndPts(const std::string& pkeys, uint64_t pts) {
    std::string buf;
    uint32_t pkeys_size = pkeys.size();
    buf.append(reinterpret_cast<const char*>(&pkeys_size), sizeof(uint32_t));
    buf.append(pkeys);
    buf.append(reinterpret_cast<const char*>(&pts), sizeof(uint64_t));
    return buf;
}

bool UnpackPkeysAndPts(const std::string& block, std::string* pkeys, uint64_t* pts) {
    uint32_t offset = 0;
    uint32_t pkeys_size = *reinterpret_cast<const uint32_t*>(block.data() + offset);
    offset += sizeof(uint32_t);
    pkeys->assign(block.data() + offset, pkeys_size);
    offset += pkeys_size;
    *pts = *reinterpret_cast<const uint64_t*>(block.data() + offset);
    return true;
}

// put_if_absent unsupported, iot table will reject put, no need to check here, just ignore
bool IOTSegment::PutUnlock(const Slice& key, uint64_t time, DataBlock* row, bool put_if_absent, bool auto_gen_ts) {
    void* entry = nullptr;
    uint32_t byte_size = 0;
    // one key just one entry
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == nullptr) {
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        // need to delete memory when free node
        Slice skey(pk, key.size());
        entry = reinterpret_cast<void*>(new KeyEntry(key_entry_max_height_));
        uint8_t height = entries_->Insert(skey, entry);
        byte_size += GetRecordPkIdxSize(height, key.size(), key_entry_max_height_);
        pk_cnt_.fetch_add(1, std::memory_order_relaxed);
        // no need to check if absent when first put
    } else if (IsClusteredTs(ts_idx_map_.begin()->first)) {
        // if cidx and key match, check ts -> insert or update
        if (auto_gen_ts) {
            // cidx(keys) has just one entry for one keys, so if keys exists, needs delete
            DLOG_ASSERT(reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() == 1)
                << "cidx keys has more than one entry";
            // TODO(hw): client will delete old row, so if pkeys exists when auto ts, fail it
            return false;
        } else {
            // cidx(keys+ts) check if ts match
            if (ListContains(reinterpret_cast<KeyEntry*>(entry), time, row, false)) {
                LOG(WARNING) << "key " << key.ToString() << " ts " << time << " exists in cidx";
                return false;
            }
        }
    }

    idx_cnt_vec_[0]->fetch_add(1, std::memory_order_relaxed);
    uint8_t height = reinterpret_cast<KeyEntry*>(entry)->entries.Insert(time, row);
    reinterpret_cast<KeyEntry*>(entry)->count_.fetch_add(1, std::memory_order_relaxed);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
    return true;
}

bool IOTSegment::Put(const Slice& key, const std::map<int32_t, uint64_t>& ts_map, DataBlock* row, bool auto_gen_ts) {
    if (ts_map.empty()) {
        return false;
    }
    if (ts_cnt_ == 1) {
        bool ret = false;
        if (auto pos = ts_map.find(ts_idx_map_.begin()->first); pos != ts_map.end()) {
            // TODO(hw): why ts_map key is int32_t, default ts is uint32_t?
            ret = Segment::Put(key, pos->second, row, false, pos->first == DEFAULT_TS_COL_ID);
        }
        return ret;
    }
    void* entry_arr = nullptr;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : ts_map) {
        uint32_t byte_size = 0;
        auto pos = ts_idx_map_.find(kv.first);
        if (pos == ts_idx_map_.end()) {
            continue;
        }
        if (entry_arr == nullptr) {
            int ret = entries_->Get(key, entry_arr);
            if (ret < 0 || entry_arr == nullptr) {
                char* pk = new char[key.size()];
                memcpy(pk, key.data(), key.size());
                Slice skey(pk, key.size());
                KeyEntry** entry_arr_tmp = new KeyEntry*[ts_cnt_];
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    entry_arr_tmp[i] = new KeyEntry(key_entry_max_height_);
                }
                entry_arr = reinterpret_cast<void*>(entry_arr_tmp);
                uint8_t height = entries_->Insert(skey, entry_arr);
                byte_size += GetRecordPkMultiIdxSize(height, key.size(), key_entry_max_height_, ts_cnt_);
                pk_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        auto entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
        if (IsClusteredTs(pos->first)) {
            // if cidx and key match, check ts -> insert or update
            if (auto_gen_ts) {
                // cidx(keys) has just one entry for one keys, so if keys exists, needs delete
                DLOG_ASSERT(reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() == 1)
                    << "cidx keys has more than one entry";
                // TODO(hw): client will delete old row, so if pkeys exists when auto ts, fail it
                if (reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() > 0) {
                    LOG(WARNING) << "key " << key.ToString() << " exists in cidx";
                    return false;
                }
            } else {
                // cidx(keys+ts) check if ts match
                if (ListContains(reinterpret_cast<KeyEntry*>(entry), kv.second, row, false)) {
                    LOG(WARNING) << "key " << key.ToString() << " ts " << kv.second << " exists in cidx";
                    return false;
                }
            }
        }
        uint8_t height = entry->entries.Insert(kv.second, row);
        entry->count_.fetch_add(1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
        idx_cnt_vec_[pos->second]->fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

}  // namespace openmldb::storage
