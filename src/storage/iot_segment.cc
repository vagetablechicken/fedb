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

}  // namespace openmldb::storage
