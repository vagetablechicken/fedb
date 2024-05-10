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

#ifndef SRC_STORAGE_INDEX_ORGANIZED_TABLE_H_
#define SRC_STORAGE_INDEX_ORGANIZED_TABLE_H_

#include "storage/mem_table.h"

#include "catalog/tablet_catalog.h"

namespace openmldb::storage {

class IndexOrganizedTable : public MemTable {
 public:
    IndexOrganizedTable(const ::openmldb::api::TableMeta& table_meta, std::shared_ptr<catalog::TabletCatalog> catalog)
        : MemTable(table_meta), catalog_(catalog) {}

    TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

    TraverseIterator* NewTraverseIterator(uint32_t index) override;

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index) override;

 private:
    // to get current distribute iterator
    std::shared_ptr<catalog::TabletCatalog> catalog_;
};
}  // namespace openmldb::storage

#endif
