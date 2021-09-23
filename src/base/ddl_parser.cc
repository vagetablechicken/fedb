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

#include "base/ddl_parser.h"

#include "ddl_parser.h"

namespace openmldb::base {

bool IndexMapBuilder::UpdateIndex(const hybridse::vm::Range &range) {
    if (latest_record_.empty() || index_map_.find(latest_record_) == index_map_.end()) {
        LOG(DFATAL) << "want to update ttl status, but index is not created before";
        return false;
    }
    auto ts_col = GetTsCol(latest_record_);

    if (!range.Valid()) {
        LOG(INFO) << "range is invalid, can't update ttl, use the default";
        return true;
    }
    if (range.range_key()->GetExprString() != ts_col) {
        LOG(ERROR) << "want ts col " << ts_col << ", but get " << range.range_key()->GetExprString();
        return false;
    }

    auto frame = range.frame();
    auto start = frame->GetHistoryRangeStart();
    auto rows_start = frame->GetHistoryRowsStart();

    LOG_ASSERT(start <= 0 && rows_start <= 0);

    std::stringstream ss;
    range.frame()->Print(ss, "");
    LOG(INFO) << "frame info: " << ss.str() << ", get bounds: " << start << ", " << rows_start;

    common::TTLSt ttl_st;

    auto type = frame->frame_type();
    if (type == hybridse::node::kFrameRows) {
        // frame_rows is valid
        LOG_ASSERT(frame->frame_range() == nullptr && frame->GetHistoryRowsStartPreceding() > 0);
        ttl_st.set_lat_ttl(frame->GetHistoryRowsStartPreceding());
        ttl_st.set_ttl_type(type::TTLType::kLatestTime);
    } else {
        // frame_range is valid
        LOG_ASSERT(type != hybridse::node::kFrameRowsMergeRowsRange) << "merge type, how to parse?";
        LOG_ASSERT(frame->frame_rows() == nullptr && frame->GetHistoryRangeStart() < 0);
        // GetHistoryRangeStart is negative, ttl needs uint64
        ttl_st.set_abs_ttl(std::max(MIN_TIME, -1 * frame->GetHistoryRangeStart()));
        ttl_st.set_ttl_type(type::TTLType::kAbsoluteTime);
    }

    index_map_[latest_record_] = ttl_st;
    LOG(INFO) << latest_record_ << " update ttl " << index_map_[latest_record_].DebugString();

    // to avoid double update
    latest_record_.clear();
    return true;
}

IndexMap IndexMapBuilder::ToMap() {
    IndexMap result;
    for (auto &pair : index_map_) {
        auto dec = Decode(pair.first);
        result[dec.first].emplace_back(dec.second);
    }

    return result;
}

std::string IndexMapBuilder::Encode(const std::string &table, const hybridse::node::ExprListNode *keys,
                                    const hybridse::node::OrderByNode *ts) {
    // children are ColumnRefNode
    auto cols = NormalizeColumns(table, keys->children_);
    if (cols.empty()) {
        return {};
    }

    std::stringstream ss;
    ss << table << ":";
    auto iter = cols.begin();
    ss << (*iter);
    iter++;
    for (; iter != cols.end(); iter++) {
        ss << "," << (*iter);
    }
    ss << ";";

    if (ts != nullptr && ts->order_expressions_ != nullptr) {
        for (auto order : ts->order_expressions_->children_) {
            auto cast = dynamic_cast<hybridse::node::OrderExpression *>(order);
            if (cast->expr() != nullptr) {
                ss << cast->expr()->GetExprString();
            }
        }
    } else {
        // If no ts, we should find one column which type is int64/timestamp
        auto schema = cl_->GetTable(DDLParser::DB_NAME, table)->GetSchema();
        bool find_ts = false;
        for (auto &col : *schema) {
            // key cols can be ts too, no need to check
            // ts col type == ::openmldb::type::kBigInt || type == ::openmldb::type::kTimestamp
            if (col.type() == hybridse::type::kInt64 || col.type() == hybridse::type::kTimestamp) {
                ss << col.name();
                find_ts = true;
                break;
            }
        }
        if (!find_ts) {
            LOG(ERROR) << "can't find one col to be ts col";
            return {};
        }
    }
    return ss.str();
}

bool GroupAndSortOptimizedParser::KeysOptimizedParse(const SchemasContext *root_schemas_ctx, PhysicalOpNode *in,
                                                     Key *left_key, Key *index_key, Key *right_key, Sort *sort,
                                                     PhysicalOpNode **new_in) {
    if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
        return false;
    }

    if (right_key != nullptr && !right_key->ValidKey()) {
        return false;
    }

    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<PhysicalDataProviderNode *>(in);
        // Do not optimize with Request DataProvider (no index has been provided)
        if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
            return false;
        }

        if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
            DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
            const hybridse::node::ExprListNode *right_partition =
                right_key == nullptr ? left_key->keys() : right_key->keys();

            size_t key_num = right_partition->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            hybridse::node::ExprListNode order_values;

            if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
                // Apply key columns and order column optimization with all indexes binding to
                // scan_op->table_handler_ Return false if fail to find an appropriate index
                auto groups = right_partition;
                auto order = (nullptr == sort ? nullptr : sort->orders_);
                DLOG(INFO) << "keys and order optimized: keys=" << hybridse::node::ExprString(groups)
                           << ", order=" << (order == nullptr ? "null" : hybridse::node::ExprString(order))
                           << " for table " << scan_op->table_handler_->GetName();

                index_map_builder_.CreateIndex(scan_op->table_handler_->GetName(), groups, order);
                // parser won't create partition_op
                return true;
            } else {
                auto partition_op = dynamic_cast<PhysicalPartitionProviderNode *>(scan_op);
                LOG_ASSERT(partition_op != nullptr);
                auto index_name = partition_op->index_name_;
                // Apply key columns and order column optimization with given index name
                // Return false if given index do not match the keys and order column
                // -- return false won't change index_name
                LOG(WARNING) << "What if the index is not best index? Do we need to adjust index?";
                return false;
            }
        }
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<PhysicalSimpleProjectNode *>(in);
        PhysicalOpNode *new_depend;
        return KeysOptimizedParse(root_schemas_ctx, simple_project->producers()[0], left_key, index_key, right_key,
                                  sort, &new_depend);

    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode *new_depend;
        return KeysOptimizedParse(root_schemas_ctx, in->producers()[0], left_key, index_key, right_key, sort,
                                  &new_depend);
    }
    return false;
}

std::ostream &operator<<(std::ostream &os, IndexMap &index_map) {
    for (auto &indexes : index_map) {
        os << " {" << indexes.first << "[";
        for (auto &ck : indexes.second) {
            os << ck.ShortDebugString() << ", ";
        }
        os << "]} ";
    }
    return os;
}
}  // namespace openmldb::base
