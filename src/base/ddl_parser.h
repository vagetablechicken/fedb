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
#ifndef SRC_BASE_DDL_PARSER_H_
#define SRC_BASE_DDL_PARSER_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "node/plan_node.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base.h"
#include "vm/simple_catalog.h"

namespace hybridse::vm {
class RunSession;
class PhysicalOpNode;
}  // namespace hybridse::vm

namespace openmldb::base {

// convert ms to minutes, ceil
int64_t AbsTTLConvert(int64_t time_ms, bool zero_eq_unbounded);
int64_t LatTTLConvert(int64_t time_ms, bool zero_eq_unbounded);

using IndexMap = std::map<std::string, std::vector<::openmldb::common::ColumnKey>>;
using MultiDBIndexMap = std::map<std::string, IndexMap>;

using TableDescMap = std::map<std::string, std::vector<openmldb::common::ColumnDesc>>;
using MultiDBTableDescMap = std::map<std::string, TableDescMap>;

struct LongWindowInfo {
    std::string window_name_;
    std::string aggr_func_;
    std::string aggr_col_;
    std::string partition_col_;
    std::string order_col_;
    std::string bucket_size_;
    std::string filter_col_;
    LongWindowInfo(std::string window_name, std::string aggr_func, std::string aggr_col, std::string partition_col,
                   std::string order_col, std::string bucket_size)
        : window_name_(window_name),
          aggr_func_(aggr_func),
          aggr_col_(aggr_col),
          partition_col_(partition_col),
          order_col_(order_col),
          bucket_size_(bucket_size) {}
};
using LongWindowInfos = std::vector<LongWindowInfo>;

class DDLParser {
 public:
    /** core funcs(with arg catalog) **/

    // simpler for test, single db
    static IndexMap ExtractIndexes(const std::string& sql, const ::hybridse::type::Database& db);
    static std::string Explain(const std::string& sql, const ::hybridse::type::Database& db);
    // remove later, catalog is core arg
    static std::shared_ptr<hybridse::sdk::Schema> GetOutputSchema(const std::string& sql,
                                                                  const hybridse::type::Database& db);
    // returns
    // 1. empty list: means valid
    // 2. otherwise a list(len 2):[0] the error msg; [1] the trace
    static std::vector<std::string> ValidateSQLInBatch(const std::string& sql, const hybridse::type::Database& db);
    static std::vector<std::string> ValidateSQLInRequest(const std::string& sql, const hybridse::type::Database& db);

    /** interfaces, the arg schema's type can be varied **/
    // single db, do not use <db>.<table> in sql
    static IndexMap ExtractIndexes(
        const std::string& sql,
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schemas);

    static IndexMap ExtractIndexes(const std::string& sql, const TableDescMap& schemas);
    // request mode, multi db, you can use <db>.<table> in sql, if has <table>, use the current used db.
    static MultiDBIndexMap ExtractIndexes(const std::string& sql, const std::string& used_db,
                                          const MultiDBTableDescMap& schemas);

    // request mode, multi db
    static std::shared_ptr<hybridse::sdk::Schema> GetOutputSchema(const std::string& sql, const std::string& db,
                                                                  // <db, <table, columns>>
                                                                  const MultiDBTableDescMap& schemas);

    static hybridse::sdk::Status ExtractLongWindowInfos(const std::string& sql,
                                                        const std::unordered_map<std::string, std::string>& window_map,
                                                        LongWindowInfos* infos);

    static std::vector<std::string> ValidateSQLInBatch(const std::string& sql, const std::string& db,
                                                       const MultiDBTableDescMap& schemas);

    static std::vector<std::string> ValidateSQLInRequest(const std::string& sql, const std::string& db,
                                                         const MultiDBTableDescMap& schemas);

 private:
    // tables are in one db, and db name will be rewritten for simplicity
    static IndexMap ExtractIndexes(const std::string& sql, const hybridse::type::Database& db,
                                   hybridse::vm::RunSession* session);

    // DLR
    static MultiDBIndexMap ParseIndexes(hybridse::vm::PhysicalOpNode* node);

    // real get plan func, multi db should use catalog, don't forget init catalog with enable_index=true
    static bool GetPlan(const std::string& sql, const std::string& db,
                        const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog, hybridse::vm::RunSession* session,
                        hybridse::base::Status* status);
    /** multi APIs, single db, ... **/
    static bool GetPlan(const std::string& sql, const std::string& db,
                        const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog, hybridse::vm::RunSession* session);
    static bool GetPlan(const std::string& sql, const hybridse::type::Database& db, hybridse::vm::RunSession* session);
    // If you want the status, use this
    static bool GetPlan(const std::string& sql, const hybridse::type::Database& db, hybridse::vm::RunSession* session,
                        hybridse::base::Status* status);

    template <typename T>
    static void AddTables(const T& table_defs, hybridse::type::Database* db);

    static std::shared_ptr<hybridse::vm::SimpleCatalog> buildCatalog(const MultiDBTableDescMap& schemas);

    // traverse plan tree to extract all long window infos
    static bool TraverseNode(hybridse::node::PlanNode* node,
                             const std::unordered_map<std::string, std::string>& window_map,
                             LongWindowInfos* long_window_infos);

    static bool ExtractInfosFromProjectPlan(hybridse::node::ProjectPlanNode* project_plan_node,
                                            const std::unordered_map<std::string, std::string>& window_map,
                                            LongWindowInfos* long_window_infos);
};
}  // namespace openmldb::base

#endif  // SRC_BASE_DDL_PARSER_H_
