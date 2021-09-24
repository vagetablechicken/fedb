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
#ifndef SRC_BASE_PARSER_H_
#define SRC_BASE_PARSER_H_

#include <utility>

#include "codec/schema_codec.h"
#include "common/timer.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base_impl.h"
#include "sdk/sql_insert_row.h"
#include "vm/engine.h"
#include "vm/physical_op.h"
#include "vm/physical_plan_context.h"
#include "vm/simple_catalog.h"  // TODO(hw):
#include "vm/sql_compiler.h"

namespace openmldb::base {

using namespace hybridse::vm;

using IndexMap = std::map<std::string, std::vector<::openmldb::common::ColumnKey>>;
std::ostream& operator<<(std::ostream& os, IndexMap& index_map);
class IndexMapBuilder {
 public:
    explicit IndexMapBuilder(std::shared_ptr<Catalog> cl) : cl_(std::move(cl)) {}
    // create the index with unset TTLSt, return false if the index(same table, same keys, same ts) existed
    bool CreateIndex(const std::string& table, const hybridse::node::ExprListNode* keys,
                     const hybridse::node::OrderByNode* ts);
    bool UpdateIndex(const hybridse::vm::Range& range);
    IndexMap ToMap();

 private:
    static std::vector<std::string> NormalizeColumns(const std::string& table,
                                                     const std::vector<hybridse::node::ExprNode*>& nodes);
    // table, keys and ts -> table:key1,key2,...;ts
    std::string Encode(const std::string& table, const hybridse::node::ExprListNode* keys,
                       const hybridse::node::OrderByNode* ts);

    static std::pair<std::string, common::ColumnKey> Decode(const std::string& index_str);

    static std::string GetTsCol(const std::string& index_str) {
        if (index_str.empty() || index_str.back() == TS_MARK) {
            return {};
        }
        auto ts_begin = index_str.find(TS_MARK) + 1;
        return index_str.substr(ts_begin);
    }

    static std::string GetTable(const std::string& index_str) {
        if (index_str.empty()) {
            return {};
        }
        auto key_sep = index_str.find(KEY_MARK);
        return index_str.substr(0, key_sep);
    }

 private:
    static constexpr int64_t MIN_TIME = 60 * 1000;
    static constexpr char KEY_MARK = ':';
    static constexpr char KEY_SEP = ',';
    static constexpr char TS_MARK = ';';

    std::string latest_record_;
    // map<table_keys_and_order_str, ttl_st>
    std::map<std::string, common::TTLSt> index_map_;
    // used to get schema
    std::shared_ptr<Catalog> cl_;
};

// no plan_ctx_, node_manager_: we assume that creating new op won't affect the upper level structure.
class GroupAndSortOptimizedParser {
 public:
    explicit GroupAndSortOptimizedParser(const std::shared_ptr<Catalog>& cl) : index_map_builder_(cl) {}

    // LRD
    void Parse(PhysicalOpNode* cur_op) {
        LOG_ASSERT(cur_op != nullptr);
        // just parse, won't modify, but need to cast ptr, so we use non-const producers.
        auto& producers = cur_op->producers();
        for (auto& producer : producers) {
            Parse(producer);
        }

        DLOG(INFO) << "parse " << hybridse::vm::PhysicalOpTypeName(cur_op->GetOpType());
        TransformParse(cur_op);
    }

    IndexMap GetIndexes() { return index_map_builder_.ToMap(); }

 private:
    // recursive parse, return true iff kProviderTypeTable optimized
    // new_in is useless, but we keep it, GroupAndSortOptimizedParser will be more similar to GroupAndSortOptimized.
    bool KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key, Key* index_key,
                            Key* right_key, Sort* sort, PhysicalOpNode** new_in);

    bool KeysAndOrderFilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                                          Key* hash, Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, group, hash, nullptr, sort, new_in);
    }

    bool JoinKeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Join* join,
                                PhysicalOpNode** new_in) {
        if (nullptr == join) {
            return false;
        }
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, join, &join->right_sort_, new_in);
    }
    bool FilterAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                                      Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, &filter->left_key_, &filter->index_key_, &filter->right_key_,
                                  sort, new_in);
    }
    bool FilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                              PhysicalOpNode** new_in) {
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, filter, nullptr, new_in);
    }
    bool KeyAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group, Sort* sort,
                                   PhysicalOpNode** new_in) {
        Key mock_key;
        return KeysAndOrderFilterOptimizedParse(root_schemas_ctx, in, group, &mock_key, sort, new_in);
    }
    bool GroupOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                             PhysicalOpNode** new_in) {
        return KeyAndOrderOptimizedParse(root_schemas_ctx, in, group, nullptr, new_in);
    }

    static std::vector<PhysicalOpNode*> InitJoinList(PhysicalWindowAggrerationNode* op);

    void TransformParse(PhysicalOpNode* in);

 private:
    IndexMapBuilder index_map_builder_;
};

class DDLParser {
 public:
    static constexpr const char* DB_NAME = "ddl_parser_db";
    // tables are in one db, and db name will be rewritten for simplicity
    static IndexMap ExtractIndexes(const std::string& sql, const ::hybridse::type::Database& db) {
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        // tablet catalog supports index, so we should add index support too
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        auto cp = db;
        LOG(INFO) << "test " << DB_NAME;
        cp.set_name(DB_NAME);
        catalog->AddDatabase(cp);

        std::shared_ptr<hybridse::vm::CompileInfo> compile_info;
        if (!GetRequestPlan(sql, catalog, cp.name(), compile_info)) {
            LOG(ERROR) << "sql get plan failed";
            return {};
        }
        auto plan = compile_info->GetPhysicalPlan();

        //        std::vector<hybridse::vm::PhysicalOpNode*> nodes;
        //        // TODO(hw): cast from needs to remove 'const'
        //        DagToList(const_cast<hybridse::vm::PhysicalOpNode*>(plan), nodes);
        //        ParseIndexes(nodes);

        auto& ctx = std::dynamic_pointer_cast<SqlCompileInfo>(compile_info)->get_sql_context();
        return ParseIndexes(catalog, const_cast<hybridse::vm::PhysicalOpNode*>(plan), ctx);
    }

    // TODO(hw): refactor
    static IndexMap ExtractIndexesForBatch(const std::string& sql, const ::hybridse::type::Database& db) {
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        // tablet catalog supports index, so we should add index support too
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        auto cp = db;
        cp.set_name(DB_NAME);
        catalog->AddDatabase(cp);

        std::shared_ptr<hybridse::vm::CompileInfo> compile_info;
        if (!GetBatchPlan(sql, catalog, cp.name(), compile_info)) {
            LOG(ERROR) << "sql get plan failed";
            return {};
        }
        auto plan = compile_info->GetPhysicalPlan();

        //        std::vector<hybridse::vm::PhysicalOpNode*> nodes;
        //        // TODO(hw): cast from needs to remove 'const'
        //        DagToList(const_cast<hybridse::vm::PhysicalOpNode*>(plan), nodes);
        //        ParseIndexes(nodes);

        auto& ctx = std::dynamic_pointer_cast<SqlCompileInfo>(compile_info)->get_sql_context();
        return ParseIndexes(catalog, const_cast<hybridse::vm::PhysicalOpNode*>(plan), ctx);
    }

    static IndexMap ExtractIndexes(
        const std::string& sql,
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schemas) {
        ::hybridse::type::Database db;
        std::string tmp_db = "temp_" + std::to_string(::baidu::common::timer::get_micros() / 1000);
        db.set_name(tmp_db);
        AddTables(schemas, db);
        return ExtractIndexes(sql, db);
    }

    // DLR
    static IndexMap ParseIndexes(const std::shared_ptr<Catalog>& catalog, hybridse::vm::PhysicalOpNode* node,
                                 hybridse::vm::SqlContext& ctx) {
        // This physical plan is optimized, but no real optimization about index(cuz no index in fake catalog).
        // So we can run GroupAndSortOptimizedParser on the plan(very like transformer's pass-ApplyPasses)
        GroupAndSortOptimizedParser parser(catalog);
        parser.Parse(node);
        return parser.GetIndexes();
    }

    static void ParseLastJoinOp(PhysicalOpNode* node,
                                std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& indexes_map) {}
    static std::map<std::string, std::vector<::openmldb::common::ColumnKey>> ParseIndexes(
        const std::vector<hybridse::vm::PhysicalOpNode*>& nodes) {
        std::map<std::string, std::vector<::openmldb::common::ColumnKey>> indexes_map;
        for (auto node : nodes) {
            auto type = node->GetOpType();
            if (type == hybridse::vm::PhysicalOpType::kPhysicalOpDataProvider) {
                //            auto cast_node = PhysicalDataProviderNode::CastFrom(node);
                //            const auto& name = cast_node->GetName();
                //            // TODO(hw): need?
                //            if (indexes_map.find(name) == indexes_map.end()) {
                //                auto& cols = indexes_map[name];
                //            }
                continue;
            }
            if (type == hybridse::vm::PhysicalOpType::kPhysicalOpRequestUnion) {
                ParseWindowOp(node, indexes_map);
                continue;
            }
            if (type == hybridse::vm::PhysicalOpType::kPhysicalOpRequestJoin) {
                ParseLastJoinOp(node, indexes_map);
                continue;
            }
            if (type == hybridse::vm::PhysicalOpType::kPhysicalOpRename) {
                continue;
            }
        }
        return {};
    }

    // find and cast, no creation
    static PhysicalDataProviderNode* FindDataProviderNode(PhysicalOpNode* node) {
        if (node->GetOpType() == PhysicalOpType::kPhysicalOpDataProvider) {
            return PhysicalDataProviderNode::CastFrom(node);
        }

        if (node->GetOpType() == PhysicalOpType::kPhysicalOpSimpleProject) {
            return FindDataProviderNode(node->GetProducer(0));
        }
        if (node->GetOpType() == PhysicalOpType::kPhysicalOpRename) {
            return FindDataProviderNode(node->GetProducer(0));
        }
        return nullptr;
    }

    // no-index plan: REQUEST_UNION(partition_keys=(xx), orders=(xx ASC), rows=(xx, -3, 0), index_keys=)
    //  ->no index_keys
    // We should create index on partition_keys
    static void ParseWindowOp(PhysicalOpNode* node,
                              std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& indexes_map) {
        auto cast_node = PhysicalRequestUnionNode::CastFrom(node);

        auto frame_range = cast_node->window().range().frame()->frame_range();
        auto frame_rows = cast_node->window().range().frame()->frame_rows();
        int64_t start = 0, end = 0, cnt_start = 0, cnt_end = 0;
        if (frame_range != nullptr) {
            start = frame_range->start()->GetSignedOffset();
            end = frame_range->end()->GetSignedOffset();
        }
        if (frame_rows != nullptr) {
            cnt_start = frame_rows->start()->GetSignedOffset();
            cnt_end = frame_rows->end()->GetSignedOffset();
        }
        // TODO(hw): not good
        std::vector<const PhysicalOpNode*> nodes;
        // left, right: both PhysicalDataProviderNode, table or request or partition
        // 1. table provider -- add index --> partition provider
        // TODO(hw): index keys and orders, how to parse ttl type or what, frame_range/rows?
        for (decltype(cast_node->GetProducerCnt()) i = 0; i < cast_node->GetProducerCnt(); ++i) {
            nodes.push_back(cast_node->GetProducer(i));
        }

        // TODO(hw): how about window_unions_, it may have data provider too, only parse table provider?
        auto union_list = cast_node->window_unions();
        for (decltype(union_list.GetSize()) i = 0; i < union_list.GetSize(); ++i) {
            nodes.push_back(union_list.GetKey(i));
        }

        for (auto node : nodes) {
            auto table = FindDataProviderNode(const_cast<PhysicalOpNode*>(node));
            // TODO(hw):
        }
    }

    static bool GetRequestPlan(const std::string& sql, const std::shared_ptr<Catalog>& catalog, const std::string& db,
                               std::shared_ptr<::hybridse::vm::CompileInfo>& compile_info) {
        // TODO(hw): engine is input, do not create in here
        ::hybridse::vm::Engine::InitializeGlobalLLVM();

        ::hybridse::vm::EngineOptions options;
        options.set_keep_ir(true);
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

        ::hybridse::vm::RequestRunSession session;
        ::hybridse::base::Status status;
        auto ok = engine->Get(sql, db, session, status);
        if (!(ok && status.isOK())) {
            LOG(WARNING) << "hybrid engine compile sql failed, " << status.msg << ", " << status.trace;
            return false;
        }
        compile_info = session.GetCompileInfo();
        return true;
    }

    static bool GetBatchPlan(const std::string& sql, const std::shared_ptr<Catalog>& catalog, const std::string& db,
                             std::shared_ptr<::hybridse::vm::CompileInfo>& compile_info) {
        // TODO(hw): engine is input, do not create in here
        ::hybridse::vm::Engine::InitializeGlobalLLVM();

        ::hybridse::vm::EngineOptions options;
        options.set_keep_ir(true);
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

        ::hybridse::vm::BatchRunSession session;
        ::hybridse::base::Status status;
        auto ok = engine->Get(sql, db, session, status);
        if (!(ok && status.isOK())) {
            LOG(WARNING) << "hybrid engine compile sql failed, " << status.msg << ", " << status.trace;
            return false;
        }
        compile_info = session.GetCompileInfo();
        return true;
    }

    static bool Explain(const std::string& sql, const ::hybridse::type::Database& database) {
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);  // tablet catalog supports index
        catalog->AddDatabase(database);

        ::hybridse::vm::EngineOptions options;
        options.set_keep_ir(true);
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);
        ::hybridse::vm::ExplainOutput explain_output;
        ::hybridse::base::Status vm_status;
        ::hybridse::codec::Schema parameter_schema;
        bool ok = engine->Explain(sql, database.name(), ::hybridse::vm::kRequestMode, parameter_schema, &explain_output,
                                  &vm_status);
        if (!ok) {
            LOG(WARNING) << "fail to explain sql " << sql;
            return false;
        }
        ::hybridse::sdk::SchemaImpl input_schema(explain_output.input_schema);
        ::hybridse::sdk::SchemaImpl output_schema(explain_output.output_schema);
        //        std::shared_ptr<ExplainInfoImpl> impl(
        //            new ExplainInfoImpl(input_schema, output_schema, explain_output.logical_plan,
        //            explain_output.physical_plan,
        //                                explain_output.ir, explain_output.request_name));
        LOG(INFO) << "logical plan:\n" << explain_output.logical_plan;
        LOG(INFO) << "physical plan:\n" << explain_output.physical_plan;
        return true;
    }

    static void AddTables(
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schema,
        hybridse::type::Database& db) {
        std::vector<::hybridse::type::TableDef> defs;
        for (auto& table : schema) {
            auto def = db.add_tables();
            def->set_name(table.first);

            auto& cols = table.second;
            for (auto& col : table.second) {
                auto add = def->add_columns();
                add->set_name(col.name());
                add->set_type(codec::SchemaCodec::ConvertType(col.data_type()));
            }
        }
    }

    static void DagToList(hybridse::vm::PhysicalOpNode* node, std::vector<hybridse::vm::PhysicalOpNode*>& vec) {
        if (node->GetOpType() == hybridse::vm::PhysicalOpType::kPhysicalOpRequestUnion) {
            auto cast_node = hybridse::vm::PhysicalRequestUnionNode::CastFrom(node);
            // TODO(hw): GetSize() is not marked const
            auto union_list = cast_node->window_unions();
            for (decltype(union_list.GetSize()) i = 0; i < union_list.GetSize(); ++i) {
                DagToList(node, vec);
            }
        }
        for (decltype(node->GetProducerCnt()) i = 0; i < node->GetProducerCnt(); ++i) {
            DagToList(node->GetProducer(i), vec);
        }
        vec.push_back(node);
    }
};

}  // namespace openmldb::base

#endif  // SRC_BASE_PARSER_H_
