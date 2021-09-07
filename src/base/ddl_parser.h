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

// TODO(hw):
#include "../../hybridse/src/vm/simple_catalog.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base_impl.h"
#include "sdk/sql_insert_row.h"
#include "vm/engine.h"

namespace openmldb::base {

using namespace hybridse::vm;

class DDLParser {
 public:
    static std::map<std::string, std::vector<::openmldb::common::ColumnKey>> ExtractIndexes(
        const std::string& sql,
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schema) {
        ::hybridse::type::Database db;
        std::string tmp_db = "temp_" + std::to_string(::baidu::common::timer::get_micros() / 1000);
        db.set_name(tmp_db);
        auto defs = GetTableDefs(schema);
        // TODO(hw): redundant copy
        for (auto& def : defs) {
            auto add = db.add_tables();
            add->CopyFrom(def);
        }

        std::shared_ptr<hybridse::vm::CompileInfo> compile_info;
        if (!GetPlan(sql, db, compile_info)) {
            // TODO(hw):
        }
        auto plan = compile_info->GetPhysicalPlan();
        std::vector<hybridse::vm::PhysicalOpNode*> nodes;
        // TODO(hw): cast from needs to remove 'const'
        DagToList(const_cast<hybridse::vm::PhysicalOpNode*>(plan), nodes);

        ParseIndexes(nodes);
        return {};
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
        for (decltype(cast_node->GetProducerCnt()) i = 0; i < cast_node->GetProducerCnt(); ++i) {
            nodes.push_back(cast_node->GetProducer(i));
        }
        auto union_list = cast_node->window_unions();
        for (decltype(union_list.GetSize()) i = 0; i < union_list.GetSize(); ++i) {
            nodes.push_back(union_list.GetKey(i));
        }

        for (auto node : nodes) {
            auto table = FindDataProviderNode(const_cast<PhysicalOpNode*>(node));
            // TODO(hw):
        }
    }

    static bool GetPlan(const std::string& sql, const ::hybridse::type::Database& database,
                        std::shared_ptr<::hybridse::vm::CompileInfo>& compile_info) {
        // TODO(hw): engine is input, do not create in here
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(database);
        ::hybridse::vm::EngineOptions options;
        options.set_keep_ir(true);
        options.set_compile_only(true);
        options.set_performance_sensitive(false);
        auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

        ::hybridse::vm::RequestRunSession session;
        ::hybridse::base::Status status;
        auto ok = engine->Get(sql, database.name(), session, status);
        if (!(ok && status.isOK())) {
            LOG(WARNING) << "hybrid engine compile sql failed, " << status.msg << ", " << status.trace;
            return false;
        }
        compile_info = session.GetCompileInfo();
        return true;
    }

    static bool Explain(const std::string& sql, const ::hybridse::type::Database& database) {
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>();
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

    static std::vector<::hybridse::type::TableDef> GetTableDefs(
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schema) {
        std::vector<::hybridse::type::TableDef> defs;
        for (auto& table : schema) {
            ::hybridse::type::TableDef def;
            def.set_name(table.first);
            auto& cols = table.second;

            for (auto& col : table.second) {
                auto add = def.add_columns();
                add->set_name(col.name());
                add->set_type(codec::SchemaCodec::ConvertType(col.data_type()));
            }
        }
        return defs;
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
