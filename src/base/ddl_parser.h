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
#include <llvm/ADT/STLExtras.h>

#include "codec/schema_codec.h"
#include "common/timer.h"
#include "passes/physical/group_and_sort_optimized.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base_impl.h"
#include "sdk/sql_insert_row.h"
#include "vm/engine.h"
#include "vm/physical_plan_context.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"

namespace openmldb::base {

using namespace hybridse::vm;

using IndexMap = std::map<std::string, std::vector<::openmldb::common::ColumnKey>>;

class GroupAndSortOptimizedParser : public hybridse::passes::GroupAndSortOptimized {
 public:
    explicit GroupAndSortOptimizedParser(PhysicalPlanContext* plan_ctx)
        : hybridse::passes::GroupAndSortOptimized(plan_ctx) {}

    void Parse(PhysicalOpNode* in) {
        LOG(INFO) << "parse ";

        PhysicalOpNode* output = nullptr;
        hybridse::passes::GroupAndSortOptimized::Transform(in, &output);
    }

    void GetIndexes() {}

 private:
    IndexMap index_map_;
};
/*
// class GroupAndSortOptimizedParser {
//  public:
//     // 递归jiexi
//     bool KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key, Key*
//     index_key,
//                             Key* right_key, Sort* sort, PhysicalOpNode** new_in) {
//         if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
//             return false;
//         }
//
//         if (right_key != nullptr && !right_key->ValidKey()) {
//             return false;
//         }
//
//         if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
//             auto scan_op = dynamic_cast<PhysicalDataProviderNode*>(in);
//             // Do not optimized with Request DataProvider (no index has been provided)
//             if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
//                 return false;
//             }
//
//             if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
//                 DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
//                 const hybridse::node::ExprListNode* right_partition = right_key == nullptr ? left_key->keys() :
//                 right_key->keys();
//
//                 size_t key_num = right_partition->GetChildNum();
//                 std::vector<bool> bitmap(key_num, false);
//                 hybridse::node::ExprListNode order_values;
//
//                 PhysicalPartitionProviderNode* partition_op = nullptr;
//                 std::string index_name;
//                 if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
//                     // Apply key columns and order column optimization with all indexes binding to
//                     // scan_op->table_handler_ Return false if fail to find an appropriate index
//                     if (!TransformKeysAndOrderExpr(root_schemas_ctx, right_partition,
//                                                    nullptr == sort ? nullptr : sort->orders_,
//                                                    scan_op->table_handler_, &index_name, &bitmap)) {
//                         return false;
//                     }
//                     Status status =
//                         plan_ctx_->CreateOp<PhysicalPartitionProviderNode>(&partition_op, scan_op, index_name);
//                     if (!status.isOK()) {
//                         LOG(WARNING) << "Fail to create partition op: " << status;
//                         return false;
//                     }
//                 } else {
//                     partition_op = dynamic_cast<PhysicalPartitionProviderNode*>(scan_op);
//                     index_name = partition_op->index_name_;
//                     // Apply key columns and order column optimization with given index name
//                     // Return false if given index do not match the keys and order column
//                     if (!TransformKeysAndOrderExpr(root_schemas_ctx, right_partition,
//                                                    nullptr == sort ? nullptr : sort->orders_,
//                                                    scan_op->table_handler_, &index_name, &bitmap)) {
//                         return false;
//                     }
//                 }
//
//                 auto new_left_keys = node_manager_->MakeExprList();
//                 auto new_right_keys = node_manager_->MakeExprList();
//                 auto new_index_keys = node_manager_->MakeExprList();
//                 for (size_t i = 0; i < bitmap.size(); ++i) {
//                     auto left = left_key->keys()->GetChild(i);
//                     if (bitmap[i]) {
//                         new_index_keys->AddChild(left);
//                     } else {
//                         new_left_keys->AddChild(left);
//                         if (right_key != nullptr) {
//                             new_right_keys->AddChild(right_key->keys()->GetChild(i));
//                         }
//                     }
//                 }
//                 if (right_key != nullptr) {
//                     right_key->set_keys(new_right_keys);
//                 }
//                 index_key->set_keys(new_index_keys);
//                 left_key->set_keys(new_left_keys);
//                 // Clear order expr list if we optimized orders
//                 if (nullptr != sort && nullptr != sort->orders_ && nullptr != sort->orders_->GetOrderExpression(0)) {
//                     auto first_order_expression = sort->orders_->GetOrderExpression(0);
//                     sort->set_orders(
//                         dynamic_cast<node::OrderByNode*>(node_manager_->MakeOrderByNode(node_manager_->MakeExprList(
//                             node_manager_->MakeOrderExpression(nullptr, first_order_expression->is_asc())))));
//                 }
//                 *new_in = partition_op;
//                 return true;
//             }
//         } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
//             auto simple_project = dynamic_cast<PhysicalSimpleProjectNode*>(in);
//             PhysicalOpNode* new_depend;
//             if (!KeysOptimized(root_schemas_ctx, simple_project->producers()[0], left_key, index_key, right_key,
//             sort,
//                                &new_depend)) {
//                 return false;
//             }
//             PhysicalSimpleProjectNode* new_simple_op = nullptr;
//             Status status =
//                 plan_ctx_->CreateOp<PhysicalSimpleProjectNode>(&new_simple_op, new_depend,
//                 simple_project->project());
//             if (!status.isOK()) {
//                 LOG(WARNING) << "Fail to create simple project op: " << status;
//                 return false;
//             }
//             *new_in = new_simple_op;
//             return true;
//         } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
//             PhysicalOpNode* new_depend;
//             if (!KeysOptimized(root_schemas_ctx, in->producers()[0], left_key, index_key, right_key, sort,
//                                &new_depend)) {
//                 return false;
//             }
//             PhysicalRenameNode* new_op = nullptr;
//             Status status = plan_ctx_->CreateOp<PhysicalRenameNode>(&new_op, new_depend,
//                                                                     dynamic_cast<PhysicalRenameNode*>(in)->name_);
//             if (!status.isOK()) {
//                 LOG(WARNING) << "Fail to create rename op: " << status;
//                 return false;
//             }
//             *new_in = new_op;
//             return true;
//         }
//         return false;
//     }
//     bool KeysAndOrderFilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
//                                           Key* hash, Sort* sort, PhysicalOpNode** new_in) {
//         return KeysOptimizedParse(root_schemas_ctx, in, group, hash, nullptr, sort, new_in);
//     }
//     void TransformParse(PhysicalOpNode* in) {
//         switch (in->GetOpType()) {
//             case PhysicalOpType::kPhysicalOpGroupBy: {
//                 auto group_op = dynamic_cast<PhysicalGroupNode*>(in);
//                 PhysicalOpNode* new_producer;
//                 // group -> index key?
//                 if (!GroupOptimized(group_op->schemas_ctx(), group_op->GetProducer(0), &group_op->group_,
//                                     &new_producer)) {
//                 }
//                 break;
//             }
//             case PhysicalOpType::kPhysicalOpProject: {
//                 auto project_op = dynamic_cast<PhysicalProjectNode*>(in);
//                 if (ProjectType::kWindowAggregation == project_op->project_type_) {
//                     auto window_agg_op = dynamic_cast<PhysicalWindowAggrerationNode*>(project_op);
//                     PhysicalOpNode* input = window_agg_op->GetProducer(0);
//
//                     PhysicalOpNode* new_producer;
//                     if (!window_agg_op->instance_not_in_window()) {
//                         if (KeyAndOrderOptimized(input->schemas_ctx(), input, &window_agg_op->window_.partition_,
//                                                  &window_agg_op->window_.sort_, &new_producer)) {
//                             input = new_producer;
//                         }
//                     }
//                     // must prepare for window join column infer
//                     auto& window_joins = window_agg_op->window_joins();
//                     auto& window_unions = window_agg_op->window_unions();
//                     window_agg_op->InitJoinList(plan_ctx_);
//                     auto& joined_op_list_ = window_agg_op->joined_op_list_;
//                     if (!window_joins.Empty()) {
//                         size_t join_idx = 0;
//                         for (auto& window_join : window_joins.window_joins()) {
//                             PhysicalOpNode* cur_joined = joined_op_list_[join_idx];
//
//                             PhysicalOpNode* new_join_right;
//                             if (JoinKeysOptimized(cur_joined->schemas_ctx(), window_join.first, &window_join.second,
//                                                   &new_join_right)) {
//                                 window_join.first = new_join_right;
//                             }
//                             join_idx += 1;
//                         }
//                     }
//                     if (!window_unions.Empty()) {
//                         for (auto& window_union : window_unions.window_unions_) {
//                             PhysicalOpNode* new_producer;
//                             if (KeyAndOrderOptimized(window_union.first->schemas_ctx(), window_union.first,
//                                                      &window_union.second.partition_, &window_union.second.sort_,
//                                                      &new_producer)) {
//                                 window_union.first = new_producer;
//                             }
//                         }
//                     }
//                 }
//                 break;
//             }
//             case PhysicalOpType::kPhysicalOpRequestUnion: {
//                 auto union_op = dynamic_cast<PhysicalRequestUnionNode*>(in);
//                 PhysicalOpNode* new_producer;
//
//                 if (!union_op->instance_not_in_window()) {
//                     KeysAndOrderFilterOptimizedParse(union_op->schemas_ctx(), union_op->GetProducer(1),
//                                                     &union_op->window_.partition_, &union_op->window_.index_key_,
//                                                     &union_op->window_.sort_, &new_producer));
//                 }
//
//                 if (!union_op->window_unions().Empty()) {
//                     for (auto& window_union : union_op->window_unions_.window_unions_) {
//                         PhysicalOpNode* new_producer;
//                         auto& window = window_union.second;
//                         if (KeysAndOrderFilterOptimizedParse(window_union.first->schemas_ctx(), window_union.first,
//                                                              &window.partition_, &window.index_key_, &window.sort_,
//                                                              &new_producer)) {
//                             window_union.first = new_producer;
//                         }
//                     }
//                 }
//                 break;
//             }
//             case PhysicalOpType::kPhysicalOpRequestJoin: {
//                 PhysicalRequestJoinNode* join_op = dynamic_cast<PhysicalRequestJoinNode*>(in);
//                 PhysicalOpNode* new_producer;
//                 // Optimized Right Table Partition
//                 if (!JoinKeysOptimized(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
//                                        &new_producer)) {
//                     return false;
//                 }
//                 if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
//                     return false;
//                 }
//
//                 return true;
//             }
//             case PhysicalOpType::kPhysicalOpJoin: {
//                 PhysicalJoinNode* join_op = dynamic_cast<PhysicalJoinNode*>(in);
//                 PhysicalOpNode* new_producer;
//                 // Optimized Right Table Partition
//                 if (!JoinKeysOptimized(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
//                                        &new_producer)) {
//                     return false;
//                 }
//                 if (!ResetProducer(plan_ctx_, join_op, 1, new_producer)) {
//                     return false;
//                 }
//                 return true;
//             }
//             case PhysicalOpType::kPhysicalOpFilter: {
//                 PhysicalFilterNode* filter_op = dynamic_cast<PhysicalFilterNode*>(in);
//                 PhysicalOpNode* new_producer;
//                 if (FilterOptimized(filter_op->schemas_ctx(), filter_op->GetProducer(0), &filter_op->filter_,
//                                     &new_producer)) {
//                     if (!ResetProducer(plan_ctx_, filter_op, 0, new_producer)) {
//                         return false;
//                     }
//                 }
//             }
//             default: {
//                 return false;
//             }
//         }
//     }
//
//     // LRD
//     void Parse(PhysicalOpNode* cur_op) {
//         // just parse, won't modify, but need to cast, so we use non-const producers.
//         auto& producers = cur_op->producers();
//         for (auto& producer : producers) {
//             Parse(producer);
//         }
//
//         TransformParse(cur_op);
//     }
//     void GetIndexes() {}
//
//  private:
//     IndexMap index_map_;
// };
 */
class DDLParser {
 public:
    static std::map<std::string, std::vector<::openmldb::common::ColumnKey>> ExtractIndexes(
        const std::string& sql, const ::hybridse::type::Database& db) {
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        // tablet catalog supports index, so we should add index support too
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(db);

        std::shared_ptr<hybridse::vm::CompileInfo> compile_info;
        if (!GetPlan(sql, catalog, db.name(), compile_info)) {
            // TODO(hw):
        }
        auto plan = compile_info->GetPhysicalPlan();

        //        std::vector<hybridse::vm::PhysicalOpNode*> nodes;
        //        // TODO(hw): cast from needs to remove 'const'
        //        DagToList(const_cast<hybridse::vm::PhysicalOpNode*>(plan), nodes);
        //        ParseIndexes(nodes);

        auto ctx = std::dynamic_pointer_cast<SqlCompileInfo>(compile_info)->get_sql_context();
        ParseIndexes(catalog, const_cast<hybridse::vm::PhysicalOpNode*>(plan), ctx);
        return {};
    }
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
        return ExtractIndexes(sql, db);
    }

    // DLR
    static std::map<std::string, std::vector<::openmldb::common::ColumnKey>> ParseIndexes(
        const std::shared_ptr<Catalog>& catalog, hybridse::vm::PhysicalOpNode* node, hybridse::vm::SqlContext& ctx) {
        // This physical plan is optimized, but no real optimization about index(cuz no index in fake catalog).
        // So we can apply optimization parser(very like transformer's pass-ApplyPasses)

        // Transform needs too many configs. Just imitate GroupAndSortOptimized
        //  GroupAndSortOptimized pass(&plan_ctx_); -> plan_ctx_
        //  transformed = pass.Apply(cur_op, &new_op);
        PhysicalOpNode* new_op = nullptr;

        // Request mode RequestModeTransformer
        // TODO(hw): should use a new ctx, nm will add the same nodes...
        PhysicalPlanContext plan_ctx(&ctx.nm, ctx.udf_library, ctx.db, catalog, &ctx.parameter_types,
                                     ctx.enable_expr_optimize);
        GroupAndSortOptimizedParser parser(&plan_ctx);
        parser.Parse(node);
        parser.GetIndexes();

        // can parse index

        // window union

        // children(producers) should apply this index
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

    static bool GetPlan(const std::string& sql, const std::shared_ptr<Catalog>& catalog, const std::string& db,
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
