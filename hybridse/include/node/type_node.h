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

#ifndef HYBRIDSE_INCLUDE_NODE_TYPE_NODE_H_
#define HYBRIDSE_INCLUDE_NODE_TYPE_NODE_H_

#include <string>
#include <vector>

#include "codec/fe_row_codec.h"
#include "node/expr_node.h"
#include "node/sql_node.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace node {

class NodeManager;

class TypeNode : public SqlNode {
 public:
    TypeNode() : SqlNode(node::kType, 0, 0), base_(hybridse::node::kVoid) {}
    explicit TypeNode(hybridse::node::DataType base)
        : SqlNode(node::kType, 0, 0), base_(base), generics_({}) {}
    explicit TypeNode(hybridse::node::DataType base, const TypeNode *v1)
        : SqlNode(node::kType, 0, 0),
          base_(base),
          generics_({v1}),
          generics_nullable_({false}) {}
    explicit TypeNode(hybridse::node::DataType base,
                      const hybridse::node::TypeNode *v1,
                      const hybridse::node::TypeNode *v2)
        : SqlNode(node::kType, 0, 0),
          base_(base),
          generics_({v1, v2}),
          generics_nullable_({false, false}) {}
    ~TypeNode() override {}

    friend bool operator==(const TypeNode &lhs, const TypeNode &rhs);

    // Return this node cast as a NodeType.
    // Use only when this node is known to be that type, otherwise, behavior is undefined.
    template <typename NodeType>
    const NodeType *GetAsOrNull() const {
        static_assert(std::is_base_of<TypeNode, NodeType>::value,
                      "NodeType must be a member of the TypeNode class hierarchy");
        return dynamic_cast<const NodeType *>(this);
    }

    template <typename NodeType>
    NodeType *GetAsOrNull() {
        static_assert(std::is_base_of<TypeNode, NodeType>::value,
                      "NodeType must be a member of the TypeNode class hierarchy");
        return dynamic_cast<NodeType *>(this);
    }

    // canonical name for the type
    // this affect the function generated by codegen
    virtual const std::string GetName() const;

    // readable string representation
    virtual std::string DebugString() const;

    const hybridse::node::TypeNode *GetGenericType(size_t idx) const;

    bool IsGenericNullable(size_t idx) const { return generics_nullable_[idx]; }

    size_t GetGenericSize() const { return generics_.size(); }

    hybridse::node::DataType base() const { return base_; }
    const std::vector<const hybridse::node::TypeNode *> &generics() const { return generics_; }

    void AddGeneric(const node::TypeNode *dtype, bool nullable);

    void Print(std::ostream &output, const std::string &org_tab) const override;
    bool Equals(const SqlNode *node) const override;
    TypeNode *ShadowCopy(NodeManager *) const override;
    TypeNode *DeepCopy(NodeManager *) const override;

    bool IsBaseOrNullType() const;
    bool IsBaseType() const;
    bool IsTuple() const;
    bool IsTupleNumbers() const;
    bool IsString() const;
    bool IsTimestamp() const;
    bool IsDate() const;
    bool IsArithmetic() const;
    bool IsNumber() const;
    bool IsIntegral() const;
    bool IsInteger() const;
    bool IsNull() const;
    bool IsBool() const;
    bool IsFloating() const;
    bool IsGeneric() const;

    virtual bool IsMap() const { return false; }
    virtual bool IsArray() const { return base_ == kArray; }

    static Status CheckTypeNodeNotNull(const TypeNode *left_type);

    hybridse::node::DataType base_;

    // generics_ not empty if it is a complex data type:
    // 1. base = ARRAY, generics = [ element_type ]
    // 2. base = MAP, generics = [ key_type, value_type ]
    // 3. base = STRUCT, generics = [ fileld_type, ... ] (unimplemented)
    // inner types, not exists in SQL level
    // 4. base = LIST, generics = [ element_type ]
    // 5. base = ITERATOR, generics = [ element_type ]
    // 6. base = TUPLE (like STRUCT), generics = [ element_type, ... ]
    // 7. ... (might others, undocumented)
    std::vector<const hybridse::node::TypeNode *> generics_;
    std::vector<int> generics_nullable_;
};


class OpaqueTypeNode : public TypeNode {
 public:
    explicit OpaqueTypeNode(size_t bytes)
        : TypeNode(node::kOpaque), bytes_(bytes) {}

    size_t bytes() const { return bytes_; }

    const std::string GetName() const override;

    OpaqueTypeNode *ShadowCopy(NodeManager *) const override;

 private:
    size_t bytes_;
};

class RowTypeNode : public TypeNode {
 public:
    // Initialize with external schemas context
    explicit RowTypeNode(const vm::SchemasContext *schemas_ctx);

    // Initialize with schema
    explicit RowTypeNode(const std::vector<const codec::Schema *> &schemas);

    ~RowTypeNode();

    const vm::SchemasContext *schemas_ctx() const { return schemas_ctx_; }

    RowTypeNode *ShadowCopy(NodeManager *) const override;

 private:
    bool IsOwnedSchema() const { return is_own_schema_ctx_; }

    // if initialized without a physical node context
    // hold a self-owned schemas context
    const vm::SchemasContext *schemas_ctx_;
    bool is_own_schema_ctx_;
};

// fixed sized array
// this appears for array construct expression
//
// There is also array type with size unknown at compile time,
// e.g. array subquery or column referenc to a array column
class FixedArrayType : public TypeNode {
 public:
    explicit FixedArrayType(const TypeNode *ele_ty, uint64_t size)
        : TypeNode(kArray), ele_ty_(ele_ty), num_elements_(size) {
        AddGeneric(ele_ty, true);
    }
    ~FixedArrayType() override {}

    uint64_t num_elements() const { return num_elements_; }
    const TypeNode *element_type() const { return ele_ty_; }

    const std::string GetName() const override;
    std::string DebugString() const override;
    FixedArrayType *ShadowCopy(NodeManager *) const override;

    bool IsArray() const override { return true; }

 private:
    const TypeNode* ele_ty_;
    uint64_t num_elements_;
};

class MapType : public TypeNode {
 public:
    MapType(const TypeNode *key_ty, const TypeNode *value_ty, bool value_not_null = false) ABSL_ATTRIBUTE_NONNULL();
    ~MapType() override;

    bool IsMap() const override { return true; }

    const TypeNode *key_type() const;
    const TypeNode *value_type() const;
    bool value_nullable() const;

    // test if input args can safely apply to a map function
    static absl::StatusOr<MapType *> InferMapType(NodeManager *, absl::Span<const ExprAttrNode> types);
};

}  // namespace node
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_NODE_TYPE_NODE_H_
