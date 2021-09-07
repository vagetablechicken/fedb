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

#include "case/sql_case.h"
#include "gtest/gtest.h"

namespace openmldb::base {
class DDLParserTest : public ::testing::Test {
 public:
    // , , {name, type, name, type, ...}
    static bool AddTableToDB(::hybridse::type::Database& db, const std::string& table_name,
                             std::initializer_list<std::string> cols_def) {
        auto table = db.add_tables();
        table->set_name(table_name);
        auto array = std::data(cols_def);
        for (std::size_t i = 0; i < cols_def.size(); i += 2) {
            auto name = array[i];
            auto type = array[i + 1];
            auto col = table->add_columns();
            col->set_name(name);
            auto t = codec::DATA_TYPE_MAP.find(type);
            if (t == codec::DATA_TYPE_MAP.end()) {
                return false;
            }
            col->set_type(codec::SchemaCodec::ConvertType(t->second));
        }
        return true;
    }
};

TEST_F(DDLParserTest, physicalPlan) {
    std::string sp;
    ::hybridse::type::Database db;
    std::shared_ptr<hybridse::vm::CompileInfo> compile_info;
    // failed if sql is empty
    //    ASSERT_FALSE(DDLParser::GetPlan(sp, db, compile_info));

    sp = "SELECT  behaviourTable.itemId as itemId,  behaviourTable.ip as ip,  behaviourTable.query as query,  "
         "behaviourTable.mcuid as mcuid,  adinfo.brandName as name,  adinfo.brandId as brandId,  "
         "feedbackTable.actionValue as label FROM behaviourTable  LAST JOIN feedbackTable "
         "ON feedbackTable.itemId = behaviourTable.itemId  "
         "LAST JOIN adinfo ON behaviourTable.itemId = adinfo.id;";

    ASSERT_TRUE(AddTableToDB(
        db, "behaviourTable",
        {"itemId",    "string", "reqId",  "string",  "tags",   "string", "instanceKey", "string", "eventTime",
         "timestamp", "ip",     "string", "browser", "string", "query",  "string",      "mcuid",  "string",
         "weight",    "double", "page",   "int",     "rank",   "int",    "_i_rank",     "string"}));
    ASSERT_TRUE(AddTableToDB(
        db, "adinfo",
        {"id", "string", "ingestionTime", "timestamp", "brandName", "string", "name", "string", "brandId", "int"}));
    ASSERT_TRUE(AddTableToDB(db, "feedbackTable",
                             {"itemId", "string", "reqId", "string", "instanceKey", "string", "eventTime", "timestamp",
                              "ingestionTime", "timestamp", "actionValue", "int"}));

    //    ASSERT_TRUE(DDLParser::GetPlan(sp, db, compile_info));
    //    std::cout << "physical plan: " << std::endl;
    //    compile_info->DumpPhysicalPlan(std::cout, "\t");
    //    std::cout << std::endl;


    sp = "SELECT * FROM behaviourTable as t1 left join feedbackTable as t2 on t1.itemId = t2.itemId;";
    ASSERT_TRUE(DDLParser::GetPlan(sp, db, compile_info));
    //    std::cout << "physical plan: " << std::endl;
    //    compile_info->DumpPhysicalPlan(std::cout, "\t");
    //    std::cout << std::endl;
    //    DDLParser::Explain(sp, db);
    LOG(INFO) << "add some indexes..";
    // TODO(hw): add index(key=itemId)?

    // how about window?
    sp = "create procedure sp1(const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date)\n"
         " begin\n"
         "  SELECT id, name, sum(brandId) OVER w1 as w1_c4_sum\n"
         "  FROM adinfo\n"
         "  WINDOW w1 AS (PARTITION BY adinfo.id ORDER BY adinfo.ingestionTime ROWS BETWEEN 2 PRECEDING AND CURRENT "
         "ROW);\n"
         " end;  ";
    // TODO(hw):
}
}  // namespace openmldb::base

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
