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

    // can't handle duplicate table names
    static int GetTableIdxInDB(::hybridse::type::Database& db, const std::string& table_name) {
        for (int i = 0; i < db.tables_size(); ++i) {
            if (db.tables(i).name() == table_name) {
                return i;
            }
        }
        return -1;
    }

 protected:
    ::hybridse::type::Database db;
};

// TODO(hw): split into units
TEST_F(DDLParserTest, physicalPlan) {
    std::string sp;

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
        db, "behaviourTable2",
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
    //    ASSERT_TRUE(DDLParser::GetPlan(sp, db, compile_info));
    //    std::cout << "physical plan: " << std::endl;
    //    compile_info->DumpPhysicalPlan(std::cout, "\t");
    //    std::cout << std::endl;
    //    DDLParser::Explain(sp, db);

    // 找一个可以证明add index有好处的？plan如何看待index？可能要把session Run给看懂
    // create table_def behaviourTable(itemId string, eventTime timestamp, rank int, index(key=rank,ts=eventTime));
    /*SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (PARTITION BY itemId ORDER BY
    eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);*/
    // index不能高效查询，所以会被performance sensitive mode拦住
    // 而create index index1 on behaviourTable(itemId) OPTIONS(ts=eventTime,ttl=1000, ttl_type=latest); 后可以查询。
    sp = "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
         "PARTITION BY itemId ORDER BY "
         "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    // TODO(hw): union's provider table->partition

    LOG(INFO) << "try explain";
    DDLParser::Explain(sp, db);
    auto idx = GetTableIdxInDB(db, "behaviourTable");
    auto table_def = db.mutable_tables(idx);
    LOG(INFO) << "add some indexes to table_def " << table_def->name();
    auto add_index = table_def->add_indexes();
    add_index->set_name("index1");
    add_index->add_first_keys("itemId");
    add_index->set_second_key("eventTime");  // ts
    LOG(INFO) << "test " << db.tables(0).indexes_size();

    // TODO(hw): for physical plan, ttl and type is useless, but parser needs.
    //    add_index->add_ttl(1000);
    //    add_index->set_ttl_type();

    // db add TableDef for table_def, simple catalog can load it
    DDLParser::Explain("create procedure sp1() begin " + sp + " end;", db);
    // create table_def t1 (id int,c1 string,c3 int,c4 bigint,c5 float,c6 double,c7 timestamp,c8 date,
    // index(key=c3,ts=c7)); SELECT id, c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM t1 WINDOW w1 AS (UNION t2 PARTITION BY
    // t1.c3 ORDER BY t1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);

    // how about window union?
    // TODO(hw): physical plan should be:
    //"LIMIT(limit=10, optimized)\n"
    //"  PROJECT(type=Aggregation, limit=10)\n"
    //"    REQUEST_UNION(partition_keys=(), orders=(ASC), range=(col5, "
    //"-3, 0), index_keys=(col1,col2))\n"
    //"      +-UNION(partition_keys=(col1), orders=(ASC), range=(col5, "
    //"-3, 0), index_keys=(col2))\n"
    //"          DATA_PROVIDER(type=Partition, table=t3, "
    //"index=index2_t3)\n"
    //"      DATA_PROVIDER(request=t1)\n"
    //"      DATA_PROVIDER(type=Partition, table=t1, index=index12)"

    sp = "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1 WINDOW w1 AS (UNION t3 PARTITION BY col1,col2 "
         "ORDER BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;";
    ASSERT_TRUE(AddTableToDB(db, "t1",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    ASSERT_TRUE(AddTableToDB(db, "t3",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    DDLParser::Explain(sp, db);
    idx = GetTableIdxInDB(db, "t1");
    table_def = db.mutable_tables(idx);
    add_index = table_def->add_indexes();
    add_index->set_name("index1_2_t1");
    add_index->add_first_keys("col1");
    add_index->add_first_keys("col2");
    add_index->set_second_key("col5");

    idx = GetTableIdxInDB(db, "t3");
    table_def = db.mutable_tables(idx);
    add_index = table_def->add_indexes();
    add_index->set_name("index1_2_t3");
    add_index->add_first_keys("col1");
    add_index->add_first_keys("col2");
    add_index->set_second_key("col5");
    DDLParser::Explain(sp, db);

    LOG(INFO) << "union select";
    sp = "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1 WINDOW w1 AS (UNION (select col0, col1,  col2, "
         "0.0f as col3, 0.0 as col4, col5, col6 from t3) PARTITION BY col1,col2 ORDER "
         "BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;";
    DDLParser::Explain(sp, db);

    LOG(INFO) << "more complicated";
    sp = "SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM (select col0, col1,  col2,"
         "0.0f as col3, 0.0 as col4, col5, col6 from t1) WINDOW w1 AS (UNION (select col0, col1,  col2, "
         "0.0f as col3, 0.0 as col4, col5, col6 from t3) PARTITION BY col1,col2 ORDER "
         "BY col5 ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;";
    DDLParser::Explain(sp, db);
}

TEST_F(DDLParserTest, lastJoin) {
    auto sql =
        "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join  t2 order by t2.col5 on t1.col1 = t2.col2 "
        "and t2.col5 >= t1.col5;";
    ASSERT_TRUE(AddTableToDB(db, "t1",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    ASSERT_TRUE(AddTableToDB(db, "t2",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    DDLParser::Explain(sql, db);
    // transform.cc:1433] Before optimization:
    // SIMPLE_PROJECT(sources=(t1.col1 -> t1_col1, t2.col2 -> t2_col2))
    //   REQUEST_JOIN(type=LastJoin, right_sort=(t2.col5 ASC), condition=t1.col1 = t2.col2 AND t2.col5 >= t1.col5,
    //   left_keys=, right_keys=, index_keys=)
    //     DATA_PROVIDER(request=t1)
    //     DATA_PROVIDER(table=t2)
    // group_and_sort_optimized.cc:439] keys and order optimized: keys=(t2.col2), order=(t2.col5 ASC)

    // so add index on t2 (key=col2, ts=col5)
    auto idx = GetTableIdxInDB(db, "t2");
    auto table_def = db.mutable_tables(idx);
    auto add_index = table_def->add_indexes();
    add_index->set_name("index1_t2");
    add_index->add_first_keys("col2");
    add_index->set_second_key("col5");
    DDLParser::Explain(sql, db);
}

TEST_F(DDLParserTest, leftJoin) {
    auto sql =
        "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on "
        "t1.col1 = t2.col2;";
    ASSERT_TRUE(AddTableToDB(db, "t1",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    ASSERT_TRUE(AddTableToDB(db, "t2",
                             {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                              "col5", "int64", "col6", "string"}));
    DDLParser::Explain(sql, db);

    // only have key, no order
    auto idx = GetTableIdxInDB(db, "t2");
    auto table_def = db.mutable_tables(idx);
    auto add_index = table_def->add_indexes();
    add_index->set_name("index1_t2");
    add_index->add_first_keys("col2");
    add_index->set_second_key("col5"); // any int64 col
    DDLParser::Explain(sql, db);
}
}  // namespace openmldb::base

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
