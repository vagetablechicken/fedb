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

#include "gtest/gtest.h"

namespace openmldb::base {
class DDLParserTest : public ::testing::Test {
 public:
    void SetUp() override {
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
                                 {"itemId", "string", "reqId", "string", "instanceKey", "string", "eventTime",
                                  "timestamp", "ingestionTime", "timestamp", "actionValue", "int"}));

        ASSERT_TRUE(AddTableToDB(db, "t1",
                                 {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                                  "col5", "int64", "col6", "string"}));
        ASSERT_TRUE(AddTableToDB(db, "t2",
                                 {"col0", "string", "col1", "int32", "col2", "int16", "col3", "float", "col4", "double",
                                  "col5", "int64", "col6", "string"}));
    }

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

// create procedure: only inner plan will be sql compiled.
TEST_F(DDLParserTest, createSpExtractIndexes) {
    std::string query =
        "SELECT sum(rank) OVER w1 as w1_rank_sum FROM behaviourTable as t1 WINDOW w1 AS (UNION behaviourTable2 "
        "PARTITION BY itemId ORDER BY "
        "eventTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    auto query_map = DDLParser::ExtractIndexes(query, db);

    auto sp_map = DDLParser::ExtractIndexes("create procedure sp1() begin " + query + " end;", db);

    ASSERT_EQ(query_map.size(), sp_map.size());
    LOG(INFO) << "query indexes " << query_map;
    LOG(INFO) << "sp indexes " << sp_map;
}

TEST_F(DDLParserTest, joinExtract) {
    {
        // last join
        auto sql =
            "SELECT t1.col1 as t1_col1, t2.col2 as t2_col2 FROM t1 last join t2 order by t2.col5 on t1.col1 = t2.col2 "
            "and t2.col5 >= t1.col5;";

        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;

        // so add index on t2 (key=col2, ts=col5)
        // TODO(hw): create index from index_map
        auto idx = GetTableIdxInDB(db, "t2");
        auto table_def = db.mutable_tables(idx);
        auto add_index = table_def->add_indexes();
        add_index->set_name("index1_t2");
        add_index->add_first_keys("col2");
        add_index->set_second_key("col5");
        // TODO(hw): check data provider type
        LOG(INFO) << "after add index: " << DDLParser::Explain(sql, db);
    }

    {
        // left join
        auto sql =
            "SELECT t1.col1, t1.col2, t2.col1, t2.col2 FROM t1 left join t2 on "
            "t1.col1 = t2.col3;";  // avoid index1_t2

        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
        // only have key, no order
        auto idx = GetTableIdxInDB(db, "t2");
        auto table_def = db.mutable_tables(idx);
        auto add_index = table_def->add_indexes();
        add_index->set_name("index2_t2");
        add_index->add_first_keys("col3");
        add_index->set_second_key("col5");  // no order by, use any int64 col
        LOG(INFO) << "after add index: " << DDLParser::Explain(sql, db);
    }
}

TEST_F(DDLParserTest, emptyIndexes) {
    {
        // request data provider, won't get indexes.
        auto sql =
            "SELECT sum(col1) as col1sum FROM (select col1, col2, "
            "col3 from t1) where col1 = 10 and col2 = 20 group by col2, col1;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_TRUE(index_map.empty());
    }
}

TEST_F(DDLParserTest, windowExtractIndexes) {
    {
        // abs 3d
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3d "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
    }
    
    {
        // abs < 1min preceding -> 1min start
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 "
            "ROWS_RANGE BETWEEN 3s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
    }

    {
        // latest
        auto sql =
            "SELECT "
            "col1, "
            "sum(col3) OVER w1 as w1_col3_sum, "
            "sum(col2) OVER w1 as w1_col2_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 "
            "ROWS BETWEEN 3 "
            "PRECEDING AND CURRENT ROW) limit 10;";
        auto index_map = DDLParser::ExtractIndexes(sql, db);
        ASSERT_FALSE(index_map.empty());
        LOG(INFO) << index_map;
    }

    {
        // no order by
        auto sql = "SELECT sum(col1) as col1sum FROM t1 group by col3, col2, col1;";
        auto index_map = DDLParser::ExtractIndexesForBatch(sql, db);
        LOG(INFO) << "result: " << index_map;

        index_map = DDLParser::ExtractIndexes(sql, db);
        LOG(INFO) << "result: " << index_map;
    }
}
}  // namespace openmldb::base

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
