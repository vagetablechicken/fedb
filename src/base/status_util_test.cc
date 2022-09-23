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

#include "base/status_util.h"

#include <iostream>
#include <string>

#include "gtest/gtest.h"
#include "base/fe_status.h"

namespace openmldb {
namespace base {
using hybridse::common::StatusCode;
class StatusUtilTest : public ::testing::Test {
 public:
    StatusUtilTest() {}
    ~StatusUtilTest() {}
};

TEST_F(StatusUtilTest, fromBase) {
    ::hybridse::sdk::Status status;
    ::hybridse::base::Status base_status(StatusCode::kCmdError, "no ok");
    std::string abc = "abc";
    APPEND_FROM_BASE_AND_WARN(&status, base_status, "pre1" + abc);

    CODE_PREPEND_AND_WARN(&status, StatusCode::kNoDatabase, "pre2");
    CODE_APPEND_AND_WARN(&status, StatusCode::kNoDatabase, "app1");

    SET_STATUS_AND_WARN(&status, StatusCode::kCmdError, "a new error");
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
