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

#ifndef SRC_TABLET_DATA_RECEIVER_H
#define SRC_TABLET_DATA_RECEIVER_H

namespace fedb {
namespace tablet {
// TODO(hw): Create a abstract receiver to manage received data. FileReceiver can inherited from it too.
class DataReceiver {
 public:
    DataReceiver(uint32_t tid, uint32_t pid) : tid_(tid), pid_(pid) {}

 private:
    uint32_t tid_;
    uint32_t pid_;
};

}  // namespace tablet
}  // namespace fedb
#endif  // SRC_TABLET_DATA_RECEIVER_H
