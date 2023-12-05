/**
 * Copyright Quadrivium LLC All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <ranges>

#include "testutil/literals.hpp"
#include "testutil/outcome.hpp"

#include "runtime/common/runtime_instances_pool.hpp"

#include "mock/core/runtime/module_factory_mock.hpp"
#include "mock/core/runtime/module_instance_mock.hpp"
#include "mock/core/runtime/module_mock.hpp"

using kagome::common::Buffer;
using kagome::runtime::ModuleFactoryMock;
using kagome::runtime::ModuleInstanceMock;
using kagome::runtime::ModuleMock;
using kagome::runtime::RuntimeInstancesPool;

RuntimeInstancesPool::CodeHash make_code_hash(int i) {
  return RuntimeInstancesPool::CodeHash::fromString(
             fmt::format("{: >32}", fmt::format("code_hash_{:0>3}", i)))
      .value();
}

TEST(InstancePoolTest, HeavilyMultithreadedCompilation) {
  using namespace std::chrono_literals;

  auto module_instance_mock = std::make_shared<ModuleInstanceMock>();

  auto module_mock = std::make_shared<ModuleMock>();
  ON_CALL(*module_mock, instantiate())
      .WillByDefault(testing::Return(module_instance_mock));

  std::atomic_int times_make_called{};

  auto module_factory = std::make_shared<ModuleFactoryMock>();
  const Buffer code = "runtime_code"_buf;
  ON_CALL(*module_factory, make(code.view()))
      .WillByDefault(testing::Invoke([module_mock, &times_make_called](auto) {
        std::this_thread::sleep_for(1s);
        times_make_called++;
        return module_mock;
      }));

  static constexpr int THREAD_NUM = 100;
  static constexpr int POOL_SIZE = 10;

  RuntimeInstancesPool pool{module_factory, POOL_SIZE};

  std::vector<std::thread> threads;
  for (int i = 0; i < THREAD_NUM; i++) {
    threads.emplace_back(std::thread([&pool, &code, i]() {
      ASSERT_OUTCOME_SUCCESS_TRY(
          pool.instantiateFromCode(make_code_hash(i % POOL_SIZE), code));
    }));
  }

  for (auto &t : threads) {
    t.join();
  }

  // check that 'make' was only called 5 times
  ASSERT_EQ(times_make_called.load(), POOL_SIZE);

  // check that all POOL_SIZE instances are in cache
  for (int i = 0; i < POOL_SIZE; i++) {
    ASSERT_OUTCOME_SUCCESS_TRY(
        pool.instantiateFromCode(make_code_hash(i), code.view()));
  }
  ASSERT_EQ(times_make_called.load(), POOL_SIZE);
}
