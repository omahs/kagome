/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <wabt/binary-reader-ir.h>
#include <wabt/binary-writer.h>
#include <wabt/error-formatter.h>
#include <wabt/validator.h>
#include <wabt/wast-lexer.h>
#include <wabt/wast-parser.h>

#include "common/buffer.hpp"
#include "common/bytestr.hpp"
#include "runtime/wabt/error.hpp"

namespace kagome::runtime {
  WabtOutcome<void> wabtTry(auto &&f) {
    wabt::Errors errors;
    if (wabt::Failed(f(errors))) {
      return WabtError{
          wabt::FormatErrorsToString(errors, wabt::Location::Type::Binary)};
    }
    return outcome::success();
  }

  inline WabtOutcome<wabt::Module> wabtDecode(common::BufferView code) {
    wabt::Module module;
    OUTCOME_TRY(wabtTry([&](wabt::Errors &errors) {
      return wabt::ReadBinaryIr(
          "",
          code.data(),
          code.size(),
          wabt::ReadBinaryOptions({}, nullptr, true, false, false),
          &errors,
          &module);
    }));
    return module;
  }

  inline WabtOutcome<void> wabtValidate(const wabt::Module &module) {
    return wabtTry([&](wabt::Errors &errors) {
      return wabt::ValidateModule(&module, &errors, wabt::ValidateOptions{});
    });
  }

  inline WabtOutcome<common::Buffer> wabtEncode(const wabt::Module &module) {
    wabt::MemoryStream s;
    if (wabt::Failed(wabt::WriteBinaryModule(
            &s, &module, wabt::WriteBinaryOptions({}, false, false, true)))) {
      return WabtError{"Failed to serialize WASM module"};
    }
    return common::Buffer{std::move(s.output_buffer().data)};
  }

  std::unique_ptr<wabt::Module> wat_to_module(std::span<const uint8_t> wat) {
    wabt::Result result;
    wabt::Errors errors;
    std::unique_ptr<wabt::WastLexer> lexer =
        wabt::WastLexer::CreateBufferLexer("", wat.data(), wat.size(), &errors);
    if (Failed(result)) {
      throw std::runtime_error{"Failed to parse WAT"};
    }

    std::unique_ptr<wabt::Module> module;
    wabt::WastParseOptions parse_wast_options{{}};
    result = wabt::ParseWatModule(
        lexer.get(), &module, &errors, &parse_wast_options);
    if (Failed(result)) {
      throw std::runtime_error{"Failed to parse module"};
    }
    return module;
  }

  std::vector<uint8_t> wat_to_wasm(std::string_view wat) {
    auto module = wat_to_module(kagome::str2byte(wat));
    wabt::MemoryStream stream;
    if (wabt::Failed(wabt::WriteBinaryModule(
            &stream,
            module.get(),
            wabt::WriteBinaryOptions{{}, true, false, true}))) {
      throw std::runtime_error{"Failed to write binary wasm"};
    }
    return std::move(stream.output_buffer().data);
  }

}  // namespace kagome::runtime
