/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include "outcome/outcome.hpp"

namespace kagome::pvm {

  enum class Error : uint8_t {
    NOT_IMPLEMENTED,
    MAGIC_PREFIX_MESSED,
    NOT_ENOUGH_DATA,
    UNSUPPORTED_VERSION,
    MEMORY_CONFIG_SECTION_DUPLICATED,
    FAILED_TO_READ_UVARINT,
    MEMORY_SECTION_HAS_INCORRECT_SIZE,
    RO_DATA_SECTION_DUPLICATED,
    RW_DATA_SECTION_DUPLICATED,
    IMPORT_OFFSETS_SECTION_DUPLICATED,
    IMPORT_SYMBOLS_SECTION_DUPLICATED,
    TOO_MANY_IMPORTS,
    UNEXPECTED_END_OF_FILE,
    IMPORT_SECTION_CORRUPTED,
  };

  template <typename T>
  using Result = outcome::result<T>;

}  // namespace kagome::pvm

OUTCOME_HPP_DECLARE_ERROR(kagome::pvm, Error);