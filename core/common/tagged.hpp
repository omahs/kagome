/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef KAGOME_TAGGED
#define KAGOME_TAGGED

#include <type_traits>
#include <utility>

#include <scale/scale.hpp>

namespace kagome {

  template <typename T, typename = std::enable_if<std::is_scalar_v<T>>>
  struct Wrapper {
    template <typename... Args>
    Wrapper(Args &&...args) : value(std::forward<T>(args)...) {}

   protected:
    T value;
  };

  template <typename T,
            typename Tag,
            typename Base =
                std::conditional_t<std::is_scalar_v<T>, Wrapper<T>, T>>
  class Tagged : public Base {
   public:
    typedef Tag tag;

    template <typename... Args>
    explicit Tagged(Args &&...args) : Base(std::forward<Args>(args)...) {}

    Tagged(T &&value) noexcept(not std::is_lvalue_reference_v<decltype(value)>)
        : Base(std::forward<T>(value)) {}

    Tagged &operator=(T &&value) noexcept(
        not std::is_lvalue_reference_v<decltype(value)>) {
      if constexpr (std::is_scalar_v<T>) {
        this->Wrapper<T>::value = std::forward<T>(value);
      } else {
        static_cast<Base &>(*this) = std::forward<T>(value);
      }
      return *this;
    }

    template <typename Out>
    explicit operator Out() {
      if constexpr (std::is_scalar_v<T>) {
        return this->Wrapper<T>::value;
      } else {
        return *this;
      }
    }

   private:
    friend inline ::scale::ScaleEncoderStream &operator<<(
        ::scale::ScaleEncoderStream &s, const Tagged<T, Tag> &tagged) {
      if constexpr (std::is_scalar_v<T>) {
        return s << tagged.Wrapper<T>::value;
      } else {
        return s << static_cast<const T &>(tagged);
      }
    }

    friend inline ::scale::ScaleDecoderStream &operator>>(
        ::scale::ScaleDecoderStream &s, Tagged<T, Tag> &tagged) {
      if constexpr (std::is_scalar_v<T>) {
        return s >> tagged.Wrapper<T>::value;
      } else {
        return s >> static_cast<T &>(tagged);
      }
    }
  };

}  // namespace kagome

#endif  // KAGOME_TAGGED