/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "common/visitor.hpp"
#include "consensus/grandpa/common.hpp"
#include "primitives/block_header.hpp"
#include "primitives/common.hpp"

namespace kagome::consensus::grandpa {

  using Precommit = primitives::detail::BlockInfoT<struct PrecommitTag>;
  using Prevote = primitives::detail::BlockInfoT<struct PrevoteTag>;
  using PrimaryPropose =
      primitives::detail::BlockInfoT<struct PrimaryProposeTag>;

  /// Note: order of types in variant matters
  using Vote = boost::variant<Prevote,          // 0
                              Precommit,        // 1
                              PrimaryPropose>;  // 2

  struct SignedMessage {
    Vote message;
    Signature signature;
    Id id;

    BlockNumber getBlockNumber() const {
      return visit_in_place(message,
                            [](const auto &vote) { return vote.number; });
    }

    BlockHash getBlockHash() const {
      return visit_in_place(message,
                            [](const auto &vote) { return vote.hash; });
    }

    primitives::BlockInfo getBlockInfo() const {
      return visit_in_place(message, [](const auto &vote) {
        return primitives::BlockInfo(vote.number, vote.hash);
      });
    }

    template <typename T>
    bool is() const {
      return message.type() == typeid(T);
    }

    bool operator==(const SignedMessage &rhs) const {
      return message == rhs.message && signature == rhs.signature
          && id == rhs.id;
    }

    bool operator!=(const SignedMessage &rhs) const {
      return !operator==(rhs);
    }
  };

  template <class Stream>
    requires Stream::is_encoder_stream
  Stream &operator<<(Stream &s, const SignedMessage &signed_msg) {
    return s << signed_msg.message << signed_msg.signature << signed_msg.id;
  }

  template <class Stream>
    requires Stream::is_decoder_stream
  Stream &operator>>(Stream &s, SignedMessage &signed_msg) {
    return s >> signed_msg.message >> signed_msg.signature >> signed_msg.id;
  }

  using EquivocatorySignedMessage = std::pair<SignedMessage, SignedMessage>;
  using VoteVariant = boost::variant<SignedMessage, EquivocatorySignedMessage>;

  class SignedPrevote : public SignedMessage {
    using SignedMessage::SignedMessage;
  };

  template <class Stream>
    requires Stream::is_encoder_stream
  Stream &operator<<(Stream &s, const SignedPrevote &signed_msg) {
    assert(signed_msg.template is<Prevote>());
    return s << boost::strict_get<Prevote>(signed_msg.message)
             << signed_msg.signature << signed_msg.id;
  }

  template <typename Stream>
    requires Stream::is_decoder_stream
  Stream &operator>>(Stream &s, SignedPrevote &signed_msg) {
    signed_msg.message = Prevote{};
    return s >> boost::strict_get<Prevote>(signed_msg.message)
        >> signed_msg.signature >> signed_msg.id;
  }

  class SignedPrecommit : public SignedMessage {
    using SignedMessage::SignedMessage;
  };

  template <class Stream>
    requires Stream::is_encoder_stream
  Stream &operator<<(Stream &s, const SignedPrecommit &signed_msg) {
    assert(signed_msg.template is<Precommit>());
    return s << boost::strict_get<Precommit>(signed_msg.message)
             << signed_msg.signature << signed_msg.id;
  }

  template <class Stream>
    requires Stream::is_decoder_stream
  Stream &operator>>(Stream &s, SignedPrecommit &signed_msg) {
    signed_msg.message = Precommit{};
    return s >> boost::strict_get<Precommit>(signed_msg.message)
        >> signed_msg.signature >> signed_msg.id;
  }

  // justification that contains a list of signed precommits justifying the
  // validity of the block
  struct GrandpaJustification {
    SCALE_TIE(4);

    RoundNumber round_number;
    primitives::BlockInfo block_info;
    std::vector<SignedPrecommit> items{};
    std::vector<primitives::BlockHeader> votes_ancestries{};
  };

  // either prevote, precommit or primary propose
  struct VoteMessage {
    SCALE_TIE(3);

    RoundNumber round_number{0};
    VoterSetId counter{0};
    SignedMessage vote;

    Id id() const {
      return vote.id;
    }
  };

  struct TotalWeight {
    uint64_t prevote = 0;
    uint64_t precommit = 0;
  };

  // A commit message with compact representation of authentication data.
  // @See
  // https://github.com/paritytech/finality-grandpa/blob/v0.14.2/src/lib.rs#L312
  struct CompactCommit {
    SCALE_TIE(4);

    // The target block's hash.
    primitives::BlockHash target_hash;
    // The target block's number.
    primitives::BlockNumber target_number;
    // Precommits for target block or any block after it that justify this
    // commit.
    std::vector<Precommit> precommits{};
    // Authentication data for the commit.
    std::vector<std::pair<Signature, Id>> auth_data{};
  };
}  // namespace kagome::consensus::grandpa
