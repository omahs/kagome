/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include "consensus/sassafras/impl/sassafras_block_validator_impl.hpp"

#include <latch>

#include "consensus/sassafras/impl/sassafras_digests_util.hpp"
#include "consensus/sassafras/impl/sassafras_vrf.hpp"
#include "consensus/sassafras/sassafras_config_repository.hpp"
#include "consensus/sassafras/types/seal.hpp"
#include "consensus/timeline/impl/slot_leadership_error.hpp"
#include "consensus/timeline/slots_util.hpp"
#include "crypto/bandersnatch_provider.hpp"
#include "crypto/ed25519_provider.hpp"
#include "crypto/vrf_provider.hpp"
#include "metrics/histogram_timer.hpp"
#include "primitives/inherent_data.hpp"
#include "runtime/runtime_api/offchain_worker_api.hpp"
#include "storage/trie/serialization/ordered_trie_hash.hpp"
#include "threshold_util.hpp"

OUTCOME_CPP_DEFINE_CATEGORY(kagome::consensus::sassafras,
                            SassafrasBlockValidatorImpl::ValidationError,
                            e) {
  using E = kagome::consensus::sassafras::SassafrasBlockValidatorImpl::
      ValidationError;
  switch (e) {
    case E::NO_AUTHORITIES:
      return "no authorities are provided for the validation";
    case E::INVALID_SIGNATURE:
      return "SR25519 signature, which is in BABE header, is invalid";
    case E::INVALID_VRF:
      return "VRF value and output are invalid";
    case E::TWO_BLOCKS_IN_SLOT:
      return "peer tried to distribute several blocks in one slot";
    case E::WRONG_AUTHOR_OF_SECONDARY_CLAIM:
      return "Wrong author of secondary claim of slot";
  }
  return "unknown error";
}

namespace kagome::consensus::sassafras {

  SassafrasBlockValidatorImpl::SassafrasBlockValidatorImpl(
      LazySPtr<SlotsUtil> slots_util,
      std::shared_ptr<SassafrasConfigRepository> config_repo,
      std::shared_ptr<crypto::Hasher> hasher,
      std::shared_ptr<crypto::BandersnatchProvider> bandersnatch_provider,
      std::shared_ptr<crypto::Ed25519Provider> ed25519_provider,
      std::shared_ptr<crypto::VRFProvider> vrf_provider)
      : log_(log::createLogger("SassafrasBlockValidator", "sassafras")),
        slots_util_(std::move(slots_util)),
        config_repo_(std::move(config_repo)),
        hasher_(std::move(hasher)),
        bandersnatch_provider_(std::move(bandersnatch_provider)),
        ed25519_provider_(std::move(ed25519_provider)),
        vrf_provider_(std::move(vrf_provider)) {
    BOOST_ASSERT(config_repo_);
    BOOST_ASSERT(hasher_);
    BOOST_ASSERT(bandersnatch_provider_);
    BOOST_ASSERT(ed25519_provider_);
    BOOST_ASSERT(vrf_provider_);
  }

  outcome::result<void> SassafrasBlockValidatorImpl::validateHeader(
      const primitives::BlockHeader &header) const {
    SL_TRACE(log_, "Validating header of block {}...", header.blockInfo());

    // get Sassafras-specific digests, which must be inside this block
    OUTCOME_TRY(slot_claim, getSlotClaim(header));
    OUTCOME_TRY(seal, getSeal(header));

    auto slot = slot_claim.slot_number;

    OUTCOME_TRY(epoch,
                slots_util_.get()->slotToEpoch(*header.parentInfo(), slot));

    OUTCOME_TRY(config_ptr, config_repo_->config(*header.parentInfo(), epoch));
    auto &config = *config_ptr;

    SL_VERBOSE(log_,
               "Validating header of block {}: "
               "{} claim of slot {}, epoch {}, authority #{}",
               header.blockInfo(),
               slot_claim.ticket_claim ? "primary" : "secondary",
               slot,
               epoch,
               slot_claim.authority_index);

    SL_TRACE(log_,
             "Actual epoch digest to apply block {} (slot {}, epoch {}). "
             "Randomness: {}",
             header.blockInfo(),
             slot,
             epoch,
             config.randomness);

    if (slot_claim.ticket_claim.has_value()) {
      OUTCOME_TRY(verifyPrimaryClaim(slot_claim, config));
    } else {
      OUTCOME_TRY(verifySecondaryClaim(slot_claim, config));
    }

    // signature in seal of the header must be valid
    if (!verifySignature(header,
                         seal.signature,
                         config.authorities[slot_claim.authority_index])) {
      return ValidationError::INVALID_SIGNATURE;
    }

    return outcome::success();
  }

  bool SassafrasBlockValidatorImpl::verifySignature(
      const primitives::BlockHeader &header,
      const Signature &signature,
      const Authority &public_key) const {
    primitives::UnsealedBlockHeaderReflection unsealed_header(header);

    auto unsealed_header_encoded = scale::encode(unsealed_header).value();

    auto signed_hash = hasher_->blake2b_256(unsealed_header_encoded);

    // secondly, use verify function to check the signature
    auto res =
        bandersnatch_provider_->verify(signature, signed_hash, public_key);
    return res && res.value();
  }

  outcome::result<void> SassafrasBlockValidatorImpl::verifyPrimaryClaim(
      const SlotClaim &claim, const Epoch &config) const {
    TicketBody ticket_body;  // TODO ???

    auto epoch_blob = common::uint64_to_be_bytes(config.epoch_index);
    auto slot_blob = common::uint32_to_be_bytes(claim.slot_number);
    auto attempt_blob = common::uint32_to_be_bytes(ticket_body.attempt_index);

    std::vector<vrf::BytesIn> b1{config.randomness, epoch_blob, slot_blob};

    auto randomness_vrf_input =
        vrf::vrf_input_from_data("sassafras-randomness-v1.0"_bytes, b1);

    std::vector<vrf::BytesIn> b2{config.randomness, epoch_blob, attempt_blob};

    auto revealed_vrf_input =
        vrf::vrf_input_from_data("sassafras-revealed-v1.0"_bytes, b2);

    auto encoded_ticket_body = scale::encode(ticket_body).value();
    std::vector<vrf::BytesIn> transcript_data{encoded_ticket_body};
    std::vector<vrf::VrfInput> vrf_inputs{randomness_vrf_input,
                                          revealed_vrf_input};

    auto sign_data = vrf::vrf_sign_data(
        "sassafras-claim-v1.0"_bytes, transcript_data, vrf_inputs);

    const auto &revealed_vrf_output = claim.signature.outputs[1];

    auto revealed_seed =
        vrf::make_bytes<32>(revealed_vrf_input, revealed_vrf_output);

    auto revealed_pub =
        ed25519_provider_
            ->generateKeypair(crypto::Ed25519Seed{revealed_seed}, {})
            .value()
            .public_key;

    return outcome::success();  // FIXME
  }

  outcome::result<void> SassafrasBlockValidatorImpl::verifySecondaryClaim(
      const SlotClaim &claim, const Epoch &config) const {
    auto auth_index_of_leader =
        le_bytes_to_uint64(hasher_->blake2b_64(
            scale::encode(config.randomness, claim.slot_number).value()))
        % config.authorities.size();

    if (claim.authority_index != auth_index_of_leader) {
      return ValidationError::WRONG_AUTHOR_OF_SECONDARY_CLAIM;
    }

    return outcome::success();
  }

}  // namespace kagome::consensus::sassafras
