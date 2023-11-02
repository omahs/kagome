/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "runtime/runtime_api/sassafras_api.hpp"

namespace kagome::runtime {

  class Executor;

  class SassafrasApiImpl final : public SassafrasApi {
   public:
    explicit SassafrasApiImpl(std::shared_ptr<Executor> executor);

    /// Get ring context to be used for ticket construction and verification.
    outcome::result<
        std::optional<consensus::sassafras::bandersnatch::RingContext>>
    ring_context(const primitives::BlockHash &block) override;

    /// Submit next epoch validator tickets via an unsigned extrinsic.
    /// This method returns `false` when creation of the extrinsics fails.
    outcome::result<bool> submit_tickets_unsigned_extrinsic(
        const primitives::BlockHash &block,
        const std::vector<consensus::sassafras::TicketEnvelope> &tickets)
        override;

    /// Get ticket id associated to the given slot.
    outcome::result<std::optional<consensus::sassafras::TicketId>>
    slot_ticket_id(const primitives::BlockHash &block,
                   consensus::SlotNumber slot) override;

    /// Get ticket id and data associated to the given slot.
    outcome::result<std::optional<std::tuple<consensus::sassafras::TicketId,
                                             consensus::sassafras::TicketBody>>>
    slot_ticket(const primitives::BlockHash &block,
                consensus::SlotNumber slot) override;

    /// Current epoch information.
    outcome::result<consensus::sassafras::Epoch> current_epoch(
        const primitives::BlockHash &block) override;

    /// Next epoch information.
    outcome::result<consensus::sassafras::Epoch> next_epoch(
        const primitives::BlockHash &block) override;

    /// Generates a proof of key ownership for the given authority in the
    /// current epoch.
    ///
    /// An example usage of this module is coupled with the session historical
    /// module to prove that a given authority key is tied to a given staking
    /// identity during a specific session.
    ///
    /// Proofs of key ownership are necessary for submitting equivocation
    /// reports.
    outcome::result<
        std::optional<consensus::sassafras::OpaqueKeyOwnershipProof>>
    generate_key_ownership_proof(
        const primitives::BlockHash &block,
        const primitives::AuthorityId &authority_id) override;

    /// Submits an unsigned extrinsic to report an equivocation.
    ///
    /// The caller must provide the equivocation proof and a key ownership proof
    /// (should be obtained using `generate_key_ownership_proof`). The extrinsic
    /// will be unsigned and should only be accepted for local authorship (not
    /// to be broadcast to the network). This method returns `false` when
    /// creation of the extrinsic fails.
    ///
    /// Only useful in an offchain context.
    outcome::result<bool> submit_report_equivocation_unsigned_extrinsic(
        const primitives::BlockHash &block,
        const consensus::EquivocationProof &equivocation_proof,
        const consensus::sassafras::OpaqueKeyOwnershipProof &key_owner_proof)
        override;

   private:
    std::shared_ptr<Executor> executor_;
  };

}  // namespace kagome::runtime