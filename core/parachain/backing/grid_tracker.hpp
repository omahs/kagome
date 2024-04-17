/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <bitset>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "network/types/collator_messages_vstaging.hpp"
#include "parachain/backing/grid.hpp"
#include "parachain/groups.hpp"
#include "parachain/types.hpp"

template <>
struct std::hash<std::pair<kagome::parachain::ValidatorIndex,
                           kagome::network::vstaging::CompactStatement>> {
  size_t operator()(const std::pair<kagome::parachain::ValidatorIndex,
                                    kagome::network::vstaging::CompactStatement>
                        &__p) const noexcept {
    size_t result = std::hash<kagome::parachain::ValidatorIndex>()(__p.first);
    auto hash = kagome::visit_in_place(
        __p.second.inner_value,
        [](const kagome::network::vstaging::Empty &) {
          return kagome::parachain::Hash{};
        },
        [](const auto &v) { return v.hash; });

    boost::hash_combine(result, std::hash<kagome::parachain::Hash>()(hash));
    return result;
  }
};

namespace kagome::parachain::grid {

  using SessionTopologyView = Views;

  enum class ManifestKind {
    Full,
    Acknowledgement,
  };

  struct ManifestSummary {
    Hash claimed_parent_hash;
    GroupIndex claimed_group_index;
    network::vstaging::StatementFilter statement_knowledge;
  };

  class ReceivedManifests {
    std::unordered_map<CandidateHash, ManifestSummary> received;
    std::unordered_map<GroupIndex, std::vector<size_t>> seconded_counts;

   public:
    std::optional<network::vstaging::StatementFilter>
    candidate_statement_filter(const CandidateHash &candidate_hash);

    outcome::result<void> import_received(
        size_t group_size,
        size_t seconding_limit,
        const CandidateHash &candidate_hash,
        const ManifestSummary &manifest_summary);

   private:
    bool updating_ensure_within_seconding_limit(
        std::unordered_map<GroupIndex, std::vector<size_t>> &seconded_counts,
        GroupIndex group_index,
        size_t group_size,
        size_t seconding_limit,
        const std::vector<bool> &fresh_seconded);
  };

  struct MutualKnowledge {
    std::optional<network::vstaging::StatementFilter> remote_knowledge;
    std::optional<network::vstaging::StatementFilter> local_knowledge;
    std::optional<network::vstaging::StatementFilter> received_knowledge;
  };

  struct KnownBackedCandidate {
    size_t group_index;
    network::vstaging::StatementFilter local_knowledge;
    std::unordered_map<ValidatorIndex, MutualKnowledge> mutual_knowledge;

    bool has_received_manifest_from(size_t validator);
    bool has_sent_manifest_to(size_t validator);
    bool note_fresh_statement(
        size_t statement_index_in_group,
        const network::vstaging::StatementKind &statement_kind);
    bool is_pending_statement(
        size_t validator,
        size_t statement_index_in_group,
        const network::vstaging::StatementKind &statement_kind);

    void manifest_sent_to(
        size_t validator,
        const network::vstaging::StatementFilter &local_knowledge);
    void manifest_received_from(
        ValidatorIndex validator,
        const network::vstaging::StatementFilter &remote_knowledge);

    void sent_or_received_direct_statement(
        ValidatorIndex validator,
        size_t statement_index_in_group,
        const network::vstaging::StatementKind &statement_kind,
        bool received);

    std::vector<std::pair<ValidatorIndex, bool>> direct_statement_senders(
        GroupIndex group_index,
        size_t originator_index_in_group,
        const network::vstaging::StatementKind &statement_kind);
    std::vector<ValidatorIndex> direct_statement_recipients(
        GroupIndex group_index,
        size_t originator_index_in_group,
        const network::vstaging::StatementKind &statement_kind);
    std::optional<network::vstaging::StatementFilter> pending_statements(
        size_t validator);
  };

  struct GridTracker {
    enum class Error {
      DISALLOWED = 1,
      MALFORMED,
      INSUFFICIENT,
      CONFLICTING,
      OVERFLOW
    };

    outcome::result<bool> import_manifest(
        const SessionTopologyView &session_topology,
        const Groups &groups,
        const CandidateHash &candidate_hash,
        size_t seconding_limit,
        const ManifestSummary &manifest,
        ManifestKind kind,
        ValidatorIndex sender);

    std::vector<std::pair<ValidatorIndex, ManifestKind>> add_backed_candidate(
        const SessionTopologyView &session_topology,
        const CandidateHash &candidate_hash,
        GroupIndex group_index,
        const network::vstaging::StatementFilter &local_knowledge);

    void manifest_sent_to(
        const Groups &groups,
        ValidatorIndex validator_index,
        const CandidateHash &candidate_hash,
        const network::vstaging::StatementFilter &local_knowledge);

    std::vector<std::pair<CandidateHash, ManifestKind>> pending_manifests_for(
        ValidatorIndex validator_index);

    std::optional<network::vstaging::StatementFilter> pending_statements_for(
        ValidatorIndex validator_index, const CandidateHash &candidate_hash);

    std::vector<std::pair<ValidatorIndex, network::vstaging::CompactStatement>>
    all_pending_statements_for(ValidatorIndex validator_index);

    bool can_request(ValidatorIndex validator,
                     const CandidateHash &candidate_hash);

    std::vector<std::pair<ValidatorIndex, bool>> direct_statement_providers(
        const Groups &groups,
        ValidatorIndex originator,
        const network::vstaging::CompactStatement &statement);

    std::vector<ValidatorIndex> direct_statement_targets(
        const Groups &groups,
        ValidatorIndex originator,
        const network::vstaging::CompactStatement &statement);

    void learned_fresh_statement(
        const Groups &groups,
        const SessionTopologyView &session_topology,
        ValidatorIndex originator,
        const network::vstaging::CompactStatement &statement);

    void sent_or_received_direct_statement(
        const Groups &groups,
        ValidatorIndex originator,
        ValidatorIndex counterparty,
        const network::vstaging::CompactStatement &statement,
        bool received);

    std::optional<network::vstaging::StatementFilter> advertised_statements(
        ValidatorIndex validator, const CandidateHash &candidate_hash);

   private:
    std::unordered_map<ValidatorIndex, ReceivedManifests> received;
    std::unordered_map<CandidateHash, KnownBackedCandidate> confirmed_backed;
    std::unordered_map<CandidateHash,
                       std::vector<std::pair<ValidatorIndex, GroupIndex>>>
        unconfirmed;
    std::unordered_map<ValidatorIndex,
                       std::unordered_map<CandidateHash, ManifestKind>>
        pending_manifests;
    std::unordered_map<
        ValidatorIndex,
        std::unordered_set<
            std::pair<ValidatorIndex, network::vstaging::CompactStatement>>>
        pending_statements;
  };

}  // namespace kagome::parachain::grid

OUTCOME_HPP_DECLARE_ERROR(kagome::parachain::grid, GridTracker::Error);
