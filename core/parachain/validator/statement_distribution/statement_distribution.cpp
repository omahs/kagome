/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include <parachain/validator/statement_distribution/statement_distribution.hpp>
#include "parachain/validator/parachain_processor.hpp"

#define COMPONENT_NAME "StatementDistribution"

OUTCOME_CPP_DEFINE_CATEGORY(kagome::parachain::statement_distribution,
                            StatementDistribution::Error,
                            e) {
  using E =
      kagome::parachain::statement_distribution::StatementDistribution::Error;
  switch (e) {
    case E::RESPONSE_ALREADY_RECEIVED:
      return COMPONENT_NAME ": Response already present";
    case E::REJECTED_BY_PROSPECTIVE_PARACHAINS:
      return COMPONENT_NAME ": Rejected by prospective parachains";
    case E::COLLATION_NOT_FOUND:
      return COMPONENT_NAME ": Collation not found";
    case E::UNDECLARED_COLLATOR:
      return COMPONENT_NAME ": Undeclared collator";
    case E::KEY_NOT_PRESENT:
      return COMPONENT_NAME ": Private key is not present";
    case E::VALIDATION_FAILED:
      return COMPONENT_NAME ": Validate and make available failed";
    case E::VALIDATION_SKIPPED:
      return COMPONENT_NAME ": Validate and make available skipped";
    case E::OUT_OF_VIEW:
      return COMPONENT_NAME ": Out of view";
    case E::CORE_INDEX_UNAVAILABLE:
      return COMPONENT_NAME ": Core index unavailable";
    case E::DUPLICATE:
      return COMPONENT_NAME ": Duplicate";
    case E::NO_INSTANCE:
      return COMPONENT_NAME ": No self instance";
    case E::NOT_A_VALIDATOR:
      return COMPONENT_NAME ": Node is not a validator";
    case E::NOT_SYNCHRONIZED:
      return COMPONENT_NAME ": Node not synchronized";
    case E::PEER_LIMIT_REACHED:
      return COMPONENT_NAME ": Peer limit reached";
    case E::PROTOCOL_MISMATCH:
      return COMPONENT_NAME ": Protocol mismatch";
    case E::NOT_CONFIRMED:
      return COMPONENT_NAME ": Candidate not confirmed";
    case E::NO_STATE:
      return COMPONENT_NAME ": No parachain state";
    case E::NO_SESSION_INFO:
      return COMPONENT_NAME ": No session info";
    case E::OUT_OF_BOUND:
      return COMPONENT_NAME ": Index out of bound";
    case E::INCORRECT_BITFIELD_SIZE:
      return COMPONENT_NAME ": Incorrect bitfield size";
    case E::INCORRECT_SIGNATURE:
      return COMPONENT_NAME ": Incorrect signature";
    case E::CLUSTER_TRACKER_ERROR:
      return COMPONENT_NAME ": Cluster tracker error";
    case E::PERSISTED_VALIDATION_DATA_NOT_FOUND:
      return COMPONENT_NAME ": Persisted validation data not found";
    case E::PERSISTED_VALIDATION_DATA_MISMATCH:
      return COMPONENT_NAME ": Persisted validation data mismatch";
    case E::CANDIDATE_HASH_MISMATCH:
      return COMPONENT_NAME ": Candidate hash mismatch";
    case E::PARENT_HEAD_DATA_MISMATCH:
      return COMPONENT_NAME ": Parent head data mismatch";
    case E::NO_PEER:
      return COMPONENT_NAME ": No peer";
    case E::ALREADY_REQUESTED:
      return COMPONENT_NAME ": Already requested";
    case E::NOT_ADVERTISED:
      return COMPONENT_NAME ": Not advertised";
    case E::WRONG_PARA:
      return COMPONENT_NAME ": Wrong para id";
    case E::THRESHOLD_LIMIT_REACHED:
      return COMPONENT_NAME ": Threshold reached";
  }
  return COMPONENT_NAME ": Unknown error";
}

namespace kagome::parachain::statement_distribution {

  StatementDistribution::StatementDistribution(
      std::shared_ptr<parachain::ValidatorSignerFactory> sf,
      std::shared_ptr<application::AppStateManager> app_state_manager,
      StatementDistributionThreadPool &statements_distribution_thread_pool,
      std::shared_ptr<ProspectiveParachains> prospective_parachains,
      std::shared_ptr<runtime::ParachainHost> parachain_host,
      std::shared_ptr<blockchain::BlockTree> block_tree,
      std::shared_ptr<authority_discovery::Query> _query_audi,
      std::shared_ptr<NetworkBridge> _network_bridge)
      : implicit_view(
            prospective_parachains, parachain_host, block_tree, std::nullopt),
        per_session(RefCache<SessionIndex, PerSessionState>::create()),
        signer_factory(std::move(sf)),
        statements_distribution_thread_handler(
            poolHandlerReadyMake(this,
                                 app_state_manager,
                                 statements_distribution_thread_pool,
                                 logger)),
        peer_use_count(
            std::make_shared<decltype(peer_use_count)::element_type>()),
        query_audi(std::move(_query_audi)), network_bridge(std::move(_network_bridge)) {}

  /* handle active leaves sub
remote_view_sub_ = std::make_shared<network::PeerView::PeerViewSubscriber>(
peer_view_->getRemoteViewObservable(), false);
primitives::events::subscribe(
*remote_view_sub_,
network::PeerView::EventType::kViewUpdated,
[wptr{weak_from_this()}](const libp2p::peer::PeerId &peer_id,
                     const network::View &view) {
TRY_GET_OR_RET(self, wptr.lock());
self->handle_peer_view_update(peer_id, view);
});
  */

  /* handle_active_leaves
      /// update `statements_distribution` subsystem
     {
       auto new_relay_parents =
           our_current_state_.implicit_view->all_allowed_relay_parents();
       std::vector<std::pair<libp2p::peer::PeerId, std::vector<Hash>>>
           update_peers;
       pm_->enumeratePeerState([&](const libp2p::peer::PeerId &peer,
                                   network::PeerState &peer_state) {
         std::vector<Hash> fresh =
             peer_state.reconcile_active_leaf(relay_parent, new_relay_parents);
         if (!fresh.empty()) {
           update_peers.emplace_back(peer, fresh);
         }
         return true;
       });
       for (const auto &[peer, fresh] : update_peers) {
         for (const auto &fresh_relay_parent : fresh) {
           send_peer_messages_for_relay_parent(peer, fresh_relay_parent);
         }
       }
     }
     new_leaf_fragment_chain_updates(relay_parent);
      */

  /*handle_active_leaves
  auto per_session_state = per_session_->get_or_insert(session_index, [&] {
    grid::Views grid_view = grid::makeViews(
        session_info->validator_groups,
        grid::shuffle(session_info->discovery_keys.size(), randomness),
        *global_v_index);

    return RefCache<SessionIndex, PerSessionState>::RefObj(
        session_index,
        *session_info,
        Groups{session_info->validator_groups, minimum_backing_votes},
        std::move(grid_view),
        validator_index,
        peer_use_count_);
  });
  */

  /*
      std::unordered_set<primitives::AuthorityDiscoveryId> peers_sent;
   std::optional<network::GroupIndex> our_group;
   if (validator_index) {
     our_group =
         per_session_state->value().groups.byValidatorIndex(*validator_index);
     if (our_group) {
       /// update peers of our group
       const auto &group = session_info->validator_groups[*our_group];
       for (const auto vi : group) {
         spawn_and_update_peer(peers_sent, session_info->discovery_keys[vi]);
       }
     }
   }

   /// update peers in grid view
   const auto &grid_view = *per_session_state->value().grid_view;
   for (const auto &view : grid_view) {
     for (const auto vi : view.sending) {
       spawn_and_update_peer(peers_sent, session_info->discovery_keys[vi]);
     }
     for (const auto vi : view.receiving) {
       spawn_and_update_peer(peers_sent, session_info->discovery_keys[vi]);
     }
   }
  */

  /*
      std::unordered_map<primitives::AuthorityDiscoveryId, ValidatorIndex>
        authority_lookup;
    for (ValidatorIndex v = 0;
         v < per_session_state->value().session_info.discovery_keys.size();
         ++v) {
      authority_lookup[per_session_state->value()
                           .session_info.discovery_keys[v]] = v;
    }

   */

  /*handle_active_leaves
    std::optional<LocalValidatorState> local_validator;
    if (mode) {
      //statement_store.emplace(per_session_state->value().groups);
      auto maybe_claim_queue =
          [&]() -> std::optional<runtime::ClaimQueueSnapshot> {
        auto r = fetch_claim_queue(relay_parent);
        if (r.has_value()) {
          return r.value();
        }
        return std::nullopt;
      }();

      const auto seconding_limit = mode->max_candidate_depth + 1;
      local_validator = [&]() -> std::optional<LocalValidatorState> {
        if (!global_v_index) {
          return std::nullopt;
        }
        if (validator_index) {
          return find_active_validator_state(*validator_index,
                                             per_session_state->value().groups,
                                             cores,
                                             group_rotation_info,
                                             maybe_claim_queue,
                                             seconding_limit,
                                             mode->max_candidate_depth);
        }
        return LocalValidatorState{};
      }();
    }
  */

  bool StatementDistribution::tryStart() {
    return true;
  }

  bool StatementDistribution::can_disconnect(const libp2p::PeerId &peer) const {
    auto audi = query_audi->get(peer);
    if (not audi) {
      return true;
    }
    auto &peers = *peer_use_count;
    return SAFE_SHARED(peers) {
      return peers.contains(*audi);
    };
  }

  std::optional<std::reference_wrapper<PerRelayParentState>>
  StatementDistribution::tryGetStateByRelayParent(
      const primitives::BlockHash &relay_parent) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    const auto it = per_relay_parent.find(relay_parent);
    if (it != per_relay_parent.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  outcome::result<std::reference_wrapper<PerRelayParentState>>
  StatementDistribution::getStateByRelayParent(
      const primitives::BlockHash &relay_parent) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    if (auto per_relay_parent = tryGetStateByRelayParent(relay_parent)) {
      return *per_relay_parent;
    }
    return Error::OUT_OF_VIEW;
  }

  //outcome::result<network::vstaging::AttestedCandidateResponse>
  void StatementDistribution::OnFetchAttestedCandidateRequest(
      const network::vstaging::AttestedCandidateRequest &request,
      const libp2p::peer::PeerId &peer_id) {
    REINVOKE(*statements_distribution_thread_handler,
             OnFetchAttestedCandidateRequest,
             request,
             peer_id);

    auto confirmed = candidates.get_confirmed(request.candidate_hash);
    if (!confirmed) {
      return Error::NOT_CONFIRMED;
    }

    OUTCOME_TRY(relay_parent_state,
                getStateByRelayParent(confirmed->get().relay_parent()));
    auto &local_validator = relay_parent_state.get().local_validator;
    if (!local_validator) {
      return Error::NOT_A_VALIDATOR;
    }
    BOOST_ASSERT(relay_parent_state.get().statement_store);

    const auto &session_info =
        relay_parent_state.get().per_session_state->value().session_info;
    const auto &groups =
        relay_parent_state.get().per_session_state->value().groups;
    auto group = groups.get(confirmed->get().group_index());
    if (!group) {
      SL_ERROR(logger_,
               "Unexpected array bound for groups. (relay parent={})",
               confirmed->get().relay_parent());
      return Error::OUT_OF_BOUND;
    }

    const auto group_size = group->size();
    auto &mask = request.mask;
    if (mask.seconded_in_group.bits.size() != group_size
        || mask.validated_in_group.bits.size() != group_size) {
      return Error::INCORRECT_BITFIELD_SIZE;
    }

    auto &active = local_validator->active;
    std::optional<ValidatorIndex> validator_id;
    bool is_cluster = false;
    [&] {
      auto audi = query_audi->get(peer_id);
      if (not audi.has_value()) {
        SL_TRACE(logger_, "No audi. (peer={})", peer_id);
        return;
      }

      ValidatorIndex v = 0;
      for (; v < session_info.discovery_keys.size(); ++v) {
        if (session_info.discovery_keys[v] == audi.value()) {
          SL_TRACE(logger_,
                   "Captured validator. (relay_parent={}, candidate_hash={})",
                   confirmed->get().relay_parent(),
                   request.candidate_hash);
          break;
        }
      }

      if (v >= session_info.discovery_keys.size()) {
        return;
      }

      if (active
          and active->cluster_tracker.can_request(v, request.candidate_hash)) {
        validator_id = v;
        is_cluster = true;

      } else if (local_validator->grid_tracker.can_request(
                     v, request.candidate_hash)) {
        validator_id = v;
      }
    }();

    if (!validator_id) {
      return Error::OUT_OF_BOUND;
    }

    auto init_with_not = [](scale::BitVec &dst, const scale::BitVec &src) {
      dst.bits.reserve(src.bits.size());
      for (const auto i : src.bits) {
        dst.bits.emplace_back(!i);
      }
    };

    network::vstaging::StatementFilter and_mask;
    init_with_not(and_mask.seconded_in_group, request.mask.seconded_in_group);
    init_with_not(and_mask.validated_in_group, request.mask.validated_in_group);

    std::vector<IndexedAndSigned<network::vstaging::CompactStatement>>
        statements;
    network::vstaging::StatementFilter sent_filter(group_size);
    relay_parent_state.get().statement_store->groupStatements(
        *group,
        request.candidate_hash,
        and_mask,
        [&](const IndexedAndSigned<network::vstaging::CompactStatement>
                &statement) {
          for (size_t ix = 0; ix < group->size(); ++ix) {
            if ((*group)[ix] == statement.payload.ix) {
              visit_in_place(
                  getPayload(statement).inner_value,
                  [&](const network::vstaging::SecondedCandidateHash &) {
                    sent_filter.seconded_in_group.bits[ix] = true;
                  },
                  [&](const network::vstaging::ValidCandidateHash &) {
                    sent_filter.validated_in_group.bits[ix] = true;
                  },
                  [&](const auto &) {});
            }
          }
          statements.emplace_back(statement);
        });

    if (!is_cluster) {
      auto threshold = std::get<1>(*groups.get_size_and_backing_threshold(
          confirmed->get().group_index()));
      const auto seconded_and_sufficient =
          (sent_filter.has_seconded()
           && sent_filter.backing_validators() >= threshold);
      if (!seconded_and_sufficient) {
        SL_INFO(logger_,
                "Dropping a request from a grid peer because the backing "
                "threshold is no longer met. (relay_parent={}, "
                "candidate_hash={}, group_index={})",
                confirmed->get().relay_parent(),
                request.candidate_hash,
                confirmed->get().group_index());
        return Error::THRESHOLD_LIMIT_REACHED;
      }
    }

    for (const auto &statement : statements) {
      if (is_cluster) {
        active->cluster_tracker.note_sent(
            *validator_id,
            statement.payload.ix,
            network::vstaging::from(getPayload(statement)));
      } else {
        local_validator->grid_tracker.sent_or_received_direct_statement(
            groups,
            statement.payload.ix,
            *validator_id,
            getPayload(statement),
            false);
      }
    }

    network_bridge->send_response(stream, protocol, std::make_shared<network::vstaging::AttestedCandidateResponse>(network::vstaging::AttestedCandidateResponse{
        .candidate_receipt = confirmed->get().receipt,
        .persisted_validation_data = confirmed->get().persisted_validation_data,
        .statements = std::move(statements),
    }));
  }

  void StatementDistribution::handle_peer_view_update(
      const libp2p::peer::PeerId &peer, const network::View &new_view) {
    REINVOKE(*main_pool_handler_, handle_peer_view_update, peer, new_view);
    TRY_GET_OR_RET(peer_state, pm_->getPeerState(peer));

    auto fresh_implicit = peer_state->get().update_view(
        new_view, implicit_view);
    for (const auto &new_relay_parent : fresh_implicit) {
      send_peer_messages_for_relay_parent(peer, new_relay_parent);
    }
  }

  outcome::result<void> StatementDistribution::handle_grid_statement(
      const RelayHash &relay_parent,
      PerRelayParentState &per_relay_parent,
      grid::GridTracker &grid_tracker,
      const IndexedAndSigned<network::vstaging::CompactStatement> &statement,
      ValidatorIndex grid_sender_index) {
    /// TODO(iceseer): do Ensure the statement is correctly signed. Signature
    /// check.
    grid_tracker.sent_or_received_direct_statement(
        per_relay_parent.per_session_state->value().groups,
        statement.payload.ix,
        grid_sender_index,
        getPayload(statement),
        true);
    return outcome::success();
  }

  network::vstaging::StatementFilter
  StatementDistribution::local_knowledge_filter(
      size_t group_size,
      GroupIndex group_index,
      const CandidateHash &candidate_hash,
      const StatementStore &statement_store) {
    network::vstaging::StatementFilter f{group_size};
    statement_store.fill_statement_filter(group_index, candidate_hash, f);
    return f;
  }

  void StatementDistribution::request_attested_candidate(
      const libp2p::peer::PeerId &peer,
      PerRelayParentState &relay_parent_state,
      const RelayHash &relay_parent,
      const CandidateHash &candidate_hash,
      GroupIndex group_index) {
    CHECK_OR_RET(relay_parent_state.local_validator);
    auto &local_validator = *relay_parent_state.local_validator;

    const auto &session_info =
        relay_parent_state.per_session_state->value().session_info;

    TRY_GET_OR_RET(
        group,
        relay_parent_state.per_session_state->value().groups.get(group_index));
    const auto seconding_limit =
        relay_parent_state.prospective_parachains_mode->max_candidate_depth + 1;

    SL_TRACE(logger_,
             "Form unwanted mask. (relay_parent={}, candidate_hash={})",
             relay_parent,
             candidate_hash);
    network::vstaging::StatementFilter unwanted_mask(group->size());
    for (size_t i = 0; i < group->size(); ++i) {
      const auto v = (*group)[i];
      if (relay_parent_state.statement_store->seconded_count(v)
          >= seconding_limit) {
        unwanted_mask.seconded_in_group.bits[i] = true;
      }
    }

    auto disabled_mask = relay_parent_state.disabled_bitmask(*group);
    if (disabled_mask.bits.size()
        > unwanted_mask.seconded_in_group.bits.size()) {
      unwanted_mask.seconded_in_group.bits.resize(disabled_mask.bits.size());
    }
    if (disabled_mask.bits.size()
        > unwanted_mask.validated_in_group.bits.size()) {
      unwanted_mask.validated_in_group.bits.resize(disabled_mask.bits.size());
    }
    for (size_t i = 0; i < disabled_mask.bits.size(); ++i) {
      unwanted_mask.seconded_in_group.bits[i] =
          unwanted_mask.seconded_in_group.bits[i] || disabled_mask.bits[i];
      unwanted_mask.validated_in_group.bits[i] =
          unwanted_mask.validated_in_group.bits[i] || disabled_mask.bits[i];
    }

    auto backing_threshold = [&]() -> std::optional<size_t> {
      auto bt = relay_parent_state.per_session_state->value()
                    .groups.get_size_and_backing_threshold(group_index);
      return bt ? std::get<1>(*bt) : std::optional<size_t>{};
    }();

    SL_TRACE(logger_,
             "Enumerate peers. (relay_parent={}, candidate_hash={})",
             relay_parent,
             candidate_hash);
    std::optional<libp2p::peer::PeerId> target;
    auto audi = query_audi->get(peer);
    if (!audi) {
      SL_TRACE(logger_,
               "No audi. (relay_parent={}, candidate_hash={})",
               relay_parent,
               candidate_hash);
      return;
    }

    ValidatorIndex validator_id = 0;
    for (; validator_id < session_info.discovery_keys.size(); ++validator_id) {
      if (session_info.discovery_keys[validator_id] == *audi) {
        SL_TRACE(logger_,
                 "Captured validator. (relay_parent={}, candidate_hash={})",
                 relay_parent,
                 candidate_hash);
        break;
      }
    }

    CHECK_OR_RET(validator_id < session_info.discovery_keys.size());
    auto filter = [&]() -> std::optional<network::vstaging::StatementFilter> {
      if (local_validator.active) {
        if (local_validator.active->cluster_tracker.knows_candidate(
                validator_id, candidate_hash)) {
          return network::vstaging::StatementFilter(
              local_validator.active->cluster_tracker.targets().size());
        }
      }

      auto filter = local_validator.grid_tracker.advertised_statements(
          validator_id, candidate_hash);
      if (filter) {
        return filter;
      }

      SL_TRACE(logger_,
               "No filter. (relay_parent={}, candidate_hash={})",
               relay_parent,
               candidate_hash);
      return std::nullopt;
    }();

    CHECK_OR_RET(filter);
    filter->mask_seconded(unwanted_mask.seconded_in_group);
    filter->mask_valid(unwanted_mask.validated_in_group);

    if (!backing_threshold
        || (filter->has_seconded()
            && filter->backing_validators() >= *backing_threshold)) {
      target.emplace(peer);
    } else {
      SL_TRACE(
          logger_,
          "Not pass backing threshold. (relay_parent={}, candidate_hash={})",
          relay_parent,
          candidate_hash);
      return;
    }

    if (!target) {
      SL_TRACE(logger_,
               "Target not found. (relay_parent={}, candidate_hash={})",
               relay_parent,
               candidate_hash);
      return;
    }

    SL_TRACE(logger_,
             "Requesting. (peer={}, relay_parent={}, candidate_hash={})",
             peer,
             relay_parent,
             candidate_hash);
    router_->getFetchAttestedCandidateProtocol()->doRequest(
        peer,
        network::vstaging::AttestedCandidateRequest{
            .candidate_hash = candidate_hash,
            .mask = unwanted_mask,
        },
        [wptr{weak_from_this()},
         relay_parent{relay_parent},
         candidate_hash{candidate_hash},
         group_index{group_index}](
            outcome::result<network::vstaging::AttestedCandidateResponse>
                r) mutable {
          TRY_GET_OR_RET(self, wptr.lock());
          self->handle_response(
              std::move(r), relay_parent, candidate_hash, group_index);
        });
  }

  void StatementDistribution::handle_response(
      outcome::result<network::vstaging::AttestedCandidateResponse> &&r,
      const RelayHash &relay_parent,
      const CandidateHash &candidate_hash,
      GroupIndex group_index) {
    REINVOKE(*main_pool_handler_,
             handle_response,
             std::move(r),
             relay_parent,
             candidate_hash,
             group_index);

    if (r.has_error()) {
      SL_INFO(logger_,
              "Fetch attested candidate returned an error. (relay parent={}, "
              "candidate={}, group index={}, error={})",
              relay_parent,
              candidate_hash,
              group_index,
              r.error());
      return;
    }

    TRY_GET_OR_RET(parachain_state, tryGetStateByRelayParent(relay_parent));
    CHECK_OR_RET(parachain_state->get().statement_store);

    const network::vstaging::AttestedCandidateResponse &response = r.value();
    SL_INFO(logger_,
            "Fetch attested candidate success. (relay parent={}, "
            "candidate={}, group index={}, statements={})",
            relay_parent,
            candidate_hash,
            group_index,
            response.statements.size());
    for (const auto &statement : response.statements) {
      parachain_state->get().statement_store->insert(
          parachain_state->get().per_session_state->value().groups,
          statement,
          StatementOrigin::Remote);
    }

    auto opt_post_confirmation =
        candidates.confirm_candidate(candidate_hash,
                                     response.candidate_receipt,
                                     response.persisted_validation_data,
                                     group_index,
                                     hasher_);
    if (!opt_post_confirmation) {
      SL_WARN(logger_,
              "Candidate re-confirmed by request/response: logic error. (relay "
              "parent={}, candidate={})",
              relay_parent,
              candidate_hash);
      return;
    }

    auto &post_confirmation = *opt_post_confirmation;
    apply_post_confirmation(post_confirmation);

    auto opt_confirmed = candidates.get_confirmed(candidate_hash);
    BOOST_ASSERT(opt_confirmed);

    if (!opt_confirmed->get().is_importable(std::nullopt)) {
      SL_INFO(logger_,
              "Not importable. (relay parent={}, "
              "candidate={}, group index={})",
              relay_parent,
              candidate_hash,
              group_index);
      return;
    }

    const auto &groups =
        parachain_state->get().per_session_state->value().groups;
    auto it = groups.groups.find(group_index);
    if (it == groups.groups.end()) {
      SL_WARN(logger_,
              "Group was not found. (relay parent={}, candidate={}, group "
              "index={})",
              relay_parent,
              candidate_hash,
              group_index);
      return;
    }

    SL_INFO(logger_,
            "Send fresh statements. (relay parent={}, "
            "candidate={})",
            relay_parent,
            candidate_hash);
    send_backing_fresh_statements(opt_confirmed->get(),
                                  relay_parent,
                                  parachain_state->get(),
                                  it->second,
                                  candidate_hash);
  }

  void StatementDistribution::apply_post_confirmation(
      const PostConfirmation &post_confirmation) {
    const auto candidate_hash = candidateHash(post_confirmation.hypothetical);
    send_cluster_candidate_statements(
        candidate_hash, relayParent(post_confirmation.hypothetical));

    new_confirmed_candidate_fragment_chain_updates(
        post_confirmation.hypothetical);
  }

  void StatementDistribution::send_cluster_candidate_statements(
      const CandidateHash &candidate_hash, const RelayHash &relay_parent) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    TRY_GET_OR_RET(relay_parent_state, tryGetStateByRelayParent(relay_parent));
    TRY_GET_OR_RET(local_group, relay_parent_state->get().our_group);
    TRY_GET_OR_RET(
        group,
        relay_parent_state->get().per_session_state->value().groups.get(
            *local_group));

    auto group_size = group->size();
    relay_parent_state->get().statement_store->groupStatements(
        *group,
        candidate_hash,
        network::vstaging::StatementFilter(group_size, true),
        [&](const IndexedAndSigned<network::vstaging::CompactStatement>
                &statement) {
          circulate_statement(
              relay_parent, relay_parent_state->get(), statement);
        });
  }

  void StatementDistribution::handle_backed_candidate_message(
      const CandidateHash &candidate_hash) {
    auto confirmed_opt = candidates.get_confirmed(candidate_hash);
    if (!confirmed_opt) {
      SL_TRACE(logger_,
               "Received backed candidate notification for unknown or "
               "unconfirmed. (candidate_hash={})",
               candidate_hash);
      return;
    }
    const auto &confirmed = confirmed_opt->get();

    const auto relay_parent = confirmed.relay_parent();
    TRY_GET_OR_RET(relay_parent_state_opt,
                   tryGetStateByRelayParent(relay_parent));
    BOOST_ASSERT(relay_parent_state_opt->get().statement_store);

    const auto &session_info =
        relay_parent_state_opt->get().per_session_state->value().session_info;
    provide_candidate_to_grid(
        candidate_hash, relay_parent_state_opt->get(), confirmed, session_info);

    prospective_backed_notification_fragment_chain_updates(
        confirmed.para_id(), confirmed.para_head());
  }

  void StatementDistribution::handle_incoming_acknowledgement(
      const libp2p::peer::PeerId &peer_id,
      const network::vstaging::BackedCandidateAcknowledgement
          &acknowledgement) {
    SL_TRACE(logger_,
             "`BackedCandidateAcknowledgement`. (candidate_hash={})",
             acknowledgement.candidate_hash);
    const auto &candidate_hash = acknowledgement.candidate_hash;
    SL_TRACE(logger_,
             "Received incoming acknowledgement. (peer={}, candidate hash={})",
             peer_id,
             candidate_hash);

    TRY_GET_OR_RET(c, candidates.get_confirmed(candidate_hash));
    const RelayHash &relay_parent = c->get().relay_parent();
    const Hash &parent_head_data_hash = c->get().parent_head_data_hash();
    GroupIndex group_index = c->get().group_index();
    ParachainId para_id = c->get().para_id();

    TRY_GET_OR_RET(opt_parachain_state, tryGetStateByRelayParent(relay_parent));
    auto &relay_parent_state = opt_parachain_state->get();
    BOOST_ASSERT(relay_parent_state.statement_store);

    SL_TRACE(logger_,
             "Handling incoming acknowledgement. (relay_parent={})",
             relay_parent);
    ManifestImportSuccessOpt x = handle_incoming_manifest_common(
        peer_id,
        candidate_hash,
        relay_parent,
        ManifestSummary{
            .claimed_parent_hash = parent_head_data_hash,
            .claimed_group_index = group_index,
            .statement_knowledge = acknowledgement.statement_knowledge,
        },
        para_id,
        grid::ManifestKind::Acknowledgement);
    CHECK_OR_RET(x);

    SL_TRACE(
        logger_, "Check local validator. (relay_parent = {})", relay_parent);
    CHECK_OR_RET(relay_parent_state.local_validator);

    const auto sender_index = x->sender_index;
    auto &local_validator = *relay_parent_state.local_validator;

    SL_TRACE(logger_, "Post ack. (relay_parent = {})", relay_parent);
    auto messages = post_acknowledgement_statement_messages(
        sender_index,
        relay_parent,
        local_validator.grid_tracker,
        *relay_parent_state.statement_store,
        relay_parent_state.per_session_state->value().groups,
        group_index,
        candidate_hash,
        peer_id,
        network::CollationVersion::VStaging);

    auto se = pm_->getStreamEngine();
    SL_TRACE(logger_, "Sending messages. (relay_parent = {})", relay_parent);
    for (auto &msg : messages) {
      if (auto m = if_type<network::vstaging::ValidatorProtocolMessage>(msg)) {
        auto message = std::make_shared<
            network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
            std::move(m->get()));
        se->send(peer_id, router_->getValidationProtocolVStaging(), message);
      } else {
        assert(false);
      }
    }
  }

  std::deque<std::pair<std::vector<libp2p::peer::PeerId>,
                       network::VersionedValidatorProtocolMessage>>
  StatementDistribution::acknowledgement_and_statement_messages(
      const libp2p::peer::PeerId &peer,
      network::CollationVersion version,
      ValidatorIndex validator_index,
      const Groups &groups,
      PerRelayParentState &relay_parent_state,
      const RelayHash &relay_parent,
      GroupIndex group_index,
      const CandidateHash &candidate_hash,
      const network::vstaging::StatementFilter &local_knowledge) {
    if (!relay_parent_state.local_validator) {
      return {};
    }

    auto &local_validator = *relay_parent_state.local_validator;
    std::deque<std::pair<std::vector<libp2p::peer::PeerId>,
                         network::VersionedValidatorProtocolMessage>>
        messages;

    switch (version) {
      case network::CollationVersion::VStaging: {
        messages.emplace_back(
            std::vector<libp2p::peer::PeerId>{peer},
            network::VersionedValidatorProtocolMessage{
                network::vstaging::ValidatorProtocolMessage{
                    network::vstaging::StatementDistributionMessage{
                        network::vstaging::BackedCandidateAcknowledgement{
                            .candidate_hash = candidate_hash,
                            .statement_knowledge = local_knowledge,
                        }}}});
      } break;
      default: {
        SL_ERROR(logger_,
                 "Bug ValidationVersion::V1 should not be used in "
                 "statement-distribution v2, legacy should have handled this");
        return {};
      } break;
    };

    local_validator.grid_tracker.manifest_sent_to(
        groups, validator_index, candidate_hash, local_knowledge);

    auto statement_messages = post_acknowledgement_statement_messages(
        validator_index,
        relay_parent,
        local_validator.grid_tracker,
        *relay_parent_state.statement_store,
        groups,
        group_index,
        candidate_hash,
        peer,
        version);

    for (auto &&m : statement_messages) {
      messages.emplace_back(std::vector<libp2p::peer::PeerId>{peer},
                            std::move(m));
    }
    return messages;
  }

  void StatementDistribution::handle_incoming_manifest(
      const libp2p::peer::PeerId &peer_id,
      const network::vstaging::BackedCandidateManifest &manifest) {
    SL_TRACE(logger_,
             "`BackedCandidateManifest`. (relay_parent={}, "
             "candidate_hash={}, para_id={}, parent_head_data_hash={})",
             manifest.relay_parent,
             manifest.candidate_hash,
             manifest.para_id,
             manifest.parent_head_data_hash);

    TRY_GET_OR_RET(relay_parent_state,
                   tryGetStateByRelayParent(manifest.relay_parent));
    CHECK_OR_RET(relay_parent_state->get().statement_store);

    SL_TRACE(logger_,
             "Handling incoming manifest common. (relay_parent={}, "
             "candidate_hash={})",
             manifest.relay_parent,
             manifest.candidate_hash);
    ManifestImportSuccessOpt x = handle_incoming_manifest_common(
        peer_id,
        manifest.candidate_hash,
        manifest.relay_parent,
        ManifestSummary{
            .claimed_parent_hash = manifest.parent_head_data_hash,
            .claimed_group_index = manifest.group_index,
            .statement_knowledge = manifest.statement_knowledge,
        },
        manifest.para_id,
        grid::ManifestKind::Full);
    CHECK_OR_RET(x);

    const auto sender_index = x->sender_index;
    if (x->acknowledge) {
      SL_TRACE(logger_,
               "Known candidate - acknowledging manifest. (candidate hash={})",
               manifest.candidate_hash);

      SL_TRACE(logger_,
               "Get groups. (relay_parent={}, candidate_hash={})",
               manifest.relay_parent,
               manifest.candidate_hash);
      auto group =
          relay_parent_state->get().per_session_state->value().groups.get(
              manifest.group_index);
      if (!group) {
        return;
      }

      network::vstaging::StatementFilter local_knowledge =
          local_knowledge_filter(group->size(),
                                 manifest.group_index,
                                 manifest.candidate_hash,
                                 *relay_parent_state->get().statement_store);
      SL_TRACE(logger_,
               "Get ack and statement messages. (relay_parent={}, "
               "candidate_hash={})",
               manifest.relay_parent,
               manifest.candidate_hash);
      auto messages = acknowledgement_and_statement_messages(
          peer_id,
          network::CollationVersion::VStaging,
          sender_index,
          relay_parent_state->get().per_session_state->value().groups,
          relay_parent_state->get(),
          manifest.relay_parent,
          manifest.group_index,
          manifest.candidate_hash,
          local_knowledge);

      SL_TRACE(logger_,
               "Send messages. (relay_parent={}, candidate_hash={})",
               manifest.relay_parent,
               manifest.candidate_hash);
      auto se = pm_->getStreamEngine();
      for (auto &[peers, msg] : messages) {
        if (auto m =
                if_type<network::vstaging::ValidatorProtocolMessage>(msg)) {
          auto message = std::make_shared<network::WireMessage<
              network::vstaging::ValidatorProtocolMessage>>(
              std::move(m->get()));
          for (const auto &p : peers) {
            se->send(p, router_->getValidationProtocolVStaging(), message);
          }
        } else {
          assert(false);
        }
      }
    } else if (!candidates.is_confirmed(manifest.candidate_hash)) {
      SL_TRACE(
          logger_,
          "Request attested candidate. (relay_parent={}, candidate_hash={})",
          manifest.relay_parent,
          manifest.candidate_hash);
      request_attested_candidate(peer_id,
                                 relay_parent_state->get(),
                                 manifest.relay_parent,
                                 manifest.candidate_hash,
                                 manifest.group_index);
    }
  }

  std::deque<network::VersionedValidatorProtocolMessage>
  StatementDistribution::post_acknowledgement_statement_messages(
      ValidatorIndex recipient,
      const RelayHash &relay_parent,
      grid::GridTracker &grid_tracker,
      const StatementStore &statement_store,
      const Groups &groups,
      GroupIndex group_index,
      const CandidateHash &candidate_hash,
      const libp2p::peer::PeerId &peer,
      network::CollationVersion version) {
    auto sending_filter =
        grid_tracker.pending_statements_for(recipient, candidate_hash);
    if (!sending_filter) {
      return {};
    }

    std::deque<network::VersionedValidatorProtocolMessage> messages;
    auto group = groups.get(group_index);
    if (!group) {
      return messages;
    }

    statement_store.groupStatements(
        *group,
        candidate_hash,
        *sending_filter,
        [&](const IndexedAndSigned<network::vstaging::CompactStatement>
                &statement) {
          grid_tracker.sent_or_received_direct_statement(groups,
                                                         statement.payload.ix,
                                                         recipient,
                                                         getPayload(statement),
                                                         false);

          switch (version) {
            case network::CollationVersion::VStaging: {
              messages.emplace_back(network::vstaging::ValidatorProtocolMessage{
                  network::vstaging::StatementDistributionMessage{
                      network::vstaging::StatementDistributionMessageStatement{
                          .relay_parent = relay_parent,
                          .compact = statement,
                      }}});
            } break;
            default: {
              SL_ERROR(
                  logger_,
                  "Bug ValidationVersion::V1 should not be used in "
                  "statement-distribution v2, legacy should have handled this");
            } break;
          }
        });
    return messages;
  }

  StatementDistribution::ManifestImportSuccessOpt
  StatementDistribution::handle_incoming_manifest_common(
      const libp2p::peer::PeerId &peer_id,
      const CandidateHash &candidate_hash,
      const RelayHash &relay_parent,
      ManifestSummary manifest_summary,
      ParachainId para_id,
      grid::ManifestKind manifest_kind) {
    auto peer_state = pm_->getPeerState(peer_id);
    if (!peer_state) {
      SL_WARN(logger_, "No peer state. (peer_id={})", peer_id);
      return {};
    }

    auto relay_parent_state = tryGetStateByRelayParent(relay_parent);
    if (!relay_parent_state) {
      return {};
    }

    if (!relay_parent_state->get().local_validator) {
      return {};
    }

    auto expected_group =
        group_for_para(relay_parent_state->get().availability_cores,
                       relay_parent_state->get().group_rotation_info,
                       para_id);

    if (!expected_group
        || *expected_group != manifest_summary.claimed_group_index) {
      return {};
    }

    if (!relay_parent_state->get().per_session_state->value().grid_view) {
      return {};
    }

    const auto &grid_topology =
        *relay_parent_state->get().per_session_state->value().grid_view;
    if (manifest_summary.claimed_group_index >= grid_topology.size()) {
      return {};
    }

    auto sender_index = [&]() -> std::optional<ValidatorIndex> {
      const auto &sub = grid_topology[manifest_summary.claimed_group_index];
      const auto &iter = (manifest_kind == grid::ManifestKind::Full)
                           ? sub.receiving
                           : sub.sending;
      if (!iter.empty()) {
        return *iter.begin();
      }
      return {};
    }();

    if (!sender_index) {
      return {};
    }

    auto group_index = manifest_summary.claimed_group_index;
    auto claimed_parent_hash = manifest_summary.claimed_parent_hash;

    auto group = [&]() -> std::span<const ValidatorIndex> {
      if (auto g =
              relay_parent_state->get().per_session_state->value().groups.get(
                  group_index)) {
        return *g;
      }
      return {};
    }();

    auto disabled_mask = relay_parent_state->get().disabled_bitmask(group);
    manifest_summary.statement_knowledge.mask_seconded(disabled_mask);
    manifest_summary.statement_knowledge.mask_valid(disabled_mask);

    BOOST_ASSERT(relay_parent_state->get().prospective_parachains_mode);
    const auto seconding_limit =
        relay_parent_state->get()
            .prospective_parachains_mode->max_candidate_depth
        + 1;

    auto &local_validator = *relay_parent_state->get().local_validator;

    SL_TRACE(
        logger_,
        "Import manifest. (peer_id={}, relay_parent={}, candidate_hash={})",
        peer_id,
        relay_parent,
        candidate_hash);
    auto acknowledge_res = local_validator.grid_tracker.import_manifest(
        grid_topology,
        relay_parent_state->get().per_session_state->value().groups,
        candidate_hash,
        seconding_limit,
        manifest_summary,
        manifest_kind,
        *sender_index);

    if (acknowledge_res.has_error()) {
      SL_WARN(logger_,
              "Import manifest failed. (peer_id={}, relay_parent={}, "
              "candidate_hash={}, error={})",
              peer_id,
              relay_parent,
              candidate_hash,
              acknowledge_res.error());
      return {};
    }

    const auto acknowledge = acknowledge_res.value();
    if (!candidates.insert_unconfirmed(peer_id,
                                       candidate_hash,
                                       relay_parent,
                                       group_index,
                                       {{claimed_parent_hash, para_id}})) {
      SL_TRACE(logger_,
               "Insert unconfirmed candidate failed. (candidate hash={}, relay "
               "parent={}, para id={}, claimed parent={})",
               candidate_hash,
               relay_parent,
               para_id,
               manifest_summary.claimed_parent_hash);
      return {};
    }

    if (acknowledge) {
      SL_TRACE(logger_,
               "immediate ack, known candidate. (candidate hash={}, from={}, "
               "local_validator={})",
               candidate_hash,
               *sender_index,
               *relay_parent_state->get().our_index);
    }

    return ManifestImportSuccess{
        .acknowledge = acknowledge,
        .sender_index = *sender_index,
    };
  }

  void StatementDistribution::new_confirmed_candidate_fragment_chain_updates(
      const HypotheticalCandidate &candidate) {
    fragment_chain_update_inner(std::nullopt, std::nullopt, {candidate});
  }

  void StatementDistribution::new_leaf_fragment_chain_updates(
      const Hash &leaf_hash) {
    fragment_chain_update_inner({leaf_hash}, std::nullopt, std::nullopt);
  }

  void
  StatementDistribution::prospective_backed_notification_fragment_chain_updates(
      ParachainId para_id, const Hash &para_head) {
    std::pair<std::reference_wrapper<const Hash>, ParachainId> p{{para_head},
                                                                 para_id};
    fragment_chain_update_inner(std::nullopt, p, std::nullopt);
  }

  void StatementDistribution::fragment_chain_update_inner(
      std::optional<std::reference_wrapper<const Hash>> active_leaf_hash,
      std::optional<std::pair<std::reference_wrapper<const Hash>, ParachainId>>
          required_parent_info,
      std::optional<std::reference_wrapper<const HypotheticalCandidate>>
          known_hypotheticals) {
    std::vector<HypotheticalCandidate> hypotheticals;
    if (!known_hypotheticals) {
      hypotheticals = candidates.frontier_hypotheticals(required_parent_info);
    } else {
      hypotheticals.emplace_back(known_hypotheticals->get());
    }

    auto frontier =
        prospective_parachains_->answer_hypothetical_membership_request(
            hypotheticals, active_leaf_hash);
    for (const auto &[hypo, membership] : frontier) {
      if (membership.empty()) {
        continue;
      }

      for (const auto &leaf_hash : membership) {
        candidates.note_importable_under(hypo, leaf_hash);
      }

      if (auto c = if_type<const HypotheticalCandidateComplete>(hypo)) {
        auto confirmed_candidate =
            candidates.get_confirmed(c->get().candidate_hash);
        auto prs =
            tryGetStateByRelayParent(c->get().receipt.descriptor.relay_parent);

        if (prs && confirmed_candidate) {
          const auto group_index =
              group_for_para(prs->get().availability_cores,
                             prs->get().group_rotation_info,
                             c->get().receipt.descriptor.para_id);

          const auto &session_info =
              prs->get().per_session_state->value().session_info;
          if (!group_index
              || *group_index >= session_info.validator_groups.size()) {
            return;
          }

          const auto &group = session_info.validator_groups[*group_index];
          send_backing_fresh_statements(
              *confirmed_candidate,
              c->get().receipt.descriptor.relay_parent,
              prs->get(),
              group,
              c->get().candidate_hash);
        }
      }
    }
  }

  void StatementDistribution::provide_candidate_to_grid(
      const CandidateHash &candidate_hash,
      PerRelayParentState &relay_parent_state,
      const ConfirmedCandidate &confirmed_candidate,
      const runtime::SessionInfo &session_info) {
    CHECK_OR_RET(relay_parent_state.local_validator);
    auto &local_validator = *relay_parent_state.local_validator;

    const auto relay_parent = confirmed_candidate.relay_parent();
    const auto group_index = confirmed_candidate.group_index();

    if (!relay_parent_state.per_session_state->value().grid_view) {
      SL_TRACE(logger_,
               "Cannot handle backable candidate due to lack of topology. "
               "(candidate={}, relay_parent={})",
               candidate_hash,
               relay_parent);
      return;
    }

    const auto &grid_view =
        *relay_parent_state.per_session_state->value().grid_view;
    const auto group =
        relay_parent_state.per_session_state->value().groups.get(group_index);
    if (!group) {
      SL_TRACE(logger_,
               "Handled backed candidate with unknown group? (candidate={}, "
               "relay_parent={}, group_index={})",
               candidate_hash,
               relay_parent,
               group_index);
      return;
    }

    const auto group_size = group->size();
    auto filter = local_knowledge_filter(group_size,
                                         group_index,
                                         candidate_hash,
                                         *relay_parent_state.statement_store);

    auto actions = local_validator.grid_tracker.add_backed_candidate(
        grid_view, candidate_hash, group_index, filter);

    network::vstaging::BackedCandidateManifest manifest{
        .relay_parent = relay_parent,
        .candidate_hash = candidate_hash,
        .group_index = group_index,
        .para_id = confirmed_candidate.para_id(),
        .parent_head_data_hash = confirmed_candidate.parent_head_data_hash(),
        .statement_knowledge = filter};

    network::vstaging::BackedCandidateAcknowledgement acknowledgement{
        .candidate_hash = candidate_hash, .statement_knowledge = filter};

    std::vector<std::pair<libp2p::peer::PeerId, network::CollationVersion>>
        manifest_peers;
    std::vector<std::pair<libp2p::peer::PeerId, network::CollationVersion>>
        ack_peers;
    std::deque<std::pair<std::vector<libp2p::peer::PeerId>,
                         network::VersionedValidatorProtocolMessage>>
        post_statements;

    for (const auto &[v, action] : actions) {
      auto peer_opt = query_audi->get(session_info.discovery_keys[v]);
      if (!peer_opt) {
        SL_TRACE(logger_,
                 "No peer info. (relay_parent={}, validator_index={}, "
                 "candidate_hash={})",
                 relay_parent,
                 v,
                 candidate_hash);
        continue;
      }

      auto peer_state = pm_->getPeerState(peer_opt->id);
      if (!peer_state) {
        SL_TRACE(logger_,
                 "No peer state. (relay_parent={}, peer={}, candidate_hash={})",
                 relay_parent,
                 peer_opt->id,
                 candidate_hash);
        continue;
      }

      if (!peer_state->get().knows_relay_parent(relay_parent)) {
        SL_TRACE(logger_,
                 "Peer doesn't know relay parent. (relay_parent={}, peer={}, "
                 "candidate_hash={})",
                 relay_parent,
                 peer_opt->id,
                 candidate_hash);
        continue;
      }

      switch (action) {
        case grid::ManifestKind::Full: {
          SL_TRACE(logger_, "Full manifest -> {}", v);
          manifest_peers.emplace_back(peer_opt->id,
                                      network::CollationVersion::VStaging);
        } break;
        case grid::ManifestKind::Acknowledgement: {
          SL_TRACE(logger_, "Ack manifest -> {}", v);
          ack_peers.emplace_back(peer_opt->id,
                                 network::CollationVersion::VStaging);
        } break;
      }

      local_validator.grid_tracker.manifest_sent_to(
          relay_parent_state.per_session_state->value().groups,
          v,
          candidate_hash,
          filter);

      auto msgs = post_acknowledgement_statement_messages(
          v,
          relay_parent,
          local_validator.grid_tracker,
          *relay_parent_state.statement_store,
          relay_parent_state.per_session_state->value().groups,
          group_index,
          candidate_hash,
          peer_opt->id,
          network::CollationVersion::VStaging);

      for (auto &msg : msgs) {
        post_statements.emplace_back(
            std::vector<libp2p::peer::PeerId>{peer_opt->id}, std::move(msg));
      }
    }

    auto se = pm_->getStreamEngine();
    if (!manifest_peers.empty()) {
      SL_TRACE(logger_,
               "Sending manifest to v2 peers. (candidate_hash={}, "
               "local_validator={}, n_peers={})",
               candidate_hash,
               *relay_parent_state.our_index,
               manifest_peers.size());
      auto message = std::make_shared<
          network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
          kagome::network::vstaging::ValidatorProtocolMessage{
              kagome::network::vstaging::StatementDistributionMessage{
                  manifest}});
      for (const auto &[p, _] : manifest_peers) {
        se->send(p, router_->getValidationProtocolVStaging(), message);
      }
    }

    if (!ack_peers.empty()) {
      SL_TRACE(logger_,
               "Sending acknowledgement to v2 peers. (candidate_hash={}, "
               "local_validator={}, n_peers={})",
               candidate_hash,
               *relay_parent_state.our_index,
               ack_peers.size());
      auto message = std::make_shared<
          network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
          kagome::network::vstaging::ValidatorProtocolMessage{
              kagome::network::vstaging::StatementDistributionMessage{
                  acknowledgement}});
      for (const auto &[p, _] : ack_peers) {
        se->send(p, router_->getValidationProtocolVStaging(), message);
      }
    }

    if (!post_statements.empty()) {
      SL_TRACE(logger_,
               "Sending statements to v2 peers. (candidate_hash={}, "
               "local_validator={}, n_peers={})",
               candidate_hash,
               *relay_parent_state.our_index,
               post_statements.size());

      for (auto &[peers, msg] : post_statements) {
        if (auto m =
                if_type<network::vstaging::ValidatorProtocolMessage>(msg)) {
          auto message = std::make_shared<network::WireMessage<
              network::vstaging::ValidatorProtocolMessage>>(
              std::move(m->get()));
          for (const auto &p : peers) {
            se->send(p, router_->getValidationProtocolVStaging(), message);
          }
        } else {
          assert(false);
        }
      }
    }
  }

  void StatementDistribution::send_backing_fresh_statements(
      const ConfirmedCandidate &confirmed,
      const RelayHash &relay_parent,
      PerRelayParentState &per_relay_parent,
      const std::vector<ValidatorIndex> &group,
      const CandidateHash &candidate_hash) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    CHECK_OR_RET(per_relay_parent.statement_store);
    std::vector<std::pair<ValidatorIndex, network::vstaging::CompactStatement>>
        imported;
    per_relay_parent.statement_store->fresh_statements_for_backing(
        group,
        candidate_hash,
        [&](const IndexedAndSigned<network::vstaging::CompactStatement>
                &statement) {
          const auto &v = statement.payload.ix;
          const auto &compact = getPayload(statement);
          imported.emplace_back(v, compact);

          SignedFullStatementWithPVD carrying_pvd{
              .payload =
                  {
                      .payload = visit_in_place(
                          compact.inner_value,
                          [&](const network::vstaging::SecondedCandidateHash &)
                              -> StatementWithPVD {
                            return StatementWithPVDSeconded{
                                .committed_receipt = confirmed.receipt,
                                .pvd = confirmed.persisted_validation_data,
                            };
                          },
                          [](const network::vstaging::ValidCandidateHash &val)
                              -> StatementWithPVD {
                            return StatementWithPVDValid{
                                .candidate_hash = val.hash,
                            };
                          },
                          [](const auto &) -> StatementWithPVD {
                            UNREACHABLE;
                          }),
                      .ix = statement.payload.ix,
                  },
              .signature = statement.signature,
          };

          if (auto parachain_proc = parachain_processor.lock()) {
            SL_TRACE(self->logger_, "Handle statement {}", relay_parent);
            parachain_proc->handleStatement(relay_parent, carrying_pvd);
          }
        });

    for (const auto &[v, s] : imported) {
      per_relay_parent.statement_store->note_known_by_backing(v, s);
    }
  }

  outcome::result<std::optional<network::vstaging::SignedCompactStatement>>
  StatementDistribution::handle_cluster_statement(
      const RelayHash &relay_parent,
      ClusterTracker &cluster_tracker,
      SessionIndex session,
      const runtime::SessionInfo &session_info,
      const network::vstaging::SignedCompactStatement &statement,
      ValidatorIndex cluster_sender_index) {
    const auto accept = cluster_tracker.can_receive(
        cluster_sender_index,
        statement.payload.ix,
        network::vstaging::from(getPayload(statement)));
    if (accept != outcome::success(Accept::Ok)
        && accept != outcome::success(Accept::WithPrejudice)) {
      SL_ERROR(logger_, "Reject outgoing error.");
      return Error::CLUSTER_TRACKER_ERROR;
    }
    OUTCOME_TRY(check_statement_signature(
        session, session_info.validators, relay_parent, statement));

    cluster_tracker.note_received(
        cluster_sender_index,
        statement.payload.ix,
        network::vstaging::from(getPayload(statement)));

    const auto should_import = (outcome::success(Accept::Ok) == accept);
    if (should_import) {
      return statement;
    }
    return std::nullopt;
  }

  void StatementDistribution::handle_incoming_statement(
      const libp2p::peer::PeerId &peer_id,
      const network::vstaging::StatementDistributionMessageStatement &stm) {
    SL_TRACE(logger_,
             "`StatementDistributionMessageStatement`. (relay_parent={}, "
             "candidate_hash={})",
             stm.relay_parent,
             candidateHash(getPayload(stm.compact)));
    auto parachain_state = tryGetStateByRelayParent(stm.relay_parent);
    if (!parachain_state) {
      SL_TRACE(logger_,
               "After request pov no parachain state on relay_parent. (relay "
               "parent={})",
               stm.relay_parent);
      return;
    }

    const auto &session_info =
        parachain_state->get().per_session_state->value().session_info;
    if (parachain_state->get().is_disabled(stm.compact.payload.ix)) {
      SL_TRACE(
          logger_,
          "Ignoring a statement from disabled validator. (relay parent={}, "
          "validator={})",
          stm.relay_parent,
          stm.compact.payload.ix);
      return;
    }

    CHECK_OR_RET(parachain_state->get().local_validator);
    auto &local_validator = *parachain_state->get().local_validator;
    auto originator_group =
        parachain_state->get()
            .per_session_state->value()
            .groups.byValidatorIndex(stm.compact.payload.ix);
    if (!originator_group) {
      SL_TRACE(logger_,
               "No correct validator index in statement. (relay parent={}, "
               "validator={})",
               stm.relay_parent,
               stm.compact.payload.ix);
      return;
    }

    auto &active = local_validator.active;
    auto cluster_sender_index = [&]() -> std::optional<ValidatorIndex> {
      std::span<const ValidatorIndex> allowed_senders;
      if (active) {
        allowed_senders = active->cluster_tracker.senders_for_originator(
            stm.compact.payload.ix);
      }

      if (auto peer = query_audi->get(peer_id)) {
        for (const auto i : allowed_senders) {
          if (i < session_info.discovery_keys.size()
              && *peer == session_info.discovery_keys[i]) {
            return i;
          }
        }
      }
      return std::nullopt;
    }();

    if (active && cluster_sender_index) {
      if (handle_cluster_statement(
              stm.relay_parent,
              active->cluster_tracker,
              parachain_state->get().per_session_state->value().session,
              parachain_state->get().per_session_state->value().session_info,
              stm.compact,
              *cluster_sender_index)
              .has_error()) {
        return;
      }
    } else {
      std::optional<std::pair<ValidatorIndex, bool>> grid_sender_index;
      for (const auto &[i, validator_knows_statement] :
           local_validator.grid_tracker.direct_statement_providers(
               parachain_state->get().per_session_state->value().groups,
               stm.compact.payload.ix,
               getPayload(stm.compact))) {
        if (i >= session_info.discovery_keys.size()) {
          continue;
        }

        /// TODO(iceseer): do check is authority
        /// const auto &ad = opt_session_info->discovery_keys[i];
        grid_sender_index.emplace(i, validator_knows_statement);
        break;
      }

      CHECK_OR_RET(grid_sender_index);
      const auto &[gsi, validator_knows_statement] = *grid_sender_index;

      CHECK_OR_RET(!validator_knows_statement);
      if (handle_grid_statement(stm.relay_parent,
                                parachain_state->get(),
                                local_validator.grid_tracker,
                                stm.compact,
                                gsi)
              .has_error()) {
        return;
      }
    }

    const auto &statement = getPayload(stm.compact);
    const auto originator_index = stm.compact.payload.ix;
    const auto &candidate_hash = candidateHash(getPayload(stm.compact));
    const bool res = candidates.insert_unconfirmed(peer_id,
                                                   candidate_hash,
                                                   stm.relay_parent,
                                                   *originator_group,
                                                   std::nullopt);
    CHECK_OR_RET(res);
    const auto confirmed = candidates.get_confirmed(candidate_hash);
    const auto is_confirmed = candidates.is_confirmed(candidate_hash);
    const auto &group = session_info.validator_groups[*originator_group];

    if (!is_confirmed) {
      request_attested_candidate(peer_id,
                                 parachain_state->get(),
                                 stm.relay_parent,
                                 candidate_hash,
                                 *originator_group);
    }

    /// TODO(iceseer): do https://github.com/qdrvm/kagome/issues/1888
    /// check statement signature

    const auto was_fresh_opt = parachain_state->get().statement_store->insert(
        parachain_state->get().per_session_state->value().groups,
        stm.compact,
        StatementOrigin::Remote);
    if (!was_fresh_opt) {
      SL_WARN(logger_,
              "Accepted message from unknown validator. (relay parent={}, "
              "validator={})",
              stm.relay_parent,
              stm.compact.payload.ix);
      return;
    }

    if (!*was_fresh_opt) {
      SL_TRACE(logger_,
               "Statement was not fresh. (relay parent={}, validator={})",
               stm.relay_parent,
               stm.compact.payload.ix);
      return;
    }

    const auto is_importable = candidates.is_importable(candidate_hash);
    if (parachain_state->get().per_session_state->value().grid_view) {
      local_validator.grid_tracker.learned_fresh_statement(
          parachain_state->get().per_session_state->value().groups,
          *parachain_state->get().per_session_state->value().grid_view,
          originator_index,
          statement);
    }

    if (is_importable && confirmed) {
      send_backing_fresh_statements(confirmed->get(),
                                    stm.relay_parent,
                                    parachain_state->get(),
                                    group,
                                    candidate_hash);
    }

    circulate_statement(stm.relay_parent, parachain_state->get(), stm.compact);
  }

  outcome::result<
      std::reference_wrapper<const network::vstaging::SignedCompactStatement>>
  StatementDistribution::check_statement_signature(
      SessionIndex session_index,
      const std::vector<ValidatorId> &validators,
      const RelayHash &relay_parent,
      const network::vstaging::SignedCompactStatement &statement) {
    OUTCOME_TRY(signing_context,
                SigningContext::make(parachain_host_, relay_parent));
    OUTCOME_TRY(verified,
                crypto_provider_->verify(
                    statement.signature,
                    signing_context.signable(*hasher_, getPayload(statement)),
                    validators[statement.payload.ix]));

    if (!verified) {
      return Error::INCORRECT_SIGNATURE;
    }
    return std::cref(statement);
  }

  void StatementDistribution::circulate_statement(
      const RelayHash &relay_parent,
      PerRelayParentState &relay_parent_state,
      const IndexedAndSigned<network::vstaging::CompactStatement> &statement) {
    const auto &session_info =
        relay_parent_state.per_session_state->value().session_info;
    const auto &compact_statement = getPayload(statement);
    const auto &candidate_hash = candidateHash(compact_statement);
    const auto originator = statement.payload.ix;
    const auto is_confirmed = candidates.is_confirmed(candidate_hash);

    CHECK_OR_RET(relay_parent_state.local_validator);
    enum DirectTargetKind : uint8_t {
      Cluster,
      Grid,
    };

    auto &local_validator = *relay_parent_state.local_validator;
    auto targets =
        [&]() -> std::vector<std::pair<ValidatorIndex, DirectTargetKind>> {
      auto statement_group =
          relay_parent_state.per_session_state->value().groups.byValidatorIndex(
              originator);

      bool cluster_relevant = false;
      std::vector<std::pair<ValidatorIndex, DirectTargetKind>> targets;
      std::span<const ValidatorIndex> all_cluster_targets;

      if (local_validator.active) {
        auto &active = *local_validator.active;
        cluster_relevant =
            (statement_group && *statement_group == active.group);
        if (is_confirmed && cluster_relevant) {
          for (const auto v : active.cluster_tracker.targets()) {
            if (active.cluster_tracker
                    .can_send(v,
                              originator,
                              network::vstaging::from(compact_statement))
                    .has_error()) {
              continue;
            }
            if (v == active.index) {
              continue;
            }
            if (v >= session_info.discovery_keys.size()) {
              continue;
            }
            targets.emplace_back(v, DirectTargetKind::Cluster);
          }
        }
        all_cluster_targets = active.cluster_tracker.targets();
      }

      for (const auto v : local_validator.grid_tracker.direct_statement_targets(
               relay_parent_state.per_session_state->value().groups,
               originator,
               compact_statement)) {
        const auto can_use_grid = !cluster_relevant
                               || std::ranges::find(all_cluster_targets, v)
                                      == all_cluster_targets.end();
        if (!can_use_grid) {
          continue;
        }
        if (v >= session_info.discovery_keys.size()) {
          continue;
        }
        targets.emplace_back(v, DirectTargetKind::Grid);
      }

      return targets;
    }();

    std::vector<std::pair<libp2p::peer::PeerId, network::CollationVersion>>
        statement_to_peers;
    for (const auto &[target, kind] : targets) {
      auto peer = query_audi->get(session_info.discovery_keys[target]);
      if (!peer) {
        continue;
      }

      auto peer_state = pm_->getPeerState(peer->id);
      if (!peer_state) {
        continue;
      }

      if (!peer_state->get().knows_relay_parent(relay_parent)) {
        continue;
      }

      network::CollationVersion version = network::CollationVersion::VStaging;
      if (peer_state->get().collation_version) {
        version = *peer_state->get().collation_version;
      }

      switch (kind) {
        case Cluster: {
          auto &active = *local_validator.active;
          if (active.cluster_tracker
                  .can_send(target,
                            originator,
                            network::vstaging::from(compact_statement))
                  .has_value()) {
            active.cluster_tracker.note_sent(
                target, originator, network::vstaging::from(compact_statement));
            statement_to_peers.emplace_back(peer->id, version);
          }
        } break;
        case Grid: {
          statement_to_peers.emplace_back(peer->id, version);
          local_validator.grid_tracker.sent_or_received_direct_statement(
              relay_parent_state.per_session_state->value().groups,
              originator,
              target,
              compact_statement,
              false);
        } break;
      }
    }

    auto se = pm_->getStreamEngine();
    auto message_v2 = std::make_shared<
        network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
        kagome::network::vstaging::ValidatorProtocolMessage{
            kagome::network::vstaging::StatementDistributionMessage{
                kagome::network::vstaging::
                    StatementDistributionMessageStatement{
                        .relay_parent = relay_parent,
                        .compact = statement,
                    }}});
    SL_TRACE(
        logger_,
        "Send statements to validators. (relay_parent={}, validators_count={})",
        relay_parent,
        statement_to_peers.size());
    for (const auto &[peer, version] : statement_to_peers) {
      if (version == network::CollationVersion::VStaging) {
        se->send(peer, router_->getValidationProtocolVStaging(), message_v2);
      } else {
        BOOST_ASSERT(false);
      }
    }
  }

  void StatementDistribution::share_local_statement(
      PerRelayParentState &per_relay_parent,
      const primitives::BlockHash &relay_parent,
      const SignedFullStatementWithPVD &statement) {
    const CandidateHash candidate_hash =
        candidateHashFrom(getPayload(statement));
    SL_TRACE(logger_,
             "Sharing statement. (relay parent={}, candidate hash={}, "
             "our_index={}, statement_ix={})",
             relay_parent,
             candidate_hash,
             *per_relay_parent.our_index,
             statement.payload.ix);

    BOOST_ASSERT(per_relay_parent.our_index);

    const Groups &groups = per_relay_parent.per_session_state->value().groups;
    const std::optional<network::ParachainId> &local_assignment =
        per_relay_parent.assigned_para;
    const network::ValidatorIndex local_index = *per_relay_parent.our_index;
    const auto local_group_opt = groups.byValidatorIndex(local_index);
    const GroupIndex local_group = *local_group_opt;

    std::optional<std::pair<ParachainId, Hash>> expected = visit_in_place(
        getPayload(statement),
        [&](const StatementWithPVDSeconded &v)
            -> std::optional<std::pair<ParachainId, Hash>> {
          return std::make_pair(v.committed_receipt.descriptor.para_id,
                                v.committed_receipt.descriptor.relay_parent);
        },
        [&](const StatementWithPVDValid &v)
            -> std::optional<std::pair<ParachainId, Hash>> {
          if (auto p = candidates.get_confirmed(v.candidate_hash)) {
            return std::make_pair(p->get().para_id(), p->get().relay_parent());
          }
          return std::nullopt;
        });
    const bool is_seconded =
        is_type<StatementWithPVDSeconded>(getPayload(statement));

    if (!expected) {
      SL_ERROR(
          logger_, "Invalid share statement. (relay parent={})", relay_parent);
      return;
    }
    const auto &[expected_para, expected_relay_parent] = *expected;

    if (local_index != statement.payload.ix) {
      SL_ERROR(logger_,
               "Invalid share statement because of validator index. (relay "
               "parent={})",
               relay_parent);
      return;
    }

    BOOST_ASSERT(per_relay_parent.statement_store);
    BOOST_ASSERT(per_relay_parent.prospective_parachains_mode);

    const auto seconding_limit =
        per_relay_parent.prospective_parachains_mode->max_candidate_depth + 1;
    if (is_seconded
        && per_relay_parent.statement_store->seconded_count(local_index)
               == seconding_limit) {
      SL_WARN(
          logger_,
          "Local node has issued too many `Seconded` statements. (limit={})",
          seconding_limit);
      return;
    }

    if (!local_assignment || *local_assignment != expected_para
        || relay_parent != expected_relay_parent) {
      SL_ERROR(
          logger_,
          "Invalid share statement because local assignment. (relay parent={})",
          relay_parent);
      return;
    }

    IndexedAndSigned<network::vstaging::CompactStatement> compact_statement =
        signed_to_compact(statement);
    std::optional<PostConfirmation> post_confirmation;
    if (auto s =
            if_type<const StatementWithPVDSeconded>(getPayload(statement))) {
      post_confirmation =
          candidates.confirm_candidate(candidate_hash,
                                       s->get().committed_receipt,
                                       s->get().pvd,
                                       local_group,
                                       hasher_);
    }

    if (auto r = per_relay_parent.statement_store->insert(
            groups, compact_statement, StatementOrigin::Local);
        !r || !*r) {
      SL_ERROR(logger_,
               "Invalid share statement because statement store insertion "
               "failed. (relay parent={})",
               relay_parent);
      return;
    }

    if (per_relay_parent.local_validator
        && per_relay_parent.local_validator->active) {
      per_relay_parent.local_validator->active->cluster_tracker.note_issued(
          local_index, network::vstaging::from(getPayload(compact_statement)));
    }

    if (per_relay_parent.per_session_state->value().grid_view) {
      auto &l = *per_relay_parent.local_validator;
      l.grid_tracker.learned_fresh_statement(
          per_relay_parent.per_session_state->value().groups,
          *per_relay_parent.per_session_state->value().grid_view,
          local_index,
          getPayload(compact_statement));
    }

    circulate_statement(relay_parent, per_relay_parent, compact_statement);
    if (post_confirmation) {
      apply_post_confirmation(*post_confirmation);
    }
  }

  void StatementDistribution::send_pending_grid_messages(
      const RelayHash &relay_parent,
      const libp2p::peer::PeerId &peer_id,
      network::CollationVersion version,
      ValidatorIndex peer_validator_id,
      const Groups &groups,
      PerRelayParentState &relay_parent_state) {
    CHECK_OR_RET(relay_parent_state.local_validator);

    auto pending_manifests =
        relay_parent_state.local_validator->grid_tracker.pending_manifests_for(
            peer_validator_id);
    std::deque<std::pair<std::vector<libp2p::peer::PeerId>,
                         network::VersionedValidatorProtocolMessage>>
        messages;
    for (const auto &[candidate_hash, kind] : pending_manifests) {
      const auto confirmed_candidate = candidates.get_confirmed(candidate_hash);
      if (!confirmed_candidate) {
        continue;
      }

      const auto group_index = confirmed_candidate->get().group_index();
      TRY_GET_OR_RET(group, groups.get(group_index));

      const auto group_size = group->size();
      auto local_knowledge =
          local_knowledge_filter(group_size,
                                 group_index,
                                 candidate_hash,
                                 *relay_parent_state.statement_store);

      switch (kind) {
        case grid::ManifestKind::Full: {
          const network::vstaging::BackedCandidateManifest manifest{
              .relay_parent = relay_parent,
              .candidate_hash = candidate_hash,
              .group_index = group_index,
              .para_id = confirmed_candidate->get().para_id(),
              .parent_head_data_hash =
                  confirmed_candidate->get().parent_head_data_hash(),
              .statement_knowledge = local_knowledge,
          };

          auto &grid = relay_parent_state.local_validator->grid_tracker;
          grid.manifest_sent_to(
              groups, peer_validator_id, candidate_hash, local_knowledge);

          switch (version) {
            case network::CollationVersion::VStaging: {
              messages.emplace_back(
                  std::vector<libp2p::peer::PeerId>{peer_id},
                  network::VersionedValidatorProtocolMessage{
                      kagome::network::vstaging::ValidatorProtocolMessage{
                          kagome::network::vstaging::
                              StatementDistributionMessage{manifest}}});
            } break;
            default: {
              SL_ERROR(logger_,
                       "Bug ValidationVersion::V1 should not be used in "
                       "statement-distribution v2, legacy should have handled "
                       "this.");
            } break;
          };
        } break;
        case grid::ManifestKind::Acknowledgement: {
          auto m = acknowledgement_and_statement_messages(
              peer_id,
              network::CollationVersion::VStaging,
              peer_validator_id,
              groups,
              relay_parent_state,
              relay_parent,
              group_index,
              candidate_hash,
              local_knowledge);
          messages.insert(messages.end(),
                          std::move_iterator(m.begin()),
                          std::move_iterator(m.end()));

        } break;
      }
    }

    {
      auto &grid_tracker = relay_parent_state.local_validator->grid_tracker;
      auto pending_statements =
          grid_tracker.all_pending_statements_for(peer_validator_id);

      for (const auto &[originator, compact] : pending_statements) {
        auto res = pending_statement_network_message(
            *relay_parent_state.statement_store,
            relay_parent,
            peer_id,
            network::CollationVersion::VStaging,
            originator,
            compact);

        if (res) {
          grid_tracker.sent_or_received_direct_statement(
              groups, originator, peer_validator_id, compact, false);

          messages.emplace_back(std::move(*res));
        }
      }
    }

    auto se = pm_->getStreamEngine();
    for (auto &[peers, msg] : messages) {
      if (auto m = if_type<network::vstaging::ValidatorProtocolMessage>(msg)) {
        auto message = std::make_shared<
            network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
            std::move(m->get()));
        for (const auto &p : peers) {
          se->send(p, router_->getValidationProtocolVStaging(), message);
        }
      } else {
        assert(false);
      }
    }
  }

  void StatementDistribution::send_pending_cluster_statements(
      const RelayHash &relay_parent,
      const libp2p::peer::PeerId &peer_id,
      network::CollationVersion version,
      ValidatorIndex peer_validator_id,
      PerRelayParentState &relay_parent_state) {
    CHECK_OR_RET(relay_parent_state.local_validator);
    CHECK_OR_RET(relay_parent_state.local_validator->active);

    const auto pending_statements =
        relay_parent_state.local_validator->active->cluster_tracker
            .pending_statements_for(peer_validator_id);
    std::deque<std::pair<std::vector<libp2p::peer::PeerId>,
                         network::VersionedValidatorProtocolMessage>>
        messages;
    for (const auto &[originator, compact] : pending_statements) {
      if (!candidates.is_confirmed(candidateHash(compact))) {
        continue;
      }

      auto res =
          pending_statement_network_message(*relay_parent_state.statement_store,
                                            relay_parent,
                                            peer_id,
                                            version,
                                            originator,
                                            network::vstaging::from(compact));

      if (res) {
        relay_parent_state.local_validator->active->cluster_tracker.note_sent(
            peer_validator_id, originator, compact);
        messages.emplace_back(*res);
      }
    }

    auto se = pm_->getStreamEngine();
    for (auto &[peers, msg] : messages) {
      if (auto m = if_type<network::vstaging::ValidatorProtocolMessage>(msg)) {
        auto message = std::make_shared<
            network::WireMessage<network::vstaging::ValidatorProtocolMessage>>(
            std::move(m->get()));
        for (const auto &p : peers) {
          se->send(p, router_->getValidationProtocolVStaging(), message);
        }
      } else {
        BOOST_ASSERT(false);
      }
    }
  }

  std::optional<std::pair<std::vector<libp2p::peer::PeerId>,
                          network::VersionedValidatorProtocolMessage>>
  StatementDistribution::pending_statement_network_message(
      const StatementStore &statement_store,
      const RelayHash &relay_parent,
      const libp2p::peer::PeerId &peer,
      network::CollationVersion version,
      ValidatorIndex originator,
      const network::vstaging::CompactStatement &compact) {
    switch (version) {
      case network::CollationVersion::VStaging: {
        auto s = statement_store.validator_statement(originator, compact);
        if (s) {
          return std::make_pair(
              std::vector<libp2p::peer::PeerId>{peer},
              network::VersionedValidatorProtocolMessage{
                  network::vstaging::ValidatorProtocolMessage{
                      network::vstaging::StatementDistributionMessage{
                          network::vstaging::
                              StatementDistributionMessageStatement{
                                  .relay_parent = relay_parent,
                                  .compact = s->get().statement,
                              }}}});
        }
      } break;
      default: {
        SL_ERROR(logger_,
                 "Bug ValidationVersion::V1 should not be used in "
                 "statement-distribution v2, legacy should have handled this");
      } break;
    }
    return {};
  }

  void StatementDistribution::send_peer_messages_for_relay_parent(
      const libp2p::peer::PeerId &peer_id, const RelayHash &relay_parent) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    // TRY_GET_OR_RET(peer_state, pm_->getPeerState(peer_id));
    TRY_GET_OR_RET(parachain_state, tryGetStateByRelayParent(relay_parent));

    network::CollationVersion version = network::CollationVersion::VStaging;
    //    if (peer_state->get().collation_version) {
    //      version = *peer_state->get().collation_version;
    //    }

    if (auto auth_id = query_audi->get(peer_id)) {
      if (auto it = parachain_state->get().authority_lookup.find(*auth_id);
          it != parachain_state->get().authority_lookup.end()) {
        ValidatorIndex vi = it->second;

        SL_TRACE(logger_,
                 "Send pending cluster/grid messages. (peer={}. validator "
                 "index={}, relay_parent={})",
                 peer_id,
                 vi,
                 relay_parent);
        send_pending_cluster_statements(
            relay_parent, peer_id, version, vi, parachain_state->get());

        send_pending_grid_messages(
            relay_parent,
            peer_id,
            version,
            vi,
            parachain_state->get().per_session_state->value().groups,
            parachain_state->get());
      }
    }
  }

  std::optional<LocalValidatorState>
  StatementDistribution::find_active_validator_state(
      ValidatorIndex validator_index,
      const Groups &groups,
      const std::vector<runtime::CoreState> &availability_cores,
      const runtime::GroupDescriptor &group_rotation_info,
      const std::optional<runtime::ClaimQueueSnapshot> &maybe_claim_queue,
      size_t seconding_limit,
      size_t max_candidate_depth) {
    BOOST_ASSERT(statements_distribution_thread_handler->isInCurrentThread());

    if (groups.all_empty()) {
      return std::nullopt;
    }

    const auto our_group = groups.byValidatorIndex(validator_index);
    if (!our_group) {
      return std::nullopt;
    }

    const auto core_index =
        group_rotation_info.coreForGroup(*our_group, availability_cores.size());
    std::optional<ParachainId> para_assigned_to_core;
    if (maybe_claim_queue) {
      para_assigned_to_core = maybe_claim_queue->get_claim_for(core_index, 0);
    } else {
      if (core_index < availability_cores.size()) {
        const auto &core_state = availability_cores[core_index];
        visit_in_place(
            core_state,
            [&](const runtime::ScheduledCore &scheduled) {
              para_assigned_to_core = scheduled.para_id;
            },
            [&](const runtime::OccupiedCore &occupied) {
              if (max_candidate_depth >= 1 && occupied.next_up_on_available) {
                para_assigned_to_core = occupied.next_up_on_available->para_id;
              }
            },
            [](const auto &) {});
      }
    }

    const auto group_validators = groups.get(*our_group);
    if (!group_validators) {
      return std::nullopt;
    }

    return LocalValidatorState{
        .grid_tracker = {},
        .active =
            ActiveValidatorState{
                .index = validator_index,
                .group = *our_group,
                .assignment = para_assigned_to_core,
                .cluster_tracker = ClusterTracker(
                    {group_validators->begin(), group_validators->end()},
                    seconding_limit),
            },
    };
  }

}  // namespace kagome::parachain::statement_distribution
