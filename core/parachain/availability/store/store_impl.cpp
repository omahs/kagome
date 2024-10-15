/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include "parachain/availability/store/store_impl.hpp"
#include "candidate_chunk_key.hpp"

namespace kagome::parachain {
  AvailabilityStoreImpl::AvailabilityStoreImpl(
      std::shared_ptr<storage::SpacedStorage> storage)
      : storage_{std::move(storage)} {
    BOOST_ASSERT(storage_ != nullptr);
  }

  bool AvailabilityStoreImpl::hasChunk(const CandidateHash &candidate_hash,
                                       ValidatorIndex index) const {
    return state_.sharedAccess([&](const auto &state) {
      auto it = state.per_candidate_.find(candidate_hash);
      if (it == state.per_candidate_.end()) {
        return false;
      }
      return it->second.chunks.count(index) != 0;
    });
  }

  bool AvailabilityStoreImpl::hasPov(
      const CandidateHash &candidate_hash) const {
    return state_.sharedAccess([&](const auto &state) {
      auto it = state.per_candidate_.find(candidate_hash);
      if (it == state.per_candidate_.end()) {
        return false;
      }
      return it->second.pov.has_value();
    });
  }

  bool AvailabilityStoreImpl::hasData(
      const CandidateHash &candidate_hash) const {
    return state_.sharedAccess([&](const auto &state) {
      auto it = state.per_candidate_.find(candidate_hash);
      if (it == state.per_candidate_.end()) {
        return false;
      }
      return it->second.data.has_value();
    });
  }

  std::optional<AvailabilityStore::ErasureChunk>
  AvailabilityStoreImpl::getChunk(const CandidateHash &candidate_hash,
                                  ValidatorIndex index) const {
    return state_.sharedAccess(
        [&](const auto &state)
            -> std::optional<AvailabilityStore::ErasureChunk> {
          auto it = state.per_candidate_.find(candidate_hash);
          if (it == state.per_candidate_.end()) {
            return std::nullopt;
          }
          auto it2 = it->second.chunks.find(index);
          if (it2 == it->second.chunks.end()) {
            return std::nullopt;
          }
          return it2->second;
        });
  }

  std::optional<AvailabilityStore::ParachainBlock>
  AvailabilityStoreImpl::getPov(const CandidateHash &candidate_hash) const {
    return state_.sharedAccess(
        [&](const auto &state)
            -> std::optional<AvailabilityStore::ParachainBlock> {
          auto it = state.per_candidate_.find(candidate_hash);
          if (it == state.per_candidate_.end()) {
            return std::nullopt;
          }
          return it->second.pov;
        });
  }

  std::optional<AvailabilityStore::AvailableData>
  AvailabilityStoreImpl::getPovAndData(
      const CandidateHash &candidate_hash) const {
    return state_.sharedAccess(
        [&](const auto &state)
            -> std::optional<AvailabilityStore::AvailableData> {
          auto it = state.per_candidate_.find(candidate_hash);
          if (it == state.per_candidate_.end()) {
            return std::nullopt;
          }
          if (not it->second.pov or not it->second.data) {
            return std::nullopt;
          }
          return AvailableData{*it->second.pov, *it->second.data};
        });
  }

  std::vector<AvailabilityStore::ErasureChunk> AvailabilityStoreImpl::getChunks(
      const CandidateHash &candidate_hash) const {
    auto chunks = state_.sharedAccess([&](const auto &state) {
      std::vector<AvailabilityStore::ErasureChunk> chunks;
      auto it = state.per_candidate_.find(candidate_hash);
      if (it != state.per_candidate_.end()) {
        for (auto &p : it->second.chunks) {
          chunks.emplace_back(p.second);
        }
      }
      return chunks;
    });
    if (chunks.empty()) {
      auto space = storage_->getSpace(storage::Space::kAvaliabilityStorage);
      if (not space) {
        SL_ERROR(logger, "Failed to get space");
        return chunks;
      }
      auto cursor =
          storage_->getSpace(storage::Space::kAvaliabilityStorage)->cursor();
      auto seek_res =
          cursor->seek(CandidateChunkKey::encode_hash(candidate_hash));
      if (not seek_res) {
        SL_ERROR(logger, "Failed to seek, error: {}", seek_res.error());
        return chunks;
      }
      if (not seek_res.value()) {
        SL_INFO(logger, "Failed to seek, not found");
        return chunks;
      }
      auto seek_first_res = cursor->seekFirst();
      if (not seek_first_res) {
        SL_ERROR(
            logger, "Failed to seek first, error: {}", seek_first_res.error());
        return chunks;
      }
      if (not seek_first_res.value()) {
        SL_INFO(logger, "Failed to seek first, not found");
        return chunks;
      }
      while (1) {
        const auto cursor_opt_value = cursor->value();
        if (cursor_opt_value) {
          auto decoded_res = scale::decode<ErasureChunk>(*cursor_opt_value);
          if (decoded_res) {
            chunks.emplace_back(std::move(decoded_res.value()));
          } else {
            SL_ERROR(logger,
                     "Failed to decode value, error: {}",
                     decoded_res.error());
          }
        } else {
          SL_ERROR(logger,
                   "Failed to get value for key {}",
                   cursor->key()->toString());
        }
        if (not cursor->next()) {
          break;
        }
      }
    }
    return chunks;
  }

  void AvailabilityStoreImpl::printStoragesLoad() {
    state_.sharedAccess([&](auto &state) {
      SL_TRACE(logger,
               "[Availability store statistics]:"
               "\n\t-> state.candidates={}"
               "\n\t-> state.per_candidate={}",
               state.candidates_.size(),
               state.per_candidate_.size());
    });
  }

  void AvailabilityStoreImpl::storeData(const network::RelayHash &relay_parent,
                                        const CandidateHash &candidate_hash,
                                        std::vector<ErasureChunk> &&chunks,
                                        const ParachainBlock &pov,
                                        const PersistedValidationData &data) {
    state_.exclusiveAccess([&](auto &state) {
      state.candidates_[relay_parent].insert(candidate_hash);
      auto &candidate_data = state.per_candidate_[candidate_hash];
      for (auto &&chunk : std::move(chunks)) {
        candidate_data.chunks[chunk.index] = std::move(chunk);
      }
      candidate_data.pov = pov;
      candidate_data.data = data;
    });
  }

  void AvailabilityStoreImpl::putChunk(const network::RelayHash &relay_parent,
                                       const CandidateHash &candidate_hash,
                                       ErasureChunk &&chunk) {
    state_.exclusiveAccess([&](auto &state) {
      state.candidates_[relay_parent].insert(candidate_hash);
      state.per_candidate_[candidate_hash].chunks[chunk.index] = chunk;
    });
    auto space = storage_->getSpace(storage::Space::kAvaliabilityStorage);
    if (not space) {
      SL_ERROR(logger, "Failed to get space");
      return;
    }
    auto encoded_chunk = scale::encode(chunk);
    if (not encoded_chunk) {
      SL_ERROR(
          logger, "Failed to encode chunk, error: {}", encoded_chunk.error());
      return;
    }
    space->put(CandidateChunkKey::encode({candidate_hash, chunk.index}),
               std::move(encoded_chunk.value()));
  }

  void AvailabilityStoreImpl::remove(const network::RelayHash &relay_parent) {
    state_.exclusiveAccess([&](auto &state) {
      if (auto it = state.candidates_.find(relay_parent);
          it != state.candidates_.end()) {
        for (const auto &l : it->second) {
          state.per_candidate_.erase(l);
        }
        state.candidates_.erase(it);
      }
    });
  }
}  // namespace kagome::parachain
