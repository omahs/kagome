/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include "api/service/author/impl/author_api_impl.hpp"

#include <jsonrpc-lean/fault.h>
#include <boost/assert.hpp>
#include <boost/system/error_code.hpp>
#include <stdexcept>

#include "api/service/api_service.hpp"
#include "blockchain/block_tree.hpp"
#include "crypto/crypto_store.hpp"
#include "crypto/crypto_store/crypto_store_impl.hpp"
#include "crypto/crypto_store/crypto_suites.hpp"
#include "crypto/crypto_store/key_file_storage.hpp"
#include "crypto/crypto_store/session_keys.hpp"
#include "crypto/hasher.hpp"
#include "primitives/transaction.hpp"
#include "runtime/runtime_api/session_keys_api.hpp"
#include "scale/scale_decoder_stream.hpp"
#include "subscription/subscriber.hpp"
#include "transaction_pool/transaction_pool.hpp"

namespace kagome::api {
  const std::vector<crypto::KeyType> kKeyTypes{
      crypto::KeyTypes::BABE,
      crypto::KeyTypes::GRANDPA,
      crypto::KeyTypes::AUTHORITY_DISCOVERY,
  };

  AuthorApiImpl::AuthorApiImpl(sptr<runtime::SessionKeysApi> key_api,
                               sptr<transaction_pool::TransactionPool> pool,
                               sptr<crypto::CryptoStore> store,
                               sptr<crypto::SessionKeys> keys,
                               sptr<crypto::KeyFileStorage> key_store,
                               LazySPtr<blockchain::BlockTree> block_tree,
                               LazySPtr<api::ApiService> api_service)
      : keys_api_(std::move(key_api)),
        pool_{std::move(pool)},
        store_{std::move(store)},
        keys_{std::move(keys)},
        key_store_{std::move(key_store)},
        api_service_{std::move(api_service)},
        block_tree_{std::move(block_tree)},
        logger_{log::createLogger("AuthorApi", "author_api")} {
    BOOST_ASSERT_MSG(keys_api_ != nullptr, "session keys api is nullptr");
    BOOST_ASSERT_MSG(pool_ != nullptr, "transaction pool is nullptr");
    BOOST_ASSERT_MSG(store_ != nullptr, "crypto store is nullptr");
    BOOST_ASSERT_MSG(keys_ != nullptr, "session keys store is nullptr");
    BOOST_ASSERT_MSG(key_store_ != nullptr, "key store is nullptr");
    BOOST_ASSERT_MSG(logger_ != nullptr, "logger is nullptr");
  }

  outcome::result<common::Hash256> AuthorApiImpl::submitExtrinsic(
      primitives::TransactionSource source,
      const primitives::Extrinsic &extrinsic) {
    return pool_->submitExtrinsic(source, extrinsic);
  }

  outcome::result<void> AuthorApiImpl::insertKey(crypto::KeyType key_type_id,
                                                 const BufferView &seed,
                                                 const BufferView &public_key) {
    if (std::find(kKeyTypes.begin(), kKeyTypes.end(), key_type_id)
        == kKeyTypes.end()) {
      std::string types;
      for (auto &type : kKeyTypes) {
        types.append(crypto::encodeKeyTypeToStr(type));
        types.push_back(' ');
      }
      types.pop_back();
      SL_INFO(logger_, "Unsupported key type, only [{}] are accepted", types);
      return Q_ERROR(crypto::CryptoStoreError::UNSUPPORTED_KEY_TYPE);
    };
    if (crypto::KeyTypes::BABE == key_type_id
        or crypto::KeyTypes::AUTHORITY_DISCOVERY == key_type_id) {
      OUTCOME_TRY(seed_typed, crypto::Sr25519Seed::fromSpan(seed));
      OUTCOME_TRY(public_key_typed,
                  crypto::Sr25519PublicKey::fromSpan(public_key));
      OUTCOME_TRY(keypair,
                  store_->generateSr25519Keypair(key_type_id, seed_typed));
      if (public_key_typed != keypair.public_key) {
        return Q_ERROR(crypto::CryptoStoreError::WRONG_PUBLIC_KEY);
      }
    }
    if (crypto::KeyTypes::GRANDPA == key_type_id) {
      OUTCOME_TRY(seed_typed, crypto::Ed25519Seed::fromSpan(seed));
      OUTCOME_TRY(public_key_typed,
                  crypto::Ed25519PublicKey::fromSpan(public_key));
      OUTCOME_TRY(keypair,
                  store_->generateEd25519Keypair(crypto::KeyTypes::GRANDPA,
                                                 seed_typed));
      if (public_key_typed != keypair.public_key) {
        return Q_ERROR(crypto::CryptoStoreError::WRONG_PUBLIC_KEY);
      }
    }
    auto res = key_store_->saveKeyPair(key_type_id, public_key, seed);
    return res;
  }

  outcome::result<common::Buffer> AuthorApiImpl::rotateKeys() {
    OUTCOME_TRY(encoded_session_keys,
                keys_api_->generate_session_keys(
                    block_tree_.get()->bestBlock().hash, std::nullopt));
    return encoded_session_keys;
  }

  // logic here is polkadot specific only!
  // it could be extended by reading config from chainspec palletSession/keys
  // value
  outcome::result<bool> AuthorApiImpl::hasSessionKeys(const BufferView &keys) {
    scale::ScaleDecoderStream stream(keys);
    std::array<uint8_t, 32> key;
    if (keys.size() < 32 || keys.size() > 32 * 6 || (keys.size() % 32) != 0) {
      SL_WARN(logger_,
              "not valid key sequence, author_hasSessionKeys RPC call expects "
              "no more than 6 public keys in concatenated string, keys should "
              "be 32 byte in size");
      return false;
    }
    stream >> key;
    if (store_->findEd25519Keypair(
            crypto::KeyTypes::GRANDPA,
            crypto::Ed25519PublicKey(common::Blob<32>(key)))) {
      unsigned count = 1;
      while (stream.currentIndex() < keys.size()) {
        stream >> key;
        if (not store_->findSr25519Keypair(
                crypto::polkadot_key_order[count++],
                crypto::Sr25519PublicKey(common::Blob<32>(key)))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  outcome::result<bool> AuthorApiImpl::hasKey(const BufferView &public_key,
                                              crypto::KeyType key_type) {
    auto res = key_store_->searchForPhrase(key_type, public_key);
    if (not res) {
      return res.error();
    } else {
      return res.value() ? true : false;
    }
  }

  outcome::result<std::vector<primitives::Extrinsic>>
  AuthorApiImpl::pendingExtrinsics() {
    std::vector<primitives::Extrinsic> result;
    /// TODO(iceseer): return size, to make reserved allocation
    pool_->getPendingTransactions(
        [&](const auto &tx) { result.emplace_back(tx->ext); });
    return result;
  }

  outcome::result<std::vector<primitives::Extrinsic>>
  AuthorApiImpl::removeExtrinsic(
      const std::vector<primitives::ExtrinsicKey> &keys) {
    BOOST_ASSERT_MSG(false, "not implemented");  // NOLINT
    return Q_ERROR(std::errc::not_supported);
  }

  outcome::result<AuthorApi::SubscriptionId>
  AuthorApiImpl::submitAndWatchExtrinsic(Extrinsic extrinsic) {
    if (auto service = api_service_.get()) {
      OUTCOME_TRY(
          tx,
          pool_->constructTransaction(TransactionSource::External, extrinsic));
      OUTCOME_TRY(sub_id, service->subscribeForExtrinsicLifecycle(tx.hash));
      OUTCOME_TRY(tx_hash,
                  submitExtrinsic(
                      // submit and watch could be executed only
                      // from RPC call, so External source is chosen
                      TransactionSource::External,
                      extrinsic));
      BOOST_ASSERT(tx_hash == tx.hash);

      SL_DEBUG(logger_, "Submit and watch transaction with hash {}", tx_hash);

      return sub_id;
    }

    throw jsonrpc::InternalErrorFault(
        "Internal error. Api service not initialized.");
  }

  outcome::result<bool> AuthorApiImpl::unwatchExtrinsic(SubscriptionId sub_id) {
    if (auto service = api_service_.get()) {
      return service->unsubscribeFromExtrinsicLifecycle(sub_id);
    }
    throw jsonrpc::InternalErrorFault(
        "Internal error. Api service not initialized.");
  }

}  // namespace kagome::api
