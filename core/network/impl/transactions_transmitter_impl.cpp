/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include "network/impl/transactions_transmitter_impl.hpp"

#include "network/impl/protocols/propagate_transactions_protocol.hpp"
#include "network/router.hpp"

namespace kagome::network {

  TransactionsTransmitterImpl::TransactionsTransmitterImpl(
      std::shared_ptr<network::Router> router)
      : router_(std::move(router)) {}

  void TransactionsTransmitterImpl::propagateTransactions(
      std::span<const primitives::Transaction> txs) {
    auto protocol = router_->getPropagateTransactionsProtocol();
    BOOST_ASSERT_MSG(protocol,
                     "Router did not provide propagate transactions protocol");
    protocol->propagateTransactions(txs);
  }
}  // namespace kagome::network
