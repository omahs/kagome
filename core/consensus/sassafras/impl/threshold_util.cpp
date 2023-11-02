/**
 * Copyright Quadrivium LLC
 * All Rights Reserved
 * SPDX-License-Identifier: Apache-2.0
 */

#include "consensus/sassafras/impl/threshold_util.hpp"

#include <boost/range/adaptors.hpp>
#include <boost/range/numeric.hpp>

namespace kagome::consensus::sassafras {

  TicketId ticket_id_threshold(RedundancyFactor redundancy,
                               SlotNumber slots,
                               AttemptsNumber attempts,
                               uint32_t validators) {
    BOOST_ASSERT(attempts > 0);
    BOOST_ASSERT(validators > 0);

    return TicketId(
        std::numeric_limits<TicketId::ValueType>::max()
        / static_cast<uint64_t>(attempts) / static_cast<uint64_t>(validators)
        * static_cast<uint64_t>(redundancy) * static_cast<uint64_t>(slots));
  }

  Threshold calculateThreshold(const std::pair<uint64_t, uint64_t> &ratio,
                               const primitives::AuthorityList &authorities,
                               primitives::AuthorityIndex authority_index) {
    double float_point_ratio = double(ratio.first) / ratio.second;

    using boost::adaptors::transformed;
    double theta = double(authorities[authority_index].weight)
                 / boost::accumulate(
                       authorities | transformed([](const auto &authority) {
                         return authority.weight;
                       }),
                       0.);

    using namespace boost::multiprecision;  // NOLINT
    cpp_rational p_rat(1. - pow(1. - float_point_ratio, theta));
    static const auto a = (uint256_t{1} << 128);
    return Threshold{a * numerator(p_rat) / denominator(p_rat)};
  }

}  // namespace kagome::consensus::sassafras