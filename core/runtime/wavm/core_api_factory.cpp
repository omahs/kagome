/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "runtime/wavm/core_api_factory.hpp"

#include "host_api/host_api_factory.hpp"
#include "runtime/common/constant_code_provider.hpp"
#include "runtime/common/trie_storage_provider_impl.hpp"
#include "runtime/executor.hpp"
#include "runtime/module_repository.hpp"
#include "runtime/runtime_api/impl/core.hpp"
#include "runtime/wavm/compartment_wrapper.hpp"
#include "runtime/wavm/intrinsics/intrinsic_functions.hpp"
#include "runtime/wavm/intrinsics/intrinsic_module.hpp"
#include "runtime/wavm/intrinsics/intrinsic_resolver_impl.hpp"
#include "runtime/wavm/module.hpp"
#include "runtime/wavm/module_instance.hpp"
#include "runtime/wavm/wavm_memory_provider.hpp"

namespace kagome::runtime::wavm {

  class OneModuleRepository final : public ModuleRepository {
   public:
    OneModuleRepository(std::shared_ptr<CompartmentWrapper> compartment,
                        std::shared_ptr<IntrinsicResolver> resolver,
                        gsl::span<const uint8_t> code)
        : resolver_{std::move(resolver)},
          compartment_{compartment},
          code_{code} {
      BOOST_ASSERT(resolver_);
      BOOST_ASSERT(compartment_);
    }

    outcome::result<std::shared_ptr<runtime::ModuleInstance>> getInstanceAt(
        std::shared_ptr<const RuntimeCodeProvider>,
        const primitives::BlockInfo &) override {
      if (instance_ == nullptr) {
        auto module = ModuleImpl::compileFrom(compartment_, resolver_, code_);
        OUTCOME_TRY(inst, module->instantiate());
        instance_ = std::move(inst);
      }
      return instance_;
    }

   private:
    std::shared_ptr<runtime::ModuleInstance> instance_;
    std::shared_ptr<IntrinsicResolver> resolver_;
    std::shared_ptr<CompartmentWrapper> compartment_;
    gsl::span<const uint8_t> code_;
  };

  class OneCodeProvider final : public RuntimeCodeProvider {
   public:
    explicit OneCodeProvider(gsl::span<const uint8_t> code) : code_{code} {}

    virtual outcome::result<gsl::span<const uint8_t>> getCodeAt(
        const storage::trie::RootHash &) const {
      return code_;
    }

   private:
    gsl::span<const uint8_t> code_;
  };

  CoreApiFactory::CoreApiFactory(
      std::shared_ptr<CompartmentWrapper> compartment,
      std::shared_ptr<runtime::wavm::IntrinsicModule> intrinsic_module,
      std::shared_ptr<storage::trie::TrieStorage> storage,
      std::shared_ptr<blockchain::BlockHeaderRepository> block_header_repo,
      std::shared_ptr<storage::changes_trie::ChangesTracker> changes_tracker,
      std::shared_ptr<host_api::HostApiFactory> host_api_factory)
      : compartment_{compartment},
        intrinsic_module_{std::move(intrinsic_module)},
        storage_{std::move(storage)},
        block_header_repo_{std::move(block_header_repo)},
        changes_tracker_{std::move(changes_tracker)},
        host_api_factory_{std::move(host_api_factory)} {
    BOOST_ASSERT(compartment_);
    BOOST_ASSERT(intrinsic_module_);
    BOOST_ASSERT(storage_);
    BOOST_ASSERT(block_header_repo_);
    BOOST_ASSERT(changes_tracker_);
    BOOST_ASSERT(host_api_factory_);
  }

  std::unique_ptr<Core> CoreApiFactory::make(
      std::shared_ptr<const crypto::Hasher> hasher,
      const std::vector<uint8_t> &runtime_code) const {
    auto new_intrinsic_module = std::shared_ptr<IntrinsicModuleInstance>(
        intrinsic_module_->instantiate());
    auto new_memory_provider = std::make_shared<WavmMemoryProvider>(
        new_intrinsic_module, compartment_);
    auto new_storage_provider =
        std::make_shared<TrieStorageProviderImpl>(storage_);
    auto host_api = std::shared_ptr<host_api::HostApi>(host_api_factory_->make(
        shared_from_this(), new_memory_provider, new_storage_provider));
    auto env_factory = std::make_shared<runtime::RuntimeEnvironmentFactory>(
        new_storage_provider,
        host_api,
        new_memory_provider,
        std::make_shared<OneCodeProvider>(runtime_code),
        std::make_shared<OneModuleRepository>(
            compartment_,
            std::make_shared<IntrinsicResolverImpl>(new_intrinsic_module),
            gsl::span<const uint8_t>{
                runtime_code.data(),
                static_cast<gsl::span<const uint8_t>::index_type>(
                    runtime_code.size())}),
        block_header_repo_);
    auto executor =
        std::make_shared<runtime::Executor>(block_header_repo_, env_factory);
    pushHostApi(host_api);
    env_factory->setEnvCleanupCallback([](auto &) { popHostApi(); });
    return std::make_unique<CoreImpl>(
        executor, changes_tracker_, block_header_repo_);
  }

}  // namespace kagome::runtime::wavm
