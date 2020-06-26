// STD headers
#include <assert.h>
#include <limits>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

// Custom headers
#include "cache_base.hpp"
#include "cache_common.hpp"
#include "utils.hpp"

using namespace caching;

/**
 * Per-flow metadata.
 */
class FlowMetadata {
private:
    size_t num_windows_ = 0;
    size_t num_packets_ = 0;
    size_t last_packet_idx_ = 0;
    size_t queue_start_idx_ = 0;
    size_t cumulative_aggdelay_ = 0;

public:
    /**
     * Records a packet arrival corresponding to this flow.
     */
    void recordPacketArrival(const size_t idx, const size_t z) {
        const size_t tssq = (idx - queue_start_idx_); // Time since start of queue

        // This packet corresponds to a new queue
        if (num_packets_ == 0 || tssq >= z) {
            num_windows_++;
            queue_start_idx_ = idx;
            cumulative_aggdelay_ += z;
        }
        // Compute the AggregateDelay for the existing queue
        else { cumulative_aggdelay_ += (z - tssq); }

        // Update the last idx and packet count
        last_packet_idx_ = idx;
        num_packets_++;
    }
    /**
     * Returns the payoff for this flow.
     */
    double getExpectedPayoff(const size_t clk) const {
        const double windowed_aggdelay = (
            static_cast<double>(cumulative_aggdelay_) /
            num_windows_);

        return (windowed_aggdelay / (clk - last_packet_idx_ + 1));
    }
};

/**
 * Represents a single set (row) in a LRUAggregateDelay-based cache.
 */
class LRUAggregateDelayCacheSet : public BaseCacheSet {
private:
    const BaseCache& kCacheImpl; // Reference to the cache implementation
    std::unordered_map<std::string, FlowMetadata> records_; // Dict mapping flow IDs to records
    std::unordered_map<std::string, CacheEntry> entries_; // Dict mapping flow IDs to CacheEntries

    /**
     * Internal helper method.
     */
    CacheEntry write(const std::string& key) {
        CacheEntry written_entry;

        // If a corresponding entry exists, update it
        if (contains(key)) {
            written_entry = entries_.at(key);

            // Sanity checks
            assert(contains(key));
            assert(written_entry.isValid());
            assert(written_entry.key() == key);
        }
        // The update was unsuccessful, create a new entry to insert
        else {
            written_entry.update(key);
            written_entry.toggleValid();

            // If required, evict the entry with lowest cost
            if (entries_.size() == getNumEntries()) {
                double min_cost = std::numeric_limits<double>::max();
                std::string flow_id_to_evict;
                for (const auto& pair : entries_) {
                    const std::string& candidate = pair.first;
                    const double candidate_cost = (records_.at(
                        candidate).getExpectedPayoff(kCacheImpl.clk()));

                    // If this flow incurs the smallest delay cost, evict it
                    if (candidate_cost < min_cost) {
                        min_cost = candidate_cost;
                        flow_id_to_evict = candidate;
                    }
                }
                // Evict the corresponding entry
                occupied_entries_set_.erase(flow_id_to_evict);
                entries_.erase(flow_id_to_evict);
            }
            // Update the cache
            entries_[key] = written_entry;
            occupied_entries_set_.insert(key);
        }
        // Sanity checks
        assert(occupied_entries_set_.size() <= getNumEntries());
        assert(occupied_entries_set_.size() == entries_.size());
        return written_entry;
    }

public:
    LRUAggregateDelayCacheSet(const size_t num_entries, const BaseCache& cache) :
                              BaseCacheSet(num_entries), kCacheImpl(cache) {}
    virtual ~LRUAggregateDelayCacheSet() {}

    /**
     * Records arrival of a new packet.
     */
    virtual void recordPacketArrival(const utils::Packet& packet) override {
        records_[packet.getFlowId()].recordPacketArrival(
            packet.getArrivalClock(), kCacheImpl.getCacheMissLatency());
    }

    /**
     * Simulates a cache write.
     *
     * @param key The key corresponding to this write request.
     * @param packet The packet corresponding to this write request.
     * @return The written CacheEntry instance.
     */
    virtual CacheEntry
    write(const std::string& key, const utils::Packet& packet) override {
        SUPPRESS_UNUSED_WARNING(packet);
        return write(key);
    }

    /**
     * Simulates a sequence of cache writes for a particular flow's packet queue.
     * Invoking this method should be functionally equivalent to invoking write()
     * on every queued packet; this simply presents an optimization opportunity
     * for policies which do not distinguish between single/multiple writes.
     *
     * @param queue The queued write requests.
     * @return The written CacheEntry instance.
     */
    virtual CacheEntry
    writeq(const std::list<utils::Packet>& queue) override {
        return write(queue.front().getFlowId());
    }
};

/**
 * Implements a single-tiered LRUAggregateDelay cache.
 */
class LRUAggregateDelayCache : public BaseCache {
private:
    double beta_inv_; // Inverse of the Beta-parameter defined in GD*

public:
    LRUAggregateDelayCache(const size_t miss_latency, const size_t cache_set_associativity,
                           const size_t num_cache_sets, const bool penalize_insertions,
                           const HashType hash_type, int argc, char** argv) : BaseCache(
                           miss_latency, cache_set_associativity, num_cache_sets,
                           penalize_insertions, hash_type) {
        SUPPRESS_UNUSED_WARNING(argc);
        SUPPRESS_UNUSED_WARNING(argv);

        // Initialize the cache sets
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) {
            cache_sets_.push_back(new LRUAggregateDelayCacheSet(
                kCacheSetAssociativity, *this));
        }
    }
    virtual ~LRUAggregateDelayCache() {}

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "LRUAggregateDelayCache"; }
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<LRUAggregateDelayCache>(argc, argv);
}
