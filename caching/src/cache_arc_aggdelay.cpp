// STD headers
#include <assert.h>
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
    // Distribution statistics
    size_t num_packets_ = 0;
    size_t num_windows_ = 0;
    size_t last_packet_idx_ = 0;
    size_t queue_start_idx_ = 0;
    size_t current_aggdelay_ = 0;
    size_t past_cumulative_aggdelay_ = 0;

public:
    /**
     * Records a packet arrival corresponding to this flow.
     */
    void recordPacketArrival(const size_t idx, const size_t z) {
        const size_t tssq = (idx - queue_start_idx_); // Time since start of queue

        // This packet corresponds to a new queue
        if (past_cumulative_aggdelay_ == 0 || tssq >= z) {
            past_cumulative_aggdelay_ += current_aggdelay_;

            num_windows_++;
            current_aggdelay_ = z;
            queue_start_idx_ = idx;
        }
        // Compute the AggregateDelay for the existing queue
        else { current_aggdelay_ += (z - tssq); }

        // Update the last idx and packet count
        last_packet_idx_ = idx;
        num_packets_++;
    }
    /**
     * Returns the average AggregateDelay cost for this flow.
     */
    double getAverageAggregateDelay(const size_t z) const {
        if (z > 100 && num_packets_ <= 2) { return 0; }
        else if (num_windows_ == 1) { return current_aggdelay_; }
        return static_cast<double>(past_cumulative_aggdelay_) / (num_windows_ - 1);
    }

    /**
     * Returns the payoff for this flow.
     */
    double getExpectedPayoff(const size_t clk, const size_t z) const {
        return getAverageAggregateDelay(z) / (clk - last_packet_idx_ + 1);
    }
};

/**
 * Implements an AggregateDelay LRU queue.
 */
class LRUAggregateDelayQueue {
private:
    typedef typename std::unordered_map<std::string, CacheEntry>::iterator EntriesIterator;
    std::unordered_map<std::string, CacheEntry> entries_;

public:
    // Accessors
    size_t size() const { return entries_.size(); }
    std::unordered_map<std::string, CacheEntry>& entries() { return entries_; }
    const std::unordered_map<std::string, CacheEntry>& entries() const { return entries_; }

    /**
     * Membership test.
     */
    bool contains(const std::string& key) const {
        return (entries_.find(key) != entries_.end());
    }

    /**
     * Erase the given queue entry.
     */
    void erase(const EntriesIterator& entries_iter) {
        entries_.erase(entries_iter);
    }

    /**
     * Pop the entry at the front of the queue.
     */
    CacheEntry popRank(const std::unordered_map<std::string, FlowMetadata>&
                       records, const size_t clk, const size_t z) {
        double min_cost = std::numeric_limits<double>::max();
        std::string flow_id_to_evict;

        // Find and replace the candidate with the
        // lowest expected payoff (AggDelay/TTNA).
        for (const auto& candidate : entries_) {
            const std::string flow_id = candidate.first;
            const double candidate_cost = records.at(
                flow_id).getExpectedPayoff(clk, z);

            if (candidate_cost < min_cost) {
                min_cost = candidate_cost;
                flow_id_to_evict = flow_id;
            }
        }

        auto iter = entries_.find(flow_id_to_evict);
        CacheEntry entry = iter->second;
        entries_.erase(iter);
        return entry;
    }

    /**
     * Insert the given entry at the back of the queue.
     */
    void insertBack(const CacheEntry& entry) {
        const std::string& key = entry.key();
        assert(entries_.find(key) == entries_.end());

        entries_[key] = entry;
    }
};

/**
 * Represents a single set (row) in an ARCAggregateDelay-based cache.
 */
template<class T> class ARCAggregateDelayCacheSet : public BaseCacheSet {
protected:
    const T& kCacheImpl; // Reference to the cache implementation
    std::unordered_map<std::string, FlowMetadata> records_; // Dict mapping flow IDs to records

    // {t1, b1, t2, b2}_queue_ are LRU lists (representing T1, B1, T2, B2)
    // as defined in ARC. T1, T2 are ordered lists of CacheEntry instances
    // and (T1 U T2) implements the real cache. Conversely, B1 and B2 are
    // ordered lists of strings, and (B1 U B2) defines the shadow cache.
    LRUAggregateDelayQueue t1_queue_;
    LRUAggregateDelayQueue t2_queue_;
    LRUQueue<std::string> b1_shadow_queue_;
    LRUQueue<std::string> b2_shadow_queue_;

    // Adaptation parameter defined in ARC (target size of T1)
    size_t p_ = 0;

    /**
     * Internal helper method. Returns the total
     * size of the ARC cache (T1 U B1 U T2 U B2).
     */
    size_t getARCCacheSize() const {
        return (t1_queue_.size() + b1_shadow_queue_.size() +
                t2_queue_.size() + b2_shadow_queue_.size());
    }

    /**
     * Internal helper method. Implements ARC's REPLACE algorithm.
     */
    CacheEntry replace(const std::string& key) {
        CacheEntry evicted_entry;
        const size_t clk = kCacheImpl.clk();
        size_t cardinality_t1 = t1_queue_.size();
        bool in_b2 = b2_shadow_queue_.contains(key);
        const size_t z = kCacheImpl.getCacheMissLatency();

        // T1 not empty, AND either: |T1| > p, OR (key in B2, and |T1| = p)
        if (cardinality_t1 != 0 && ((cardinality_t1 > p_) ||
                                    (in_b2 && cardinality_t1 == p_))) {
            evicted_entry = t1_queue_.popRank(records_, clk, z);    // Delete the LRU entry from T1
            occupied_entries_set_.erase(evicted_entry.key());       // and the occupied entries set.
            b1_shadow_queue_.insertBack(evicted_entry.key());       // Make it the MRU entry in B1.
        }
        else {
            evicted_entry = t2_queue_.popRank(records_, clk, z);    // Delete the LRU entry from T2
            occupied_entries_set_.erase(evicted_entry.key());       // and the occupied entries set.
            b2_shadow_queue_.insertBack(evicted_entry.key());       // Make it the MRU entry in B2.
        }
        return evicted_entry;
    }

    /**
     * Internal helper method. Implements an ARC cache write.
     */
    CacheEntry write(const std::string& key,
                     const size_t num_writes) {
        CacheEntry written_entry;

        // Cache parameters
        const size_t clk = kCacheImpl.clk();
        const size_t z = kCacheImpl.getCacheMissLatency();

        // Perform lookups
        auto t2_entries_iter = t2_queue_.entries().find(key);
        auto t1_entries_iter = t1_queue_.entries().find(key);
        auto b1_position_iter = b1_shadow_queue_.positions().find(key);
        auto b2_position_iter = b2_shadow_queue_.positions().find(key);

        const bool in_t2 = (t2_entries_iter != t2_queue_.entries().end());
        const bool in_t1 = (t1_entries_iter != t1_queue_.entries().end());
        const bool in_b1 = (b1_position_iter != b1_shadow_queue_.positions().end());
        const bool in_b2 = (b2_position_iter != b2_shadow_queue_.positions().end());

        // If the flow is currently in the cache (either
        // T1 or T2), move it to the MRU position of T2.
        if (in_t1 || in_t2) {
            // The required entry is in T1
            if (in_t1) {
                written_entry = t1_entries_iter->second;
                t1_queue_.erase(t1_entries_iter);
            }
            // The required entry is in T2
            else {
                written_entry = t2_entries_iter->second;
                t2_queue_.erase(t2_entries_iter);
            }
            // Sanity checks
            assert(contains(key));
            assert(written_entry.isValid());
            assert(written_entry.key() == key);
            assert((in_t1 && !in_t2) || (!in_t1 && in_t2));

            // Move the entry to the MRU position of T2
            t2_queue_.insertBack(written_entry);
        }
        // The entry exists in the shadow cache B1
        else if (in_b1) {
            assert(!contains(key)); // Sanity check
            size_t cardinality_b1 = b1_shadow_queue_.size();
            size_t cardinality_b2 = b2_shadow_queue_.size();

            // Update the adaptation parameter
            size_t delta = (cardinality_b1 >= cardinality_b2) ?
                1 : static_cast<size_t>(round(static_cast<double>(cardinality_b2) /
                                              static_cast<double>(cardinality_b1)));
            p_ = std::min(p_ + delta, kNumEntries);

            // Perform REPLACE, create a new cache entry
            // and insert it into the MRU position of T2.
            replace(key);
            written_entry.update(key);
            written_entry.toggleValid();

            b1_shadow_queue_.erase(b1_position_iter);
            t2_queue_.insertBack(written_entry);
            occupied_entries_set_.insert(key);
        }
        // The entry exists in the shadow cache B2
        else if (in_b2) {
            assert(!contains(key)); // Sanity check
            size_t cardinality_b1 = b1_shadow_queue_.size();
            size_t cardinality_b2 = b2_shadow_queue_.size();

            // Update the adaptation parameter
            size_t delta = (cardinality_b2 >= cardinality_b1) ?
                1 : static_cast<size_t>(round(static_cast<double>(cardinality_b1) /
                                              static_cast<double>(cardinality_b2)));
            p_ = (p_ < delta) ? 0ul : (p_ - delta);

            // Perform REPLACE, create a new cache entry
            // and insert it into the MRU position of T2.
            replace(key);
            written_entry.update(key);
            written_entry.toggleValid();

            b2_shadow_queue_.erase(b2_position_iter);
            t2_queue_.insertBack(written_entry);
            occupied_entries_set_.insert(key);
        }
        // The entry is not in (T1 U T2 U B1 U B2)
        else {
            size_t cardinality_t1 = t1_queue_.size();
            size_t cardinality_l1 = cardinality_t1 + b1_shadow_queue_.size();

            // Case A: L1 = T1 U B1 has exactly c pages
            if (cardinality_l1 == kNumEntries) {
                if (cardinality_t1 < kNumEntries) {

                    // Delete the LRU entry in B1 and perform REPLACE
                    b1_shadow_queue_.popFront();
                    replace(key);
                }
                else {
                    // Delete the LRU entry in T1 and remove it from the cache
                    assert(b1_shadow_queue_.size() == 0);
                    auto evicted_entry = t1_queue_.popRank(records_, z, clk);
                    occupied_entries_set_.erase(evicted_entry.key());
                }
            }
            // Case B: L1 = T1 U B1 has less than c pages
            else {
                size_t arc_cache_size = getARCCacheSize();
                if (arc_cache_size >= kNumEntries) {

                    // If the ARC cache size equals 2c, delete the LRU page in B2. Perform REPLACE.
                    if (arc_cache_size == (2 * kNumEntries)) { b2_shadow_queue_.popFront(); }
                    replace(key);
                }
            }
            // Create a new cache entry and insert
            // it into the MRU position of T1.
            written_entry.update(key);
            written_entry.toggleValid();
            occupied_entries_set_.insert(key);

            if (num_writes > 1) { t2_queue_.insertBack(written_entry); }
            else { t1_queue_.insertBack(written_entry); }
        }
        // Sanity checks
        assert(getARCCacheSize() <= 2 * kNumEntries);
        assert(occupied_entries_set_.size() <= kNumEntries);
        assert(occupied_entries_set_.size() == t1_queue_.size() + t2_queue_.size());
        assert(b1_shadow_queue_.positions().size() == b1_shadow_queue_.entries().size());
        assert(b2_shadow_queue_.positions().size() == b2_shadow_queue_.entries().size());

        return written_entry;
    }

public:
    ARCAggregateDelayCacheSet(const size_t num_entries, const T& cache) :
                              BaseCacheSet(num_entries), kCacheImpl(cache) {}
    virtual ~ARCAggregateDelayCacheSet() {}

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
        return write(key, 1ull);
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
        return write(queue.front().getFlowId(), queue.size());
    }
};

/**
 * Implements a single-tiered ARCAggregateDelay cache.
 */
class ARCAggregateDelayCache : public BaseCache {
public:
    ARCAggregateDelayCache(
             const size_t miss_latency, const size_t cache_set_associativity, const size_t
             num_cache_sets, const bool penalize_insertions, const HashType hash_type, int
             argc, char** argv) : BaseCache(miss_latency, cache_set_associativity,
             num_cache_sets, penalize_insertions, hash_type) {
        SUPPRESS_UNUSED_WARNING(argc);
        SUPPRESS_UNUSED_WARNING(argv);

        // Initialize the cache sets
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) {
            cache_sets_.push_back(new ARCAggregateDelayCacheSet<
                ARCAggregateDelayCache>(kCacheSetAssociativity, *this));
        }
    }
    virtual ~ARCAggregateDelayCache() {}

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "ARCAggregateDelayCache"; }
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<ARCAggregateDelayCache>(argc, argv);
}
