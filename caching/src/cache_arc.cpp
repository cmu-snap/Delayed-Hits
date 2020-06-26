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
 * Represents a single set (row) in an ARC-based cache.
 */
class ARCCacheSet : public caching::BaseCacheSet {
protected:
    // {t1, b1, t2, b2}_queue_ are LRU lists (representing T1, B1, T2, B2)
    // as defined in ARC. T1, T2 are ordered lists of CacheEntry instances
    // and (T1 U T2) implements the real cache. Conversely, B1 and B2 are
    // ordered lists of strings, and (B1 U B2) defines the shadow cache.
    LRUQueue<CacheEntry> t1_queue_;
    LRUQueue<CacheEntry> t2_queue_;
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
        size_t cardinality_t1 = t1_queue_.size();
        bool in_b2 = b2_shadow_queue_.contains(key);

        // T1 not empty, AND either: |T1| > p, OR (key in B2, and |T1| = p)
        if (cardinality_t1 != 0 && ((cardinality_t1 > p_) ||
                                    (in_b2 && cardinality_t1 == p_))) {
            evicted_entry = t1_queue_.popFront();               // Delete the LRU entry from T1
            occupied_entries_set_.erase(evicted_entry.key());   // and the occupied entries set.
            b1_shadow_queue_.insertBack(evicted_entry.key());   // Make it the MRU entry in B1.
        }
        else {
            evicted_entry = t2_queue_.popFront();               // Delete the LRU entry from T2
            occupied_entries_set_.erase(evicted_entry.key());   // and the occupied entries set.
            b2_shadow_queue_.insertBack(evicted_entry.key());   // Make it the MRU entry in B2.
        }
        return evicted_entry;
    }

public:
    ARCCacheSet(const size_t num_entries) : BaseCacheSet(num_entries) {}
    virtual ~ARCCacheSet() {}

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
        CacheEntry written_entry;
        CacheEntry evicted_entry;

        // Perform lookups
        auto t1_position_iter = t1_queue_.positions().find(key);
        auto t2_position_iter = t2_queue_.positions().find(key);
        auto b1_position_iter = b1_shadow_queue_.positions().find(key);
        auto b2_position_iter = b2_shadow_queue_.positions().find(key);

        const bool in_t1 = (t1_position_iter != t1_queue_.positions().end());
        const bool in_t2 = (t2_position_iter != t2_queue_.positions().end());
        const bool in_b1 = (b1_position_iter != b1_shadow_queue_.positions().end());
        const bool in_b2 = (b2_position_iter != b2_shadow_queue_.positions().end());

        // If the flow is currently in the cache (either
        // T1 or T2), move it to the MRU position of T2.
        if (in_t1 || in_t2) {
            // The required entry is in T1
            if (in_t1) {
                written_entry = *(t1_position_iter->second);
                t1_queue_.erase(t1_position_iter);
            }
            // The required entry is in T2
            else {
                written_entry = *(t2_position_iter->second);
                t2_queue_.erase(t2_position_iter);
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
            evicted_entry = replace(key);
            written_entry.toggleValid();
            written_entry.update(key);

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
            evicted_entry = replace(key);
            written_entry.toggleValid();
            written_entry.update(key);

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
                    evicted_entry = replace(key);
                }
                else {
                    // Delete the LRU entry in T1 and remove it from the cache
                    assert(b1_shadow_queue_.size() == 0);
                    evicted_entry = t1_queue_.popFront();
                    occupied_entries_set_.erase(evicted_entry.key());
                }
            }
            // Case B: L1 = T1 U B1 has less than c pages
            else {
                size_t arc_cache_size = getARCCacheSize();
                if (arc_cache_size >= kNumEntries) {

                    // If the ARC cache size equals 2c, delete the LRU page in B2. Perform REPLACE.
                    if (arc_cache_size == (2 * kNumEntries)) { b2_shadow_queue_.popFront(); }
                    evicted_entry = replace(key);
                }
            }
            // Create a new cache entry and insert
            // it into the MRU position of T1.
            written_entry.update(key);
            written_entry.toggleValid();
            occupied_entries_set_.insert(key);
            t1_queue_.insertBack(written_entry);
        }
        // Sanity checks
        assert(getARCCacheSize() <= 2 * kNumEntries);
        assert(occupied_entries_set_.size() <= kNumEntries);
        assert(t1_queue_.positions().size() == t1_queue_.entries().size());
        assert(t2_queue_.positions().size() == t2_queue_.entries().size());
        assert(occupied_entries_set_.size() == t1_queue_.size() + t2_queue_.size());
        assert(b1_shadow_queue_.positions().size() == b1_shadow_queue_.entries().size());
        assert(b2_shadow_queue_.positions().size() == b2_shadow_queue_.entries().size());

        return written_entry;
    }
};

/**
 * Implements a single-tiered ARC cache.
 */
class ARCCache : public BaseCache {
public:
    ARCCache(const size_t miss_latency, const size_t cache_set_associativity, const size_t
             num_cache_sets, const bool penalize_insertions, const HashType hash_type, int
             argc, char** argv) : BaseCache(miss_latency, cache_set_associativity,
             num_cache_sets, penalize_insertions, hash_type) {
        SUPPRESS_UNUSED_WARNING(argc);
        SUPPRESS_UNUSED_WARNING(argv);

        // Initialize the cache sets
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) {
            cache_sets_.push_back(new ARCCacheSet(kCacheSetAssociativity));
        }
    }
    virtual ~ARCCache() {}

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "ARCCache"; }
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<ARCCache>(argc, argv);
}
