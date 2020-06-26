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
 * Note: The following implementation is adapted from the
 * simulation code for the LHD cache replacement policy:
 * https://github.com/CMU-CORGI/LHD.
 */

/**
 * Placeholder random number generator from Knuth MMIX.
 */
class Rand {
private:
    size_t state;

public:
    Rand(size_t seed = 0) : state(seed) {}
    inline size_t next() {
        state = 6364136223846793005 * state + 1442695040888963407;
        return state;
    }
};

/**
 * Per-flow metadata.
 */
struct FlowMetadata {
    size_t clk = 0;
    bool explorer = false;
    size_t last_hit_age = 0;
    size_t last_last_hit_age = 0;
};

/**
 * Per-class metadata.
 */
struct ClassMetadata {
    double total_hits = 0;
    double total_evictions = 0;

    std::vector<double> hits;
    std::vector<double> evictions;
    std::vector<double> hit_densities;
};

/**
 * Represents a single set (row) in a LHD-based cache.
 */
template<class T>
class LHDCacheSet : public BaseCacheSet {
private:
    T& kCacheImpl; // Reference to the cache implementation
    std::unordered_map<std::string, FlowMetadata> records_; // Dict mapping flow IDs to records
    std::unordered_map<std::string, CacheEntry> entries_; // Dict mapping flow IDs to CacheEntries

    // Explorer objects
    int64_t explorer_budget_ = 0;
    static constexpr double kExplorerBudgetFraction = 0.01;

public:
    LHDCacheSet(const size_t num_entries, T& cache) :
                BaseCacheSet(num_entries), kCacheImpl(cache) {
        explorer_budget_ = num_entries * kExplorerBudgetFraction;
    }

    /**
     * Simulates a cache write.
     *
     * @param key The key corresponding to this write request.
     * @param packet The packet corresponding to this write request.
     * @return The written CacheEntry instance.
     */
    virtual CacheEntry write(const std::string& key,
                             const utils::Packet& packet) override {
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
                    // Compute the hit density for this flow
                    const std::string& candidate = pair.first;
                    const double candidate_cost = kCacheImpl.getHitDensity(
                        records_.at(candidate));

                    // If this flow incurs the smallest delay cost, evict it
                    if (candidate_cost < min_cost) {
                        min_cost = candidate_cost;
                        flow_id_to_evict = candidate;
                    }
                }
                // Update the flow record before erasure
                kCacheImpl.replaced(records_,flow_id_to_evict, explorer_budget_);

                // Evict the corresponding entry
                auto evicted_entry = entries_.at(flow_id_to_evict);
                occupied_entries_set_.erase(flow_id_to_evict);
                entries_.erase(flow_id_to_evict);
                assert(evicted_entry.isValid());
            }
            // Insert the written entry into the cache
            entries_[key] = written_entry;
            occupied_entries_set_.insert(key);
        }
        kCacheImpl.update(records_, packet, explorer_budget_);

        // Sanity checks
        assert(occupied_entries_set_.size() <= getNumEntries());
        assert(occupied_entries_set_.size() == entries_.size());
        assert(records_.size() == entries_.size());

        return written_entry;
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
        CacheEntry written_entry;

        for (auto iter = queue.begin(); iter != queue.end(); iter++) {
            // For the first packet in the queue, perform a full write
            if (iter == queue.begin()) {
                written_entry = write(iter->getFlowId(), *iter);
                assert(written_entry.isValid()); // Sanity check
                assert(records_.find(iter->getFlowId()) != records_.end());
            }
            // For the remaining packets, simply perform updates
            else { kCacheImpl.update(records_, *iter, explorer_budget_); }
        }
        return written_entry;
    }
};

/**
 * Implements a single-tiered LHD cache.
 */
class LHDCache : public BaseCache {
private:
    static constexpr double kExplorerBudgetFraction = 0.01;
    static constexpr size_t kExploreInverseProbability = 32;

    // Object classification
    static constexpr size_t kNumClassesApp = 1;
    static constexpr size_t kNumClassesHitAge = 16;
    static constexpr size_t kNumClassesTotal = kNumClassesHitAge * kNumClassesApp;

    // Simulation performance
    static constexpr size_t kMaxAge = 20000;
    static constexpr double kEWMADecay = 0.9;
    static constexpr double kAgeCoarseningErrorTolerance = 0.01;
    static constexpr size_t kAccessesPerReconfiguration = (1 << 20);

    // Housekeeping
    Rand rand;
    size_t overflows_ = 0;
    double ewma_num_objects_ = 0;
    size_t next_reconfiguration_ = 0;
    size_t num_reconfigurations_ = 0;
    size_t age_coarsening_shift_ = 10;
    double ewma_num_objects_mass_ = 0;
    std::vector<ClassMetadata> classes_; // List of object classes

public:
    LHDCache(const size_t miss_latency, const size_t cache_set_associativity,
             const size_t num_cache_sets, const bool penalize_insertions,
             const HashType hash_type, int argc, char** argv) : BaseCache(
             miss_latency, cache_set_associativity, num_cache_sets,
             penalize_insertions, hash_type), rand(12345) {
        SUPPRESS_UNUSED_WARNING(argc);
        SUPPRESS_UNUSED_WARNING(argv);

        next_reconfiguration_ = kAccessesPerReconfiguration;
        for (size_t i = 0; i < kNumClassesTotal; i++) {
            classes_.push_back(ClassMetadata());
            auto& cl = classes_.back();
            cl.hits.resize(kMaxAge, 0);
            cl.evictions.resize(kMaxAge, 0);
            cl.hit_densities.resize(kMaxAge, 0);
        }

        // Initialize policy to ~GDSF by default
        for (size_t c = 0; c < kNumClassesTotal; c++) {
            for (size_t a = 0; a < kMaxAge; a++) {
                classes_[c].hit_densities[a] =
                    1. * (c + 1) / (a + 1);
            }
        }
        // Initialize the cache sets
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) {
            cache_sets_.push_back(new LHDCacheSet<LHDCache>(
                kCacheSetAssociativity, *this));
        }
    }
    virtual ~LHDCache() {}

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "LHDCache"; }

    /**
     * Returns something like log(maxAge - age).
     */
    inline size_t hitAgeClass(size_t age) const {
        if (age == 0) { return kNumClassesHitAge - 1; }
        size_t log = 0;
        while (age < kMaxAge && log < kNumClassesHitAge - 1) {
            age <<= 1;
            log += 1;
        }
        return log;
    }

    /**
     * Returns the class ID corresponding to the given flow.
     */
    inline size_t getClassId(const FlowMetadata& data) const {
        return hitAgeClass(data.last_hit_age + data.last_last_hit_age);
    }

    /**
     * Returns the class corresponding to the given flow.
     */
    inline ClassMetadata& getClass(const FlowMetadata& data) {
        return classes_[getClassId(data)];
    }

    /**
     * Returns the age for the given flow.
     */
    inline size_t getAge(const FlowMetadata& data) {
        size_t age = (clk() - data.clk) >> age_coarsening_shift_;
        if (age >= kMaxAge) {
            ++overflows_;
            return kMaxAge - 1;
        }
        else {
            return age;
        }
    }

    /**
     * Returns the hit density corresponding to the given flow.
     */
    inline double getHitDensity(const FlowMetadata& data) {
        const size_t age = getAge(data);
        if (age == kMaxAge - 1) {
            return std::numeric_limits<double>::lowest();
        }
        const ClassMetadata& cl = getClass(data);
        double density = cl.hit_densities[age];
        if (data.explorer) { density += 1.; }
        return density;
    }

    void reconfigure() {
        for (auto& cl : classes_) {
            updateClass(cl);
        }
        adaptAgeCoarsening();
        modelHitDensity();
    }

    void updateClass(ClassMetadata& cl) {
        cl.total_hits = 0;
        cl.total_evictions = 0;

        for (size_t age = 0; age < kMaxAge; age++) {
            cl.hits[age] *= kEWMADecay;
            cl.evictions[age] *= kEWMADecay;

            cl.total_hits += cl.hits[age];
            cl.total_evictions += cl.evictions[age];
        }
    }

    void modelHitDensity() {
        for (size_t c = 0; c < classes_.size(); c++) {
            double total_events = classes_[c].hits[kMaxAge-1] + classes_[c].evictions[kMaxAge-1];
            double total_hits = classes_[c].hits[kMaxAge-1];
            double lifetime_unconditioned = total_events;

            // We use a small trick here to compute expectation
            // in O(N) by accumulating all values at later ages.
            for (size_t a = kMaxAge - 2; a < kMaxAge; a--) {
                total_hits += classes_[c].hits[a];
                total_events += classes_[c].hits[a] + classes_[c].evictions[a];

                lifetime_unconditioned += total_events;
                if (total_events > 1e-5) {
                    classes_[c].hit_densities[a] = total_hits / lifetime_unconditioned;
                } else {
                    classes_[c].hit_densities[a] = 0.;
                }
            }
        }
    }

    /**
     * This happens very rarely!
     *
     * It is simple enough to set the age coarsening if you know roughly
     * how big your objects are. to make LHD run on different traces
     * without needing to configure this, we set the age coarsening
     * automatically near the beginning of the trace.
     */
    void adaptAgeCoarsening() {
        ewma_num_objects_ *= kEWMADecay;
        ewma_num_objects_mass_ *= kEWMADecay;

        ewma_num_objects_ += getNumMemoryEntries();
        ewma_num_objects_mass_ += 1.;

        double num_objects = ewma_num_objects_ / ewma_num_objects_mass_;
        double optimal_age_coarsening = (num_objects /
            (kAgeCoarseningErrorTolerance * kMaxAge));

        // Simplify. Just do this once shortly after the trace starts and
        // again after 25 iterations. It only matters that we are within
        // the right order of magnitude to avoid tons of overflows.
        if (num_reconfigurations_ == 5 || num_reconfigurations_ == 25) {
            uint32_t optimal_age_coarsening_log2 = 1;

            while ((1 << optimal_age_coarsening_log2) < optimal_age_coarsening) {
                optimal_age_coarsening_log2 += 1;
            }

            int32_t delta = optimal_age_coarsening_log2 - age_coarsening_shift_;
            age_coarsening_shift_ = optimal_age_coarsening_log2;

            // Increase weight to delay another shift for a while
            ewma_num_objects_ *= 8;
            ewma_num_objects_mass_ *= 8;

            // Compress or stretch distributions to approximate new scaling regime
            if (delta < 0) {
                // Stretch
                for (auto& cl : classes_) {
                    for (size_t a = kMaxAge >> (-delta); a < kMaxAge - 1; a++) {
                        cl.hits[kMaxAge - 1] += cl.hits[a];
                        cl.evictions[kMaxAge - 1] += cl.evictions[a];
                    }
                    for (size_t a = kMaxAge - 2; a < kMaxAge; a--) {
                        cl.hits[a] = cl.hits[a >> (-delta)] / (1 << (-delta));
                        cl.evictions[a] = cl.evictions[a >> (-delta)] / (1 << (-delta));
                    }
                }
            } else if (delta > 0) {
                // Compress
                for (auto& cl : classes_) {
                    for (size_t a = 0; a < kMaxAge >> delta; a++) {
                        cl.hits[a] = cl.hits[a << delta];
                        cl.evictions[a] = cl.evictions[a << delta];
                        for (int i = 1; i < (1 << delta); i++) {
                            cl.hits[a] += cl.hits[(a << delta) + i];
                            cl.evictions[a] += cl.evictions[(a << delta) + i];
                        }
                    }
                    for (size_t a = (kMaxAge >> delta); a < kMaxAge - 1; a++) {
                        cl.hits[a] = 0;
                        cl.evictions[a] = 0;
                    }
                }
            }
        }
    }
    void update(std::unordered_map<std::string, FlowMetadata>& records,
                const utils::Packet& packet, int64_t& explorer_budget) {
        const std::string& id = packet.getFlowId();
        auto iter = records.find(id);
        bool insert = (iter == records.end());

        if (insert) {
            FlowMetadata& data = records[id];
            data.clk = packet.getArrivalClock();
            data.last_last_hit_age = kMaxAge;
            data.last_hit_age = 0;

            iter = records.find(id);
            assert(iter != records.end());
        }
        else {
            FlowMetadata& data = iter->second;
            ClassMetadata& cl = getClass(data);
            auto age = getAge(data);
            cl.hits[age] += 1;

            if (data.explorer) { explorer_budget += 1; }
            data.last_last_hit_age = data.last_hit_age;
            data.clk = packet.getArrivalClock();
            data.last_hit_age = age;
        }

        // With some probability, some candidates will never be evicted
        // ... but limit how many resources we spend on doing this.
        bool explore = (rand.next() % kExploreInverseProbability) == 0;
        if (explore && explorer_budget > 0 && num_reconfigurations_ < 50) {
            iter->second.explorer = true;
            explorer_budget -= 1;
        } else {
            iter->second.explorer = false;
        }

        if (--next_reconfiguration_ == 0) {
            reconfigure();
            ++num_reconfigurations_;
            next_reconfiguration_ = kAccessesPerReconfiguration;
        }
    }

    void replaced(std::unordered_map<std::string, FlowMetadata>& records,
                  const std::string& id, int64_t& explorer_budget) {
        auto iter = records.find(id);
        assert(iter != records.end());

        // Record stats before removing item
        const FlowMetadata& data = iter->second;
        ClassMetadata& cl = getClass(data);
        auto age = getAge(data);
        cl.evictions[age] += 1;

        if (data.explorer) { explorer_budget += 1; }
        records.erase(iter);
    }
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<LHDCache>(argc, argv);
}
