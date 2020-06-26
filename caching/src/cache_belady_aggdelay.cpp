// STD headers
#include <assert.h>
#include <limits>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

// Custom headers
#include "cache_base.hpp"
#include "cache_belady.hpp"
#include "cache_common.hpp"
#include "utils.hpp"

using namespace caching;
namespace bopt = boost::program_options;

/**
 * Implements a single-tiered BeladyAggregateDelay cache.
 */
class BeladyAggregateDelayCache : public BaseCache {
private:
    utils::TraceAnalyzer* analyzer_; // A TraceAnalyzer instance
    typedef std::list<size_t>::const_iterator IdxIterator;
    std::unordered_map<std::string, IdxIterator>
    flow_ids_to_current_iters_; // Dict mapping flow IDs to their current
                                // positions in corresponding idxs lists.
    /**
     * Given the current state of the and the contending flow,
     * returns the flow ID corresponding to the flow to evict.
     */
    std::string getFlowIdToEvict(const std::unordered_map<std::string, CacheEntry>&
                                 candidates, const std::string& contender) {
        double min_candidate_cost = std::numeric_limits<double>::max();
        std::string flow_id_to_evict;
        for (const auto& pair : candidates) {
            const std::string& candidate = pair.first;

            // First, forward the flow's occurence idx until it corresponds to
            // a packet arrival that is GEQ clk. This value indicates when the
            // very next packet for this flow arrives.
            const auto& indices = analyzer_->getFlowData(candidate).indices();
            IdxIterator iter = flow_ids_to_current_iters_.at(candidate);
            while (iter != indices.end() && (*iter < clk())) { iter++; }
            flow_ids_to_current_iters_[candidate] = iter;

            size_t next_occurence = (iter != indices.end()) ?
                *iter : std::numeric_limits<size_t>::max();

            double current_cost = 0;
            while (iter != indices.end() && (*iter - next_occurence) < kCacheMissLatency) {
                current_cost += kCacheMissLatency - (*iter - next_occurence);
                iter++;
            }

            // Normalize the cost by distance
            if (current_cost > 0) { current_cost /= (next_occurence - clk() + 1); }

            // If this flow has finished, or evicting it
            // incurs the smallest delay-cost, evict it.
            if (current_cost < min_candidate_cost) {
                min_candidate_cost = current_cost;
                flow_id_to_evict = candidate;
            }
        }
        // Forward the contending flow's occurence idx until it corresponds
        // to a packet arrival that is GT clk.
        const auto& indices = analyzer_->getFlowData(contender).indices();
        IdxIterator iter = flow_ids_to_current_iters_.at(contender);
        while (iter != indices.end() && (*iter <= clk())) { iter++; }
        flow_ids_to_current_iters_[contender] = iter;

        size_t next_contender_occurence = (iter != indices.end()) ?
            *iter : std::numeric_limits<size_t>::max();

        double contender_cost = 0;
        while (iter != indices.end() &&
               (*iter - next_contender_occurence) < kCacheMissLatency) {
            contender_cost += kCacheMissLatency - (*iter - next_contender_occurence);
            iter++;
        }

        // Normalize the cost by distance
        if (contender_cost > 0) { contender_cost /= (next_contender_occurence - clk() + 1); }

        // If the cost of rejecting the contender is smaller than evicting
        // any of the candidate flows, do not admit it into the cache.
        if (contender_cost < min_candidate_cost) {
            flow_id_to_evict = contender;
        }
        return flow_id_to_evict;
    }

public:
    BeladyAggregateDelayCache(const size_t miss_latency, const size_t cache_set_associativity,
                              const size_t num_cache_sets, const bool penalize_insertions,
                              const HashType hash_type, int argc, char** argv) : BaseCache(
                              miss_latency, cache_set_associativity, num_cache_sets,
                              penalize_insertions, hash_type) {
        // Command-line arguments
        bopt::options_description options{"BeladyAggregateDelayCache"};
        options.add_options()("trace", bopt::value<std::string>(), "(Input) trace file path");

        // Parse model parameters
        bopt::variables_map variables;
        bopt::store(bopt::command_line_parser(argc, argv).options(
            options).allow_unregistered().run(), variables);

        bopt::notify(variables);
        std::string trace_fp = variables.at("trace").as<std::string>();

        // Initialize the TraceAnalyzer and the cache sets
        analyzer_ = new utils::TraceAnalyzer(trace_fp);
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) {
            cache_sets_.push_back(new BeladyCacheSet<BeladyAggregateDelayCache>(
                kCacheSetAssociativity, *this));
        }
        // Prime the iterators map
        const auto& flowIdsToDataMap = analyzer_->getFlowIdsToDataMap();
        for (const auto& pair : flowIdsToDataMap) {
            flow_ids_to_current_iters_[pair.first] = (
                pair.second.indices().begin());
        }
    }
    virtual ~BeladyAggregateDelayCache() {
        delete(analyzer_);
        analyzer_ = nullptr;
    }

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "BeladyAggregateDelayCache"; }

    // Allow access to getFlowIdToEvict()
    friend class BeladyCacheSet<BeladyAggregateDelayCache>;
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<BeladyAggregateDelayCache>(argc, argv);
}
