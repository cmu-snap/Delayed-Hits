// STD headers
#include <assert.h>
#include <limits>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

// Boost headers
#include <boost/program_options.hpp>

// Custom headers
#include "cache_base.hpp"
#include "cache_belady.hpp"
#include "cache_common.hpp"
#include "utils.hpp"

using namespace caching;
namespace bopt = boost::program_options;

/**
 * Implements a single-tiered Belady cache.
 */
class BeladyCache : public BaseCache {
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
        size_t max_candidate_occurence = 0;
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

            // If this flow has finished, or the next packet arrival
            // for this flow is the furthest in the future, evict it.
            if (next_occurence > max_candidate_occurence) {
                max_candidate_occurence = next_occurence;
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

        // If the next occurence to the contender is beyond the
        // next occurence to any of the existing cache entries,
        // do not admit the contender into the cache.
        if (next_contender_occurence >= max_candidate_occurence) {
            flow_id_to_evict = contender;
        }
        return flow_id_to_evict;
    }

public:
    BeladyCache(const size_t miss_latency, const size_t cache_set_associativity, const size_t
                num_cache_sets, const bool penalize_insertions, const HashType hash_type, int
                argc, char** argv) : BaseCache(miss_latency, cache_set_associativity,
                num_cache_sets, penalize_insertions, hash_type) {
        // Command-line arguments
        bopt::options_description options{"BeladyCache"};
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
            cache_sets_.push_back(new BeladyCacheSet<BeladyCache>(
                kCacheSetAssociativity, *this));
        }
        // Prime the iterators map
        const auto& flowIdsToDataMap = analyzer_->getFlowIdsToDataMap();
        for (const auto& pair : flowIdsToDataMap) {
            flow_ids_to_current_iters_[pair.first] = (
                pair.second.indices().begin());
        }
    }
    virtual ~BeladyCache() {
        delete(analyzer_);
        analyzer_ = nullptr;
    }

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const override { return "BeladyCache"; }

    // Allow access to getFlowIdToEvict()
    friend class BeladyCacheSet<BeladyCache>;
};

// Run default benchmarks
int main(int argc, char** argv) {
    BaseCache::defaultBenchmark<BeladyCache>(argc, argv);
}
