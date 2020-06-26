// STD headers
#include <assert.h>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Boost headers
#include <boost/bimap.hpp>
#include <boost/program_options.hpp>

// Cereal headers
#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>

// Gurobi headers
#include <gurobi_c++.h>

// Custom headers
#include "flow_dict.hpp"
#include "simulator.hpp"
#include "utils.hpp"

// Typedefs
namespace bopt = boost::program_options;
typedef std::tuple<std::string, bool, std::string, std::string,
double> Edge;  // Format: (flow_id, in_cache, src, dst, cost)

// Constant hyperparameters
constexpr unsigned int kNumThreads = 24;

/**
 * Implements a Multi-Commodity Min-Cost Flow-based
 * solver for the Delayed Hits caching problem.
 */
class MCMCFSolver {
private:
    typedef std::vector<size_t> Indices;

    // Trace and cache parameters
    const uint z_;
    const uint c_;
    const bool is_forced_;
    const size_t trace_len_;
    const std::string trace_file_path_;
    const std::string model_file_prefix_;
    const std::string packets_file_path_;
    const std::vector<std::string> trace_;

    // Housekeeping
    std::vector<double> solution_;
    std::unordered_set<size_t> cache_node_idxs_;
    std::unordered_map<std::string, Indices> indices_;
    boost::bimap<size_t, std::string> idx_to_nodename_;
    std::unordered_map<size_t, double> idx_to_miss_cost_;
    std::unordered_map<std::string, std::string> sink_nodes_;
    std::unordered_map<std::string, Indices> cache_interval_start_idxs_;

    // "Split nodes" are used to enforce forced admission, if required.
    // We detect nodes where the forced admission constraint *might* be
    // violated (i.e. an object doesn't remain in the cache for atleast
    // one timestep), and add manually a constraint that enforces this.
    std::unordered_set<size_t> split_node_idxs_;

    // Internal helper methods
    double getMissCostStartingAt(const size_t idx) const;
    const std::string& cchNodeForMemAt(const size_t idx) const;
    size_t getNextMemNodeIdx(const size_t idx, const std::string& flow_id,
                             std::unordered_map<std::string,
                             size_t>& next_idxs_map) const;

    // Thread-safe, concurrent helper methods
    void populateOutOfCchEdgesConcurrent(
        const std::list<std::string>& flow_ids,
        std::list<Edge>& edges, std::unordered_set<size_t>& cache_node_idxs);

    void populateCchCapacityConstraintsConcurrent(
        const std::pair<size_t, size_t> range,
        const belatedly::FlowDict& flows, std::list<GRBLinExpr>& exprs) const;

    // Setup and checkpointing
    void setup();
    bool loadOptimizedModelSolution(belatedly::FlowDict& flows);
    void saveOptimizedModelSolution(const belatedly::FlowDict& flows) const;

    // Model creation
    void populateOutOfCchEdges(std::list<Edge>& all_edges);
    void createFlowVariables(GRBModel& model, belatedly::FlowDict&
                             flows, const std::list<Edge>& edges) const;

    void populateCchCapacityConstraints(
        GRBModel& model, const belatedly::FlowDict& flows) const;

    void populateSplitNodeFlowConstraints(
        GRBModel& model, const belatedly::FlowDict& flows) const;

    void populateFlowConservationConstraints(
        GRBModel& model, const belatedly::FlowDict& flows) const;

    void addMissCostStartingAt(const size_t start_idx,
                               const double coefficient,
                               std::vector<utils::Packet>& packets) const;

    // Cost computation and validation
    double getCostLowerBound(const belatedly::FlowDict& flows,
                             const std::list<Edge>& edges) const;

    double getCostUpperBoundAndValidateSolution(
        const belatedly::FlowDict& flows, const std::list<Edge>& edges) const;

public:
    MCMCFSolver(const std::string& trace_fp, const std::string& model_fp,
                const std::string& packets_fp, const bool forced, const
                uint z, const uint c, const std::vector<std::string>& trace) :
                z_(z), c_(c), is_forced_(forced), trace_len_(trace.size()),
                trace_file_path_(trace_fp), model_file_prefix_(model_fp),
                packets_file_path_(packets_fp), trace_(trace) {}

    void solve();
};

/**
 * Given an index, returns the corresponding cache node.
 *
 * Thread-safe.
 */
const std::string& MCMCFSolver::cchNodeForMemAt(const size_t idx) const {
    return idx_to_nodename_.left.at(idx);
}

/**
 * Returns the cost of a cache miss starting at idx.
 *
 * Thread-safe.
 */
double MCMCFSolver::getMissCostStartingAt(const size_t idx) const {
    const std::string& flow_id = trace_[idx];
    double miss_cost = z_;

    // Iterate over the next z timesteps, and add the
    // latency cost corresponding to each subsequent
    // packet of the same flow.
    for (size_t offset = 1; offset < z_; offset++) {
        if (idx + offset >= trace_len_) { break; }
        else if (trace_[idx + offset] == flow_id) {
            miss_cost += (z_ - offset);
        }
    }
    return miss_cost;
}

/**
 * Returns the index of the first mem node corresponding to the given
 * flow ID occuring after idx. If no reference to this flow is found
 * in (idx, trace_len_), returns maxval.
 *
 * The state for each flow is cached in next_idxs_map. This dict must
 * be cleared before starting a new operation (that is, when indices
 * are no longer expected to continue increasing monotonically).
 *
 * Thread-safe.
 */
size_t MCMCFSolver::getNextMemNodeIdx(
    const size_t idx, const std::string& flow_id,
    std::unordered_map<std::string, size_t>& next_idxs_map) const {
    const std::vector<size_t>& indices = indices_.at(flow_id);
    size_t indices_len = indices.size();

    while ((next_idxs_map[flow_id] < indices_len) &&
           (indices[next_idxs_map[flow_id]] <= idx)) {
        next_idxs_map[flow_id]++;
    }
    // If the next index is beyond the idxs list for
    // this flow, return the corresponding sink node.
    size_t next_idx = next_idxs_map[flow_id];
    if (next_idx == indices_len) {
        return std::numeric_limits<size_t>::max();
    }
    else {
        size_t next_mem_idx = indices[next_idx];
        assert(trace_[next_mem_idx] == flow_id);
        return next_mem_idx;
    }
}

/**
 * Attempts to load the optimal solution and the flow mappings
 * for the given model. Returns true if successful, else false.
 */
bool MCMCFSolver::loadOptimizedModelSolution(belatedly::FlowDict& flows) {
    if (model_file_prefix_.empty()) { return false; }
    std::ifstream solution_ifs(model_file_prefix_ + ".sol");
    const std::string flows_fp = model_file_prefix_ + ".flows";

    // Either of the required model files is missing, indicating failure
    if (!solution_ifs.good() || !std::ifstream(flows_fp).good()) {
        return false;
    }
    // Load the solutions vector
    {
        cereal::BinaryInputArchive ar(solution_ifs);
        ar(solution_);
    }
    // Load the flow mappings
    flows.loadFlowMappings(flows_fp);

    // Sanity check: The number of decision variables should
    // be equal to the number of entries in the flows map.
    assert(solution_.size() == flows.numVariables());
    return true;
}

/**
 * Saves the optimal solution and flow mappings to disk.
 */
void MCMCFSolver::saveOptimizedModelSolution(
    const belatedly::FlowDict& flows) const {

    if (model_file_prefix_.empty()) { return; }
    std::ofstream solution_ofs(model_file_prefix_ + ".sol");
    const std::string flows_fp = model_file_prefix_ + ".flows";

    // Save the solutions vector
    cereal::BinaryOutputArchive oarchive(solution_ofs);
    oarchive(solution_);

    // Save the flow mappings
    flows.saveFlowMappings(flows_fp);
}

/**
 * Populate the indices map and internal data structures.
 */
void MCMCFSolver::setup() {
    size_t num_nonempty_packets = 0;
    for (size_t idx = 0; idx < trace_len_; idx++) {
        const std::string& flow_id = trace_[idx];

        // If the packet is non-empty, append the idx to
        // the corresponding flow's idxs. Also, populate
        // the miss-costs map.
        if (!flow_id.empty()) {
            num_nonempty_packets++;
            indices_[flow_id].push_back(idx);
            idx_to_miss_cost_[idx] = getMissCostStartingAt(idx);
        }
        // Found a possible violation of forced admission; track this as a split node
        if (is_forced_ && !flow_id.empty() && (idx > 0) && (idx < trace_len_ - z_) &&
            (flow_id == trace_[idx - 1]) && (flow_id == trace_[idx + z_])) {
            split_node_idxs_.insert(idx);
        }
        // Populate the nodenames map
        idx_to_nodename_.insert(boost::bimap<size_t, std::string>::value_type(
            idx, "cch_t" + std::to_string(idx + z_ - 1) + "_" + flow_id));
    }
    // Populate the sink nodes map and
    // prime the cache intervals map.
    for (const auto& iter : indices_) {
        const std::string& flow_id = iter.first;
        sink_nodes_[flow_id] = ("x_" + flow_id);
        cache_interval_start_idxs_[flow_id];
    }
    // Debug
    std::cout << "Finished setup with " << trace_len_
              << " packets (" << num_nonempty_packets
              << " nonempty), " << indices_.size()
              << " flows." << std::endl;
}

/**
 * Helper method. Populates the out-of-cache edges (mem->cch/admittance,
 * and cch->mem/eviction) for the given flow IDs in a thread-safe manner.
 *
 * Important note: This method relies on the fact that separate threads
 * operate on disjoint sets of flow IDs (and only ever access different
 * STL containers concurrently), precluding the need for mutexes.
 *
 * Thread-safe.
 */
void MCMCFSolver::populateOutOfCchEdgesConcurrent(
    const std::list<std::string>& flow_ids, std::list<Edge>& edges,
    std::unordered_set<size_t>& local_cache_node_idxs) {

    // Iterate over the given flow IDs to populate the edges list
    std::unordered_map<std::string, size_t> next_idxs_map;
    for (const std::string& flow_id : flow_ids) {
        std::string evicted_reference = std::string();
        bool has_flow_started = false;

        // Iterate over each timestep and fetch the next mem reference
        for (size_t idx = 0; idx < trace_len_; idx++) {
            const std::string& trace_flow_id = trace_[idx];
            bool is_same_flow = (!trace_flow_id.empty() &&
                                 (trace_flow_id == flow_id));
            if (has_flow_started) {

                // Fetch the next mem reference corresponding to this flow
                const size_t next_reference_idx = getNextMemNodeIdx(
                    idx + z_ - 1, flow_id, next_idxs_map);

                const bool is_dst_sink_node = (next_reference_idx ==
                                               std::numeric_limits<size_t>::max());

                // Fetch the dst node for this out-of-cache edge
                const std::string& next_reference = is_dst_sink_node ?
                                                    sink_nodes_.at(flow_id) :
                                                    cchNodeForMemAt(next_reference_idx);
                if (is_same_flow) {
                    // In optional admission, unconditionally create an
                    // outedge from this node to the next mem reference.
                    if (!is_forced_) {
                        evicted_reference = next_reference;
                        edges.push_back(std::make_tuple(
                            flow_id, false, cchNodeForMemAt(idx), next_reference,
                            (is_dst_sink_node ? 0.0 : idx_to_miss_cost_.at(next_reference_idx)))
                        );
                    }
                    // The following node in the trace (mem_t{idx + z}) maps to
                    // this flow, but the previous node in the caching sequence
                    // (cch_t{idx + z - 1}) also maps to it. Thus, this is the
                    // final opportunity to reach mem_t{idx + z}, and we create
                    // an edge to it.
                    else if (split_node_idxs_.find(idx) != split_node_idxs_.end()) {
                        assert(!is_dst_sink_node);
                        edges.push_back(std::make_tuple(
                            flow_id, false, cchNodeForMemAt(idx),
                            next_reference, idx_to_miss_cost_.at(next_reference_idx))
                        );
                    }
                    // Discard the next mem reference for this flow
                    else {
                        evicted_reference.clear();
                    }

                    // Mark this idx as the start of a new interval for this flow
                    local_cache_node_idxs.insert(idx);
                    cache_interval_start_idxs_.at(flow_id).push_back(idx);
                }
                // If we did not already created an eviction edge to the
                // next mem reference for this flow, create one to it.
                else if (evicted_reference != next_reference) {
                    evicted_reference = next_reference;
                    edges.push_back(std::make_tuple(
                        flow_id, false, cchNodeForMemAt(idx), next_reference,
                        (is_dst_sink_node ? 0.0 : idx_to_miss_cost_.at(next_reference_idx)))
                    );

                    // Mark this idx as the start of a new interval for this flow
                    local_cache_node_idxs.insert(idx);
                    cache_interval_start_idxs_.at(flow_id).push_back(idx);
                }
            }
            // The flow has not yet started and the packet at this timestep
            // belongs to this flow, indicating that this is the first ever
            // request to it.
            else if (is_same_flow) {
                has_flow_started = true;

                // In optional admission, unconditionally create an
                // outedge from this node to the next mem reference.
                if (!is_forced_) {
                    // Fetch the next mem reference corresponding to this flow
                    const size_t next_reference_idx = getNextMemNodeIdx(
                        idx + z_ - 1, flow_id, next_idxs_map);

                    const bool is_dst_sink_node = (next_reference_idx ==
                                                   std::numeric_limits<size_t>::max());

                    // Fetch the dst node for this out-of-cache edge
                    const std::string& next_reference = is_dst_sink_node ?
                                                        sink_nodes_.at(flow_id) :
                                                        cchNodeForMemAt(next_reference_idx);
                    evicted_reference = next_reference;
                    edges.push_back(std::make_tuple(
                        flow_id, false, cchNodeForMemAt(idx), next_reference,
                        (is_dst_sink_node ? 0.0 : idx_to_miss_cost_.at(next_reference_idx)))
                    );
                }
                // Mark this idx as the start of a new interval for this flow
                local_cache_node_idxs.insert(idx);
                cache_interval_start_idxs_.at(flow_id).push_back(idx);
            }
        }
    }
}

/**
 * Populates the out-of-cache (eviction and admittance) edges.
 */
void MCMCFSolver::populateOutOfCchEdges(std::list<Edge>& all_edges) {
    size_t num_out_of_cch_edges = 0;
    size_t num_total_flows = indices_.size();
    size_t num_flows_per_thread = int(ceil(num_total_flows /
                                      static_cast<double>(kNumThreads)));

    // Temporary containers for each thread
    size_t num_flows_allotted = 0;
    std::array<std::list<Edge>, kNumThreads> edges;
    std::array<std::list<std::string>, kNumThreads> flow_ids;
    std::array<std::unordered_set<size_t>, kNumThreads> local_cache_node_idxs;

    // Partition the entire set of flow IDs into kNumThreads disjoint sets
    auto iter = indices_.begin();
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        for (size_t i = 0; (i < num_flows_per_thread) &&
             (iter != indices_.end()); i++, iter++) {
            num_flows_allotted++;
            flow_ids[t_idx].push_back(iter->first);
        }
    }
    // Sanity check
    assert(num_flows_allotted == num_total_flows);

    // Launch kNumThreads on the corresponding sets of flow IDs
    std::thread workers[kNumThreads];
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        workers[t_idx] = std::thread(&MCMCFSolver::populateOutOfCchEdgesConcurrent, this,
                                     std::ref(flow_ids[t_idx]), std::ref(edges[t_idx]),
                                     std::ref(local_cache_node_idxs[t_idx]));
    }
    // Wait for all the worker threads to finish execution
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        workers[t_idx].join();
    }
    // Then, combine their results
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        num_out_of_cch_edges += edges[t_idx].size();
        all_edges.splice(all_edges.end(), edges[t_idx]);
        cache_node_idxs_.insert(local_cache_node_idxs[t_idx].begin(),
                                local_cache_node_idxs[t_idx].end());
    }
    // Next, sort the cache intervals for each flow in-place
    for (auto& iter : cache_interval_start_idxs_) {
        std::vector<size_t>& start_idxs = iter.second;
        std::sort(start_idxs.begin(), start_idxs.end());
    }
    // Finally, for the last packet in the trace, unconditionally
    // create an outedge to the corresponding flow's sink node.
    size_t idx = (trace_len_ - 1);
    const std::string& flow_id = trace_[idx];
    if (is_forced_ && !flow_id.empty()) {
        num_out_of_cch_edges++;
        all_edges.push_back(std::make_tuple(flow_id, false,
                                            cchNodeForMemAt(idx),
                                            sink_nodes_.at(flow_id), 0.0));
    }
    // Debug
    std::cout << "Finished populating " << num_out_of_cch_edges
              << " out-of-cache edges concurrently, using "
              << kNumThreads << " threads." << std::endl;
}

/**
 * Given a list of edges, populates the flow tupledict.
 */
void MCMCFSolver::
createFlowVariables(GRBModel& model, belatedly::FlowDict& flows,
                    const std::list<Edge>& edges) const {
    size_t num_total_cache_intervals = 0;

    // First, create variables representing mem->cch and cch->mem edges
    for (const Edge& edge : edges) {
        flows.addVariable(std::get<0>(edge),
                          std::get<1>(edge),
                          std::get<2>(edge),
                          std::get<3>(edge),
                          std::get<4>(edge));
    }
    // Next, for every flow, create decision variables
    // corresponding to each possible caching interval.
    for (const auto& iter : cache_interval_start_idxs_) {
        const std::string& flow_id = iter.first;
        const std::vector<size_t>& start_idxs = iter.second;
        const size_t num_intervals = (start_idxs.size() - 1);

        // Iterate over each interval
        for (size_t i = 0; i < num_intervals; i++) {
            size_t idx = start_idxs[i];
            size_t next_idx = start_idxs[i + 1];

            // Sanity check: Both indices should be in the nodes set
            assert((cache_node_idxs_.find(idx) != cache_node_idxs_.end()) &&
                   (cache_node_idxs_.find(next_idx) != cache_node_idxs_.end()));

            // Create a decision variable corresponding to cch_{idx}->cch_{next_idx}
            num_total_cache_intervals++;
            flows.addVariable(flow_id, true,
                              cchNodeForMemAt(idx),
                              cchNodeForMemAt(next_idx), 0.0);
        }
    }
    // Finally, update the model
    model.update();

    // Debug
    std::cout << "Finished creating " << flows.numVariables() << " flow variables (for "
              << edges.size() << " edges, plus " << num_total_cache_intervals
              << " caching intervals)." << std::endl;
}

/**
 * Helper method. Populates the cache capacity constraints for
 * the given range of trace indices in a thread-safe manner.
 *
 * Thread-safe.
 */
void MCMCFSolver::populateCchCapacityConstraintsConcurrent(
    const std::pair<size_t, size_t> range, const belatedly::
    FlowDict& flows, std::list<GRBLinExpr>& exprs) const {
    std::unordered_map<std::string, GRBVar> decision_variables;
    std::unordered_map<std::string, size_t> current_idxs;

    // Starting index is out-of-bounds, do nothing
    if (range.first >= trace_len_) { return; }

    // First, for each flow ID, forward its current iterator idx
    // to point to the appropriate location in its indices list.
    for (const auto& iter : cache_interval_start_idxs_) {
        const std::vector<size_t>& intervals = iter.second;
        const std::string& flow_id = iter.first;

        size_t i = 0;
        while (i < (intervals.size() - 1) &&
               range.first > intervals[i + 1]) { i++; }

        // Update the current iterator idx
        current_idxs[flow_id] = i;
    }
    // Next, generate a linear-expression corresponding to the sum of
    // all flow decision variables at every timestep, and impose that
    // it is LEQ the cache size as a constraint.
    for (size_t idx = range.first; idx < range.second; idx++) {
        if (cache_node_idxs_.find(idx) ==
            cache_node_idxs_.end()) { continue; }

        bool is_split_node = (split_node_idxs_.find(idx) !=
                              split_node_idxs_.end());
        GRBLinExpr node_capacity_expr;
        bool is_expr_changed = false;

        // Update the current decision variable for each
        // flow and add it to the LHS of the constraints.
        for (const auto& iter : cache_interval_start_idxs_) {
            const std::string& flow_id = iter.first;
            const std::vector<size_t>& intervals = iter.second;
            bool is_split_node_for_flow = (is_split_node &&
                                           (trace_[idx] == flow_id));

            // If this trace index either precedes the first decision
            // variable, or is either at or beyond the last decision
            // variable for this flow, do nothing.
            size_t i = current_idxs[flow_id];
            if (idx < intervals[0] || idx >= intervals.back()) {
                continue;
            }
            // Reached the first interval or the end
            // of the current interval for this flow.
            else if ((decision_variables.find(flow_id) ==
                      decision_variables.end()) ||
                     (idx == intervals[i + 1])) {

                // If this is the end of the current
                // interval, update the idx iterator.
                if (idx == intervals[i + 1]) {
                    current_idxs[flow_id] = ++i;
                }
                // Fetch the current interval
                size_t start_idx = intervals[i];
                size_t end_idx = intervals[i + 1];

                // This is a regular node
                GRBVar decision_variable = (
                    flows.getVariable(flow_id, true,
                                      cchNodeForMemAt(start_idx),
                                      cchNodeForMemAt(end_idx)));

                // Update the decision variable for this flow
                decision_variables[flow_id] = decision_variable;
                node_capacity_expr += decision_variable;
                is_expr_changed = true;
            }
            // Else, add the current decision variable to the expression
            else {
                GRBVar decision_variable = decision_variables.at(flow_id);
                node_capacity_expr += decision_variable;
                assert(!is_split_node_for_flow);
            }
        }
        // Finally, add the expression as a cache constraints
        if (is_expr_changed) { exprs.push_back(node_capacity_expr); }
    }
}

/**
 * Populates the cache capacity constraints for the given flow variables.
 */
void MCMCFSolver::populateCchCapacityConstraints(
    GRBModel& model, const belatedly::FlowDict& flows) const {
    std::array<std::list<GRBLinExpr>, kNumThreads> exprs;
    std::thread workers[kNumThreads];
    size_t num_constraints = 0;

    // Partition the trace into kNumThreads disjoint sets
    size_t start_idx = 0;
    size_t idxs_per_thread = int(ceil(trace_len_ /
                                 static_cast<double>(kNumThreads)));

    // Launch kNumThreads on the corresponding range of indices
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        size_t end_idx = std::min(trace_len_,
                                  start_idx + idxs_per_thread);

        workers[t_idx] = std::thread(
            &MCMCFSolver::populateCchCapacityConstraintsConcurrent, this,
            std::make_pair(start_idx, end_idx), std::ref(flows),
            std::ref(exprs[t_idx]));

        // Update the starting idx
        start_idx = end_idx;
    }
    // Wait for all the worker threads to finish execution
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        workers[t_idx].join();
    }
    // Populate the model constraints
    for (size_t t_idx = 0; t_idx < kNumThreads; t_idx++) {
        for (auto& expr : exprs[t_idx]) {
            num_constraints++;
            model.addConstr(expr, GRB_LESS_EQUAL, c_, "");
        }
        exprs[t_idx].clear();
    }
    // Debug
    std::cout << "Finished adding " << num_constraints << " arc-capacity"
              << " constraints for cch->cch edges concurrently, using "
              << kNumThreads << " threads." << std::endl;
}

/**
 * Populates flow constraints for split nodes. These ensure that once
 * flow enters the cache, it remains there for at least one timestep.
 */
void MCMCFSolver::populateSplitNodeFlowConstraints(
    GRBModel& model, const belatedly::FlowDict& flows) const {
    size_t num_split_node_constraints = 0;

    for (const size_t idx : split_node_idxs_) {
        const std::string& flow_id = trace_[idx];
        num_split_node_constraints++;

        // Fetch the decision variable corresponding
        // to the cch_{idx}->next_mem_node edge.
        const GRBVar outflow = flows.getVariable(
            flow_id, false, cchNodeForMemAt(idx),
            cchNodeForMemAt(idx + z_));

        // Fetch the expression corresponding
        // to the mem_{idx}->cch_{idx} edge.
        const GRBLinExpr inflow = (
            flows.sumAcrossSrcNodes(flow_id, false,
                                    cchNodeForMemAt(idx)));

        // Ensure that the outflow is LEQ (1 - inflow)
        model.addConstr(outflow + inflow, GRB_LESS_EQUAL, 1, "");
    }
    // Debug
    std::cout << "Finished populating " << num_split_node_constraints
              << " flow constraints for split nodes." << std::endl;
}

/**
 * Populates the flow conservation constraints for all nodes.
 */
void MCMCFSolver::populateFlowConservationConstraints(
    GRBModel& model, const belatedly::FlowDict& flows) const {

    // Flow-conservation constraints for sink nodes
    size_t num_sink_node_constraints = 0;
    for (const auto& iter : indices_) {
        num_sink_node_constraints++;
        const std::string& flow_id = iter.first;
        const std::string& node = sink_nodes_.at(flow_id);
        model.addConstr(flows.sumAcrossSrcNodes(flow_id, node), GRB_EQUAL, 1, "");
    }
    // Debug
    std::cout << "Finished adding " << num_sink_node_constraints
              << " flow-conservation constraints for sink nodes."
              << std::endl;

    // Flow-conservation constraints for cache nodes
    size_t num_cache_node_constraints = 0;
    for (const auto& iter : cache_interval_start_idxs_) {
        const std::string& flow_id = iter.first;
        const std::vector<size_t>& start_idxs = iter.second;

        for (size_t idx : start_idxs) {
            num_cache_node_constraints++;
            const std::string& node = cchNodeForMemAt(idx);
            assert(cache_node_idxs_.find(idx) != cache_node_idxs_.end());

            // This is the source node for this flow
            if (idx == start_idxs.front()) {
                model.addConstr(flows.sumAcrossDstNodes(flow_id, node), GRB_EQUAL, 1, "");
            }
            // Else, this is a regular node
            else {
                model.addConstr(flows.sumAcrossSrcNodes(flow_id, node), GRB_EQUAL,
                                flows.sumAcrossDstNodes(flow_id, node), "");
            }
        }
    }
    // Debug
    std::cout << "Finished adding " << num_cache_node_constraints
              << " flow-conservation constraints for cache nodes."
              << std::endl;
}

/**
 * Helper method. Given a start idx and cost coefficient (flow fraction),
 * updates latencies of subsequent same-flow packets that occur within z
 * timesteps of start_idx.
 */
void MCMCFSolver::
addMissCostStartingAt(const size_t start_idx, const double coefficient,
                      std::vector<utils::Packet>& packets) const {
    if (utils::DoubleApproxEqual(coefficient, 0.)) { return; }

    // Iterate over the next z timesteps, and add the weighted
    // latency cost to each subsequent packet of the same flow.
    const std::string& flow_id = trace_[start_idx];
    for (size_t offset = 0; offset < z_; offset++) {
        const size_t idx = start_idx + offset;

        if (idx >= trace_len_) { break; }
        else if (trace_[idx] == flow_id) {
            packets[idx].addLatency((z_ - offset) * coefficient);
        }
    }
}

/**
 * Returns a (tight) lower-bound on the cost of the LP solution.
 */
double MCMCFSolver::getCostLowerBound(const belatedly::FlowDict& flows,
                                      const std::list<Edge>& edges) const {
    // Create packets corresponding to each timestep
    std::vector<utils::Packet> processed_packets;
    for (const std::string& flow_id : trace_) {
        processed_packets.push_back(utils::Packet(flow_id));
    }
    // Update the packet latency corresponding
    // to the first occurence of each flow.
    for (const auto& elem : indices_) {
        const size_t src_idx = elem.second.front();
        addMissCostStartingAt(src_idx, 1., processed_packets);
    }
    // Next, add the latency cost of flow
    // routed through out-of-cch edges.
    for (const auto& edge : edges) {
        const bool in_cache = std::get<1>(edge);
        const std::string& src = std::get<2>(edge);
        const std::string& dst = std::get<3>(edge);
        const std::string& flow_id = std::get<0>(edge);

        // This is an out-of-cch edge and dst is not a sink node
        if (!in_cache && dst != sink_nodes_.at(flow_id)) {
            double flow_fraction = solution_[
                flows.getVariableIdx(flow_id, in_cache, src, dst)];

            size_t dst_idx = idx_to_nodename_.right.at(dst);
            addMissCostStartingAt(dst_idx, flow_fraction,
                                  processed_packets);
        }
    }
    // Compute the total latency for this solution
    double total_latency = 0;
    for (const utils::Packet& packet : processed_packets) {
        const std::string& flow_id = packet.getFlowId();
        const double latency = packet.getTotalLatency();
        if (!flow_id.empty()) {

            // Sanity check: For non-empty packets, latency should be in [0, z]
            assert(utils::DoubleApproxGreaterThanOrEqual(latency, 0.) &&
                   utils::DoubleApproxGreaterThanOrEqual(z_, latency));

            total_latency += latency;
        }
        // Else, for empty packets, do nothing
        else { assert(latency == 0.); }
    }
    return total_latency;
}

/**
 * Validates the computed LP solution.
 */
double MCMCFSolver::getCostUpperBoundAndValidateSolution(
    const belatedly::FlowDict& flows, const std::list<Edge>& edges) const {
    std::vector<std::tuple<size_t, std::string, double>> evictions;
    std::vector<std::tuple<size_t, std::string, double>> inductions;

    // Parse the eviction schedule from the computed LP solution
    bool is_solution_integral = true;
    size_t num_fractional_vars = 0;
    for (const auto& edge : edges) {
        const bool in_cache = std::get<1>(edge);
        const std::string& flow_id = std::get<0>(edge);

        // If this is an out-of-cch edge, fetch the
        // decision variable corresponding to it.
        if (!in_cache) {
            const size_t src_idx = idx_to_nodename_.right.at(
                    std::get<2>(edge)) + z_ - 1;

            const size_t dst_idx =
                (std::get<3>(edge) == sink_nodes_.at(flow_id)) ?
                std::numeric_limits<size_t>::max() :
                idx_to_nodename_.right.at(std::get<3>(edge)) + z_ - 1;

            double value = solution_[flows.getVariableIdx(
                flow_id, in_cache, std::get<2>(edge),
                std::get<3>(edge))];

            // The solution has non-integral variables
            if (!utils::DoubleApproxEqual(value, 1.) &&
                !utils::DoubleApproxEqual(value, 0.)) {
                is_solution_integral = false;
                num_fractional_vars++;
            }
            // Perform an eviction at this node
            if (value > 0.) {
                evictions.push_back(std::make_tuple(src_idx, flow_id, value));
                if (dst_idx != std::numeric_limits<size_t>::max()) {
                    inductions.push_back(std::make_tuple(dst_idx, flow_id, value));
                }
            }
        }
    }
    // Populate the inductions list for source nodes
    for (const auto& item : indices_) {
        const std::string& flow_id = item.first;
        const size_t src_idx = item.second.front() + z_ - 1;
        inductions.push_back(std::make_tuple(src_idx, flow_id, 1.));
    }
    // Sort the eviction schedule
    std::sort(evictions.begin(), evictions.end(), [](
        const std::tuple<size_t, std::string, double>& a,
            const std::tuple<size_t, std::string, double>& b) {
                return (std::get<0>(a) < std::get<0>(b)); });

    // Sort the induction schedule
    std::sort(inductions.begin(), inductions.end(), [](
        const std::tuple<size_t, std::string, double>& a,
            const std::tuple<size_t, std::string, double>& b) {
                return (std::get<0>(a) < std::get<0>(b)); });
    // Debug
    std::cout << "Solution is "
              << (is_solution_integral ? "INTEGRAL." : "FRACTIONAL.") << std::endl;

    // For fractional solutions, output the fraction of non-integer decision variables
    if (!is_solution_integral) {
        double percentage_fractional = (num_fractional_vars * 100.) / flows.numVariables();
        std::cout << "Warning: Solution has " << num_fractional_vars << " fractional "
                  << "decision variables (" << flows.numVariables() << " in total => "
                  << std::fixed << std::setprecision(2) << percentage_fractional
                  << "%)." << std::endl;
    }
    // Finally, run the cache simulator and validate the computed LP solution
    belatedly::CacheSimulator simulator(packets_file_path_, is_solution_integral,
                                        is_forced_, z_, c_, trace_, evictions,
                                        inductions);
    return simulator.run();
}

/**
 * Solve the MCMCF instance for the given trace.
 */
void MCMCFSolver::solve() {

    // Populate the internal structs
    setup();
    std::list<Edge> edges;
    populateOutOfCchEdges(edges);

    // Create optimization model
    GRBEnv env = GRBEnv();
    GRBModel model = GRBModel(env);
    belatedly::FlowDict flows = belatedly::FlowDict(model);

    // If the solution does not already exist, re-run the optimizer
    bool is_loaded_from_file = loadOptimizedModelSolution(flows);
    double opt_cost = std::numeric_limits<double>::max();
    if (!is_loaded_from_file) {

        // Create variables, populate constraints
        createFlowVariables(model, flows, edges);
        populateCchCapacityConstraints(model, flows);
        populateSplitNodeFlowConstraints(model, flows);
        populateFlowConservationConstraints(model, flows);

        // Compute the optimal solution
        model.set(GRB_IntAttr_ModelSense, GRB_MINIMIZE);
        model.set(GRB_DoubleParam_NodefileStart, 5);
        model.set(GRB_IntParam_Threads, 1);
        model.set(GRB_IntParam_Method, 1);
        model.optimize();

        // Populate the solution vector
        int status = model.get(GRB_IntAttr_Status);
        assert(status == GRB_OPTIMAL); // Sanity check
        for (size_t idx = 0; idx < flows.numVariables(); idx++) {
            solution_.push_back(flows.getVariableAt(idx).get(GRB_DoubleAttr_X));
        }

        // Save the optimal solution
        saveOptimizedModelSolution(flows);

        // Fetch the optimal cost
        opt_cost = model.get(GRB_DoubleAttr_ObjVal);
        for (const auto& iter : indices_) {
            size_t first_idx = iter.second.front();
            opt_cost += idx_to_miss_cost_.at(first_idx);
        }

        // Print the cost corresponding to the optimal solution
        std::cout << "Optimal cost is: " << std::fixed
                  << std::setprecision(3) << opt_cost
                  << std::endl << std::endl;
    }

    // Note: This point onwards, we cannot assume that the Gurobi model parameters are
    // available (since the solution may have been loaded from file). Instead, we use
    // the flow mappings and the solutions vector in the remainder of the pipeline.
    double lower_bound = getCostLowerBound(flows, edges);
    double upper_bound = getCostUpperBoundAndValidateSolution(flows, edges);
    double delta_percent = ((upper_bound - lower_bound) / lower_bound) * 100;

    // Debug
    std::cout << "Lower-bound (LB) on the total latency cost is: "
              << std::fixed << std::setprecision(3) << lower_bound
              << "." << std::endl;

    std::cout << "Upper-bound (UB) on the total latency cost is: "
              << std::fixed << std::setprecision(3) << upper_bound
              << " (" << delta_percent << "% worse than LB)."
              << std::endl << std::endl;

    // If the model was solved in this instance, also validate the opt_cost
    if (!is_loaded_from_file) {
        assert(utils::DoubleApproxEqual(opt_cost, lower_bound, 1e-2));
    }
}

int main(int argc, char **argv) {
    // Parameters
    uint z;
    double c_scale;
    std::string trace_fp;
    std::string model_fp;
    std::string packets_fp;

    // Program options
    bopt::variables_map variables;
    bopt::options_description desc{"BELATEDLY's MCMCF-based solver for"
                                   " the Delayed Hits caching problem"};
    try {
        // Command-line arguments
        desc.add_options()
            ("help",        "Prints this message")
            ("trace",       bopt::value<std::string>(&trace_fp)->required(),            "Input trace file path")
            ("cscale",      bopt::value<double>(&c_scale)->required(),                  "Parameter: Cache size (%Concurrent Flows)")
            ("zfactor",     bopt::value<uint>(&z)->required(),                          "Parameter: Z")
            ("model",       bopt::value<std::string>(&model_fp)->default_value(""),     "[Optional] Output model file prefix (for checkpointing)")
            ("packets",     bopt::value<std::string>(&packets_fp)->default_value(""),   "[Optional] Output packets file path")
            ("forced,f",                                                                "[Optional] Use forced admission");

        // Parse model parameters
        bopt::store(bopt::parse_command_line(argc, argv, desc), variables);

        // Handle help flag
        if (variables.count("help")) {
            std::cout << desc << std::endl;
            return 0;
        }
        bopt::notify(variables);
    }
    // Flag argument errors
    catch(const bopt::required_option& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    catch(...) {
        std::cerr << "Unknown Error." << std::endl;
        return 1;
    }

    // Use forced admission?
    bool is_forced = (variables.count("forced") != 0);

    // Parse the trace file, and compute the absolute cache size
    std::vector<std::string> trace = utils::parseTrace(trace_fp);
    size_t num_cfs = utils::getFlowCounts(trace).num_concurrent_flows;
    uint c = std::max(1u, static_cast<uint>(round((num_cfs * c_scale) / 100.)));

    // Debug
    std::cout << "Optimizing trace: " << trace_fp << " (with " << num_cfs
              << " concurrent flows) using z = " << z << ", c = " << c
              << ", and " << (is_forced ? "forced" : "optional")
              << " admission." << std::endl;

    // Instantiate the solver and optimize
    MCMCFSolver solver(trace_fp, model_fp, packets_fp,
                       is_forced, z, c, trace);
    solver.solve();
}
