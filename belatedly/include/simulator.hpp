#ifndef simulator_hpp
#define simulator_hpp

// STD headers
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <random>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Boost headers
#include <boost/bimap.hpp>

// Custom headers
#include "utils.hpp"

namespace belatedly {

/**
 * Implements a lightweight simulator for a fully-associative cache. Also
 * implements integer rounding and flow balance heuristics to generate an
 * integral cache schedule for a fractional MCMCF solution.
 */
class CacheSimulator {
private:
    typedef std::list<size_t>::const_iterator IdxIterator;
    typedef std::tuple<size_t, std::string, double> CacheDecision;
    typedef std::list<size_t>::const_reverse_iterator RevIdxIterator;

    // Trace and cache parameters
    const size_t z_;
    const size_t c_;
    const bool is_forced_;
    const size_t trace_len_;
    const bool validate_schedule_;
    const std::string packets_file_path_;
    const std::vector<std::string> trace_;
    const std::vector<CacheDecision> evictions_;
    const std::vector<CacheDecision> inductions_;

    // Randomized rounding
    std::random_device rd_;
    std::mt19937 generator_;
    std::uniform_real_distribution<> distribution_;

    // Flow balance heuristic. This is currently implemented
    // using a map, but it should really be a priority queue.
    std::unordered_map<std::string, double> expected_flows_;

    // Housekeeping
    size_t clk_ = 0;
    size_t evictions_idx_= 0;
    size_t total_latency_ = 0;
    size_t inductions_idx_ = 0;
    size_t num_eviction_violations_ = 0;
    std::unordered_set<std::string> cache_;
    std::list<utils::Packet> all_processed_packets_;
    std::unordered_map<std::string, std::list<utils::Packet>> packet_queues_;

    // A dictionary mapping clk values to the keys
    // whose blocking reads complete on that cycle.
    boost::bimap<size_t, std::string> completed_reads_;

public:
    CacheSimulator(
        const std::string& packets_fp, const bool validate, const bool is_forced,
        const uint z, const uint c, const std::vector<std::string>& trace, const
        std::vector<CacheDecision>& evictions, const std::vector<CacheDecision>&
        inductions) : z_(z), c_(c), is_forced_(is_forced), trace_len_(trace.size()),
        validate_schedule_(validate), packets_file_path_(packets_fp), trace_(trace),
        evictions_(evictions), inductions_(inductions), generator_(rd_()),
        distribution_(0.0, 1.0) {}

    void process(utils::Packet& packet);
    void processAllPacketQueues();
    void teardown();
    double run();
};

/**
 * Processes the remaining packets in every queue.
 */
void CacheSimulator::processAllPacketQueues() {
    std::unordered_map<std::string, double> evictions;
    std::string flow_read_completed;
    std::string induction;

    // Forward the inductions iterator until the idx
    // corresponds to one performed on this cycle.
    while (inductions_idx_ < inductions_.size() &&
           std::get<0>(inductions_[inductions_idx_]) <= clk_) {
        const auto& item = inductions_[inductions_idx_];
        const std::string& flow_id = std::get<1>(item);
        const double inducted_fraction = std::get<2>(item);

        // Update the inducted item. There should
        // be *at most* one induction every cycle.
        if (std::get<0>(item) == clk_) {
            auto iter = expected_flows_.find(flow_id);
            assert(induction.empty() || induction == flow_id);
            double net_flow = (((iter != expected_flows_.end()) ?
                                iter->second : 0.) + inducted_fraction);
            // Sanity checks
            assert(utils::DoubleApproxGreaterThanOrEqual(net_flow, 0.) &&
                   utils::DoubleApproxGreaterThanOrEqual(1., net_flow));

            // Update the expected flow fraction
            if (iter != expected_flows_.end()) { iter->second = net_flow; }
            else { expected_flows_[flow_id] = net_flow; }
            induction = flow_id;
        }
        inductions_idx_++;
    }
    // Forward the evictions iterator until the idx
    // corresponds to one performed on this cycle.
    while (evictions_idx_ < evictions_.size() &&
           std::get<0>(evictions_[evictions_idx_]) <= clk_) {
        const auto& item = evictions_[evictions_idx_];
        const std::string& flow_id = std::get<1>(item);
        const double evicted_fraction = std::get<2>(item);

        // Populate the evictions map
        if (std::get<0>(item) == clk_) {
            auto iter = expected_flows_.find(flow_id);
            assert(iter != expected_flows_.end()); // Sanity check

            double net_flow = (iter->second - evicted_fraction);
            assert(utils::DoubleApproxGreaterThanOrEqual(net_flow, 0.) &&
                   utils::DoubleApproxGreaterThanOrEqual(1., net_flow));

            // Update the evictions map and expected flow fraction
            assert(evictions.find(flow_id) == evictions.end());
            evictions[flow_id] = evicted_fraction;
            iter->second = net_flow;
        }
        evictions_idx_++;
    }
    // A blocking read completed on this cycle
    auto completed_read = completed_reads_.left.find(clk_);
    if (completed_read != completed_reads_.left.end()) {
        const std::string flow_id = completed_read->second;
        std::list<utils::Packet>& queue = packet_queues_.at(flow_id);
        assert(!queue.empty()); // Sanity check: Queue must not be empty

        // Sanity checks
        flow_read_completed = flow_id;
        assert(cache_.find(flow_id) == cache_.end());
        assert(queue.front().getTotalLatency() == z_);

        // Purge the queue, as well as the bidict mapping
        all_processed_packets_.insert(all_processed_packets_.end(),
                                      queue.begin(), queue.end());
        queue.clear();
        packet_queues_.erase(flow_id);
        completed_reads_.left.erase(completed_read);

        cache_.insert(flow_id); // Finally, cache this flow
        if (is_forced_ && (clk_ != (trace_len_ + z_ - 2))) {

            // Sanity check: Ensure that in case of forced admission,
            // split-node constraints (all packets must remain in the
            // cache for at least one cycle) are respected.
            if (validate_schedule_) {
                assert(evictions.find(flow_id) == evictions.end());
            }
        }
    }
    // Evict the appropriate flows
    std::unordered_set<std::string> evicted_flows;
    for (const auto& eviction : evictions) {
        const double evicted_fraction = eviction.second;
        const std::string& evicted_flow_id = eviction.first;

        // If performing validation, ensure that the required flow(s) are evicted.
        // Else, perform the operation with a probability equal to the fraction of
        // flow evicted at this time-step.
        if (validate_schedule_ || (distribution_(generator_) <= evicted_fraction)) {
            bool is_evicted = cache_.erase(evicted_flow_id);
            if (validate_schedule_) {  assert(is_evicted); }
            if (is_evicted) { evicted_flows.insert(evicted_flow_id); }
        }
    }
    // Now, if the cache size constraint is still violated,
    // evict the flow in the cache which is most unbalanced
    // (that is, with the lowest expected flow fraction).
    if (cache_.size() > c_) {
        std::string flow_id_to_evict;
        double min_flow_fraction = std::numeric_limits<double>::max();

        for (const std::string& flow_id : cache_) {
            auto iter = expected_flows_.find(flow_id);
            assert(iter != expected_flows_.end() &&
                   utils::DoubleApproxGreaterThanOrEqual(iter->second, 0.));

            // Update the parameters of the flow to evict
            if (iter->second < min_flow_fraction) {
                min_flow_fraction = iter->second;
                flow_id_to_evict = flow_id;
            }
        }
        // Record violations of split-node constraints. TODO(natre):
        // Rewrite this logic to better accommodate forced admission.
        if (is_forced_ && (clk_ != (trace_len_ + z_ - 2) &&
            (flow_read_completed == flow_id_to_evict))) {
            num_eviction_violations_++;
        }

        // Evict the required flow
        evicted_flows.insert(flow_id_to_evict);
        assert(cache_.erase(flow_id_to_evict) == 1);
    }
    // Sanity check: Ensure that the
    // cache size constraint is met.
    assert(cache_.size() <= c_);
    clk_++;
}

/**
 * Processes the packet that appears in the trace on the current cycle.
 */
void CacheSimulator::process(utils::Packet& packet) {
    packet.setArrivalClock(clk_);
    const std::string& flow_id = packet.getFlowId();

    // First, if the flow is cached, process the packet immediately
    auto queue_iter = packet_queues_.find(flow_id);
    if (cache_.find(flow_id) != cache_.end()) {
        assert(queue_iter == packet_queues_.end());

        packet.finalize();
        all_processed_packets_.push_back(packet);
    }
    // Else, we must either: a) perform a blocking read from memory,
    // or b) wait for an existing blocking read to complete. Insert
    // this packet into the corresponding packet queue.
    else {
        // If this flow's packet queue doesn't yet exist, this is the
        // blocking packet, and its read completes on cycle (clk + z).
        if (queue_iter == packet_queues_.end()) {
            size_t target_clk = clk_ + z_ - 1;
            assert(completed_reads_.right.find(flow_id) ==
                   completed_reads_.right.end());

            assert(completed_reads_.left.find(target_clk) ==
                   completed_reads_.left.end());

            completed_reads_.insert(boost::bimap<size_t, std::string>::
                                    value_type(target_clk, flow_id));
            packet.addLatency(z_);
            packet.finalize();

            // Initialize a new queue for this flow
            packet_queues_[flow_id].push_back(packet);
        }
        // Update the flow's packet queue
        else {
            size_t target_clk = completed_reads_.right.at(flow_id);
            packet.setQueueingDelay(queue_iter->second.size());
            packet.addLatency(target_clk - clk_ + 1);
            packet.finalize();

            // Add this packet to the existing flow queue
            queue_iter->second.push_back(packet);
        }
        assert(packet.isFinalized()); // Sanity check
    }

    // Finally, process all queued packets
    processAllPacketQueues();
}

/**
 * Indicates completion of the simulation.
 */
void CacheSimulator::teardown() {
    while (!packet_queues_.empty()) {
        processAllPacketQueues();
    }
}

/**
 * Run the simulation.
 */
double CacheSimulator::run() {
    for (size_t idx = 0; idx < trace_len_; idx++) {
        const std::string& flow_id = trace_[idx];

        utils::Packet packet(flow_id);
        !flow_id.empty() ? process(packet) :
                           processAllPacketQueues();
    }
    teardown();

    // Compute the total latency
    for (const auto& packet : all_processed_packets_) {
        total_latency_ += packet.getTotalLatency();
    }
    // Write the raw packets to file
    if (!packets_file_path_.empty()) {
        std::ofstream packets_file(packets_file_path_, std::ios::out | std::ios::trunc);
        packets_file << "Latency-OPT;" << c_ << ";1;" << c_ << ";1.00" << std::endl;
        for (const auto& packet : all_processed_packets_) {
            packets_file << packet.getFlowId() << ";"
                         << packet.getTotalLatency()
                         << ";" << packet.getQueueingDelay() << std::endl;
        }
    }
    // Debug
    if (num_eviction_violations_ > 0) {
        std::cout << "Warning: Found " << num_eviction_violations_
                  << " schedule violations." << std::endl;
    }
    else {
        std::cout << "Validated schedule, all checks passed." << std::endl;
    }
    // Return the total latency for this simulation
    return total_latency_;
}

} // namespace belatedly

#endif // simulator_hpp
