#ifndef cache_base_h
#define cache_base_h

// STD headers
#include <assert.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <string>
#include <unordered_map>
#include <unordered_set>

// Boost headers
#include <boost/bimap.hpp>
#include <boost/program_options.hpp>

// Custom headers
#include "utils.hpp"
#include "cache_common.hpp"

namespace caching {

/**
 * Abstract base class representing a generic cache-set.
 */
class BaseCacheSet {
protected:
    const size_t kNumEntries; // The number of cache entries in this set
    std::unordered_set<std::string> occupied_entries_set_; // Set of currently
                                                           // cached flow IDs.
public:
    BaseCacheSet(const size_t num_entries) : kNumEntries(num_entries) {}
    virtual ~BaseCacheSet() {}

    // Membership test (internal use only)
    bool contains(const std::string& flow_id) const {
        return (occupied_entries_set_.find(flow_id) !=
                occupied_entries_set_.end());
    }
    /**
     * Returns the number of cache entries in this set
     */
    size_t getNumEntries() const { return kNumEntries; }

    /**
     * Records arrival of a new packet.
     */
    virtual void recordPacketArrival(const utils::Packet& packet) {
        SUPPRESS_UNUSED_WARNING(packet);
    }

    /**
     * Simulates a cache write.
     *
     * @param key The key corresponding to this write request.
     * @param packet The packet corresponding to this write request.
     * @return The written CacheEntry instance.
     */
    virtual CacheEntry
    write(const std::string& key, const utils::Packet& packet) = 0;

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
    writeq(const std::list<utils::Packet>& queue) {
        CacheEntry written_entry;
        for (const auto& packet : queue) {
            written_entry = write(packet.getFlowId(), packet);
        }
        return written_entry;
    }
};

/**
 * Abstract base class representing a single-tiered cache.
 */
class BaseCache {
protected:
    const size_t kCacheMissLatency;         // Cost (in cycles) of an L1 cache miss
    const size_t kMaxNumCacheSets;          // Maximum number of sets in the L1 cache
    const size_t kMaxNumCacheEntries;       // Maximum number of entries in the L1 cache
    const size_t kCacheSetAssociativity;    // Set-associativity of the L1 cache
    const size_t kIsPenalizeInsertions;     // Whether insertions should incur an L1 cache miss
    const HashFamily kHashFamily;           // A HashFamily instance

    size_t clk_ = 0; // Time in clock cycles
    size_t total_latency_ = 0; // Total packet latency
    std::vector<BaseCacheSet*> cache_sets_; // Fixed-sized array of CacheSet instances
    std::unordered_set<std::string> memory_entries_; // Set of keys in the global store
    boost::bimap<size_t, std::string> completed_reads_; // A dictionary mapping clk values to the keys
                                                        // whose blocking reads complete on that cycle.

    std::unordered_map<std::string, std::list<utils::Packet>>
    packet_queues_; // Dictionary mapping keys to queued requests. Each queue
                    // contains zero or more packets waiting to be processed.
public:
    BaseCache(const size_t miss_latency, const size_t cache_set_associativity, const size_t
              num_cache_sets, const bool penalize_insertions, const HashType hash_type) :
              kCacheMissLatency(miss_latency), kMaxNumCacheSets(num_cache_sets),
              kMaxNumCacheEntries(num_cache_sets * cache_set_associativity),
              kCacheSetAssociativity(cache_set_associativity),
              kIsPenalizeInsertions(penalize_insertions),
              kHashFamily(1, hash_type) {}

    virtual ~BaseCache() {
        // Deallocate the cache sets
        assert(cache_sets_.size() == kMaxNumCacheSets);
        for (size_t idx = 0; idx < kMaxNumCacheSets; idx++) { delete(cache_sets_[idx]); }
    }

    /**
     * Returns the canonical cache name.
     */
    virtual std::string name() const = 0;

    /**
     * Records arrival of a new packet.
     */
    virtual void recordPacketArrival(const utils::Packet& packet) {
        SUPPRESS_UNUSED_WARNING(packet);
    }

    /**
     * Returns the cache miss latency.
     */
    size_t getCacheMissLatency() const { return kCacheMissLatency; }

    /**
     * Returns the number of entries in the memory at any instant.
     */
    size_t getNumMemoryEntries() const { return memory_entries_.size(); }

    /**
     * Returns the current time in clock cycles.
     */
    size_t clk() const { return clk_; }

    /**
     * Returns the total packet latency for this simulation.
     */
    size_t getTotalLatency() const { return total_latency_; }

    /**
     * Returns the cache index corresponding to the given key.
     */
    size_t getCacheIndex(const std::string& key) const {
        return (kMaxNumCacheSets == 1) ?
            0 : kHashFamily.hash(0, key) % kMaxNumCacheSets;
    }

    /**
     * Increments the internal clock.
     */
    void incrementClk() { clk_++; }

    /**
     * Handles any blocking read completions on this cycle.
     */
    void processAll(std::list<utils::Packet>& processed_packets) {

        // A blocking read completed on this cycle
        auto completed_read = completed_reads_.left.find(clk());
        if (completed_read != completed_reads_.left.end()) {
            const std::string& key = completed_read->second;
            std::list<utils::Packet>& queue = packet_queues_.at(key);
            assert(!queue.empty()); // Sanity check: Queue may not be empty

            // Fetch the cache set corresponding to this key
            size_t cache_idx = getCacheIndex(key);
            BaseCacheSet& cache_set = *cache_sets_[cache_idx];

            // Sanity checks
            assert(!cache_set.contains(key));
            assert(queue.front().getTotalLatency() == kCacheMissLatency);

            // Commit the queued entries
            cache_set.writeq(queue);
            processed_packets.insert(processed_packets.end(),
                                     queue.begin(), queue.end());

            // Purge the queue, as well as the bidict mapping
            queue.clear();
            packet_queues_.erase(key);
            completed_reads_.left.erase(completed_read);
        }

        // Finally, increment the clock
        incrementClk();
    }

    /**
     * Processes the parameterized packet.
     */
    void process(utils::Packet& packet, std::list<
                 utils::Packet>& processed_packets) {
        packet.setArrivalClock(clk());

        const std::string& key = packet.getFlowId();
        auto queue_iter = packet_queues_.find(key);
        BaseCacheSet& cache_set = *cache_sets_.at(
            getCacheIndex(key));

        // Record arrival of the packet at
        // the cache and cache-set levels.
        recordPacketArrival(packet);
        cache_set.recordPacketArrival(packet);

        // If this packet corresponds to a new flow, allocate its context
        if (memory_entries_.find(key) == memory_entries_.end()) {
            assert(!cache_set.contains(key));
            memory_entries_.insert(key);

            // Assume that insertions have zero cost.
            // Insert the new entry into the cache.
            if (!kIsPenalizeInsertions) {
                cache_set.write(key, packet);
            }
        }
        // First, if the flow is cached, process the packet immediately.
        // This implies that the packet queue must be non-existent.
        if (cache_set.contains(key)) {
            assert(queue_iter == packet_queues_.end());

            // Note: We currently assume a
            // zero latency cost for hits.
            cache_set.write(key, packet);

            packet.finalize();
            processed_packets.push_back(packet);
            total_latency_ += packet.getTotalLatency();
        }
        // Else, we must either: a) perform a blocking read from memory,
        // or b) wait for an existing blocking read to complete. Insert
        // this packet into the corresponding packet queue.
        else {
            // If this flow's packet queue doesn't yet exist, this is the
            // blocking packet, and its read completes on cycle (clk + z).
            if (queue_iter == packet_queues_.end()) {
                size_t target_clk = clk() + kCacheMissLatency - 1;
                assert(completed_reads_.right.find(key) == completed_reads_.right.end());
                assert(completed_reads_.left.find(target_clk) == completed_reads_.left.end());

                completed_reads_.insert(boost::bimap<size_t, std::string>::
                                        value_type(target_clk, key));
                packet.addLatency(kCacheMissLatency);
                packet.finalize();

                // Initialize a new queue for this flow
                packet_queues_[key].push_back(packet);
            }
            // Update the flow's packet queue
            else {
                size_t target_clk = completed_reads_.right.at(key);
                packet.setQueueingDelay(queue_iter->second.size());
                packet.addLatency(target_clk - clk() + 1);
                packet.finalize();

                // Add this packet to the existing flow queue
                queue_iter->second.push_back(packet);
            }
            assert(packet.isFinalized()); // Sanity check
            total_latency_ += packet.getTotalLatency();
        }
        // Process any completed reads
        processAll(processed_packets);
    }

    /**
     * Indicates completion of the warmup period.
     */
    void warmupComplete() {
        total_latency_ = 0;
        packet_queues_.clear();
        completed_reads_.clear();
    }

    /**
     * Indicates completion of the simulation.
     */
    void teardown(std::list<utils::Packet>& processed_packets) {
        while (!packet_queues_.empty()) {
            processAll(processed_packets);
        }
    }

    /**
     * Save the raw packet data to file.
     */
    static void savePackets(std::list<utils::Packet>& packets,
                            const std::string& packets_fp) {
        if (!packets_fp.empty()) {
            std::ofstream file(packets_fp, std::ios::out |
                                           std::ios::app);
            // Save the raw packets to file
            for (const utils::Packet& packet : packets) {
                file << packet.getFlowId() << ";"
                     << static_cast<size_t>(packet.getTotalLatency()) << ";"
                     << static_cast<size_t>(packet.getQueueingDelay()) << std::endl;
            }
        }
        packets.clear();
    }

    /**
     * Generate and output model benchmarks.
     */
    static void benchmark(BaseCache& model, const std::string& trace_fp, const std::
                          string& packets_fp, const size_t num_warmup_cycles) {
        std::list<utils::Packet> packets; // List of processed packets
        size_t num_counted_packets = 0; // Post-warmup packet count
        size_t num_total_packets = 0; // Total packet count
        size_t num_total_cycles = 0; // Total cycle count

        if (!packets_fp.empty()) {
            std::ofstream file(packets_fp, std::ios::out |
                                           std::ios::trunc);
            // Write the header
            file << model.name() << ";" << model.kCacheSetAssociativity << ";"
                 << model.kMaxNumCacheSets << ";" << model.kMaxNumCacheEntries
                 << std::endl;
        }
        // Process the trace
        std::string line;
        std::ifstream trace_ifs(trace_fp);
        while (std::getline(trace_ifs, line)) {
            std::string timestamp, flow_id;

            /****************************************
             * Important note: Currently, we ignore *
             * packet timestamps and inject at most *
             * 1 packet into the system each cycle. *
             ****************************************/

            // Nonempty packet
            if (!line.empty()) {
                std::stringstream linestream(line);

                // Parse the packet's flow ID and timestamp
                std::getline(linestream, timestamp, ';');
                std::getline(linestream, flow_id, ';');
            }
            // Cache warmup completed
            if (num_total_cycles == num_warmup_cycles) {
                model.warmupComplete(); packets.clear();
                num_counted_packets = 0;

                std::cout << "> Warmup complete after "
                          << num_warmup_cycles
                          << " cycles." << std::endl;
            }
            // Periodically save packets to file
            if (num_counted_packets > 0 &&
                num_counted_packets % 5000000 == 0) {
                if (num_total_cycles >= num_warmup_cycles) {
                    savePackets(packets, packets_fp);
                }
                std::cout << "On packet: " << num_counted_packets
                          << ", latency: " << model.getTotalLatency() << std::endl;
            }
            // Process the packet
            if (!flow_id.empty()) {
                num_total_packets++;
                num_counted_packets++;
                utils::Packet packet(flow_id);
                model.process(packet, packets);
            }
            else { model.processAll(packets); }
            num_total_cycles++;
        }

        // Perform teardown
        model.teardown(packets);
        savePackets(packets, packets_fp);

        // Simulations results
        size_t total_latency = model.getTotalLatency();
        double average_latency = (
            (num_counted_packets == 0) ? 0 :
            static_cast<double>(total_latency) / num_counted_packets);

        // Debug: Print trace and simulation statistics
        std::cout << std::endl;
        std::cout << "Total number of packets in trace: " << num_total_packets << std::endl;
        std::cout << "Post-warmup packet count: " << num_counted_packets << std::endl;
        std::cout << "Total latency is: " << model.getTotalLatency() << std::endl;
        std::cout << "Average latency is: " << std::fixed << std::setprecision(2)
                  << average_latency << std::endl << std::endl;
    }

    /**
     * Run default benchmarks.
     */
    template<class T>
    static void defaultBenchmark(int argc, char** argv) {
        using namespace boost::program_options;

        // Parameters
        size_t z;
        double c_scale;
        std::string trace_fp;
        std::string packets_fp;
        size_t set_associativity;
        size_t num_warmup_cycles;

        // Program options
        variables_map variables;
        options_description desc{"A caching simulator that models Delayed Hits"};

        try {
            // Command-line arguments
            desc.add_options()
                ("help",        "Prints this message")
                ("trace",       value<std::string>(&trace_fp)->required(),            "Input trace file path")
                ("cscale",      value<double>(&c_scale)->required(),                  "Parameter: Cache size (%Concurrent Flows)")
                ("zfactor",     value<size_t>(&z)->required(),                        "Parameter: Z")
                ("packets",     value<std::string>(&packets_fp)->default_value(""),   "[Optional] Output packets file path")
                ("csa",         value<size_t>(&set_associativity)->default_value(0),  "[Optional] Parameter: Cache set-associativity")
                ("warmup",      value<size_t>(&num_warmup_cycles)->default_value(0),  "[Optional] Parameter: Number of cache warm-up cycles");

            // Parse model parameters
            store(parse_command_line(argc, argv, desc), variables);

            // Handle help flag
            if (variables.count("help")) {
                std::cout << desc << std::endl;
                return;
            }
            store(command_line_parser(argc, argv).options(
                desc).allow_unregistered().run(), variables);
            notify(variables);
        }
        // Flag argument errors
        catch(const required_option& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return;
        }
        catch(...) {
            std::cerr << "Unknown Error." << std::endl;
            return;
        }

        const auto flow_counts = utils::getFlowCounts(trace_fp);
        size_t num_total_flows = flow_counts.num_total_flows;
        size_t num_cfs = flow_counts.num_concurrent_flows;

        // Compute the set associativity and set count
        double cache_size = (num_cfs * c_scale) / 100.0;
        if (set_associativity == 0) { set_associativity = std::max<size_t>(
            1, static_cast<size_t>(round(cache_size)));
        }
        size_t num_cache_sets = std::max<size_t>(1,
            static_cast<size_t>(round(cache_size / set_associativity)));

        // Debug: Print the cache and trace parameters
        std::cout << "Parameters: c=" << c_scale << "%, z=" << z << std::endl;
        std::cout << "Total number of flows: " << num_total_flows << std::endl;
        std::cout << "Maximum number of concurrent flows: " << num_cfs << std::endl;

        // Instantiate the model
        T model(z, set_associativity, num_cache_sets, true,
                HashType::MURMUR_HASH, argc, argv);

        // Debug: Print the model parameters
        std::cout << model.name() << ": kCacheSetAssociativity=" << model.kCacheSetAssociativity
                  << ", kMaxNumCacheSets=" << model.kMaxNumCacheSets << ", kMaxNumCacheEntries="
                  << model.kMaxNumCacheEntries << std::endl;

        std::cout << "Starting trace " << trace_fp << "..." << std::endl << std::endl;
        BaseCache::benchmark(model, trace_fp, packets_fp, num_warmup_cycles);
    }
};

} // namespace caching

#endif // cache_base_h
