#ifndef cache_common_h
#define cache_common_h

// STD headers
#include <assert.h>
#include <string>
#include <unordered_map>

// Custom headers
#include "MurmurHash3.h"

namespace caching {

/**
 * Represents a single cache entry.
 */
class CacheEntry {
private:
    std::string key_; // Tag used to uniquely identify objects
    bool is_valid_ = false; // Whether this cache entry is valid

public:
    // Accessors
    bool isValid() const { return is_valid_; }
    const std::string& key() const { return key_; }

    // Mutators
    void toggleValid() { is_valid_ = !is_valid_; }
    void update(const std::string& key) { key_ = key; }
};

/**
 * Implements a parameterizable LRU queue.
 */
template<class T> class LRUQueue {
private:
    typedef typename std::list<T>::iterator Iterator;
    typedef typename std::unordered_map<std::string, Iterator>::iterator PositionIterator;
    std::unordered_map<std::string, Iterator> positions_; // A dict mapping keys to iterators
    std::list<T> entries_; // An ordered list of T instances. The list is ordered such that, at
                           // any time, the element at the front of the queue is the LRU entry.
    // Helper method
    const std::string& getKey(const T& entry) const { return entry.key(); }

public:
    // Accessors
    std::list<T>& entries() { return entries_; }
    size_t size() const { return entries_.size(); }
    const std::list<T>& entries() const { return entries_; }
    std::unordered_map<std::string, Iterator>& positions() { return positions_; }
    const std::unordered_map<std::string, Iterator>& positions() const { return positions_; }

    /**
     * Membership test.
     */
    bool contains(const std::string& key) const {
        return (positions_.find(key) != positions_.end());
    }

    /**
     * Erase the given queue entry.
     */
    void erase(const PositionIterator& position_iter) {
        entries_.erase(position_iter->second);
        positions_.erase(position_iter);
    }

    /**
     * Pop the entry at the front of the queue.
     */
    T popFront() {
        T entry = entries_.front();
        positions_.erase(getKey(entry));
        entries_.pop_front();
        return entry;
    }

    /**
     * Insert the given entry at the back of the queue.
     */
    void insertBack(const T& entry) {
        const std::string& key = getKey(entry);
        assert(positions_.find(key) == positions_.end());

        entries_.push_back(entry);
        positions_[key] = std::prev(entries_.end());
    }
};

// Template specializations
template<> const std::string&
LRUQueue<std::string>::getKey(const std::string& entry) const { return entry; }

/**
 * Represents a min-heap entry.
 */
template<class T> class MinHeapEntry {
private:
    std::string key_; // Cache tag corresponding to this min-heap entry
    size_t last_ref_time_; // Time (in clock cycles) of last reference
    size_t insertion_time_; // Time (in clock cycles) of insertion
    T primary_metric_; // The primary priority metric

public:
    MinHeapEntry(const std::string& key, const T& metric, const size_t lr_time,
                 const size_t in_time) : key_(key), last_ref_time_(lr_time),
                 insertion_time_(in_time), primary_metric_(metric) {}
    // Accessors
    const std::string& key() const { return key_; }
    T getPrimaryMetric() const { return primary_metric_; }
    size_t getLastRefTime() const { return last_ref_time_; }
    size_t getInsertionTime() const { return insertion_time_; }

    // Comparator
    bool operator<(const MinHeapEntry& other) const {
        // Sort by the primary metric
        if (primary_metric_ != other.primary_metric_) {
            return primary_metric_ >= other.primary_metric_;
        }
        // Sort by last reference time
        else if (last_ref_time_ != other.last_ref_time_) {
            return last_ref_time_ >= other.last_ref_time_;
        }
        // Finally, sort by insertion time
        return insertion_time_ >= other.insertion_time_;
    }
};

/**
 * Available hash functions.
 */
enum HashType {
    NO_HASH = 0,
    MURMUR_HASH,
};

/**
 * Represents a family of hash functions, H = {h_0, h_1, ...}.
 */
class HashFamily {
private:
    const size_t kNumHashes;
    const HashType kHashType;

public:
    HashFamily(const size_t num_hashes, const HashType type) :
        kNumHashes(num_hashes), kHashType(type) {}

    /**
     * Computes the h_{idx}'th hash for the given key.
     */
    size_t hash(const size_t idx, const std::string& key) const {
        assert(idx < kNumHashes && kHashType == MURMUR_HASH);
        uint64_t hashes[2]; // The computed, 128-bit hash

        MurmurHash3_x64_128(key.data(), static_cast<int>(key.size()), idx + 1, &hashes);
        return hashes[0]; // Return the lower 64 bits
    }
};

} // namespace caching

#endif // cache_common_h
