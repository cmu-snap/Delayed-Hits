#ifndef flow_dict_hpp
#define flow_dict_hpp

// STD headers
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <tuple>
#include <unordered_map>
#include <vector>

// Boost headers
#include <boost/bimap.hpp>
#include <boost/bimap/multiset_of.hpp>
#include <boost/functional/hash.hpp>
#include <boost/range/iterator_range.hpp>

// Cereal headers
#include <cereal/archives/binary.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/unordered_map.hpp>

// Gurobi headers
#include <gurobi_c++.h>

// Custom headers
#include "utils.hpp"

namespace belatedly {

// Typedefs
typedef std::tuple<std::string, bool> Keyname;
typedef std::tuple<std::string, bool, std::string, std::string> Edgename;
typedef boost::bimap<boost::bimaps::multiset_of<std::string>,
                     boost::bimaps::multiset_of<std::string>> Bimap;

/**
 * Implements the equivalent of a Python tupledict, specialized for flow
 * decision variables of the form: (flow_id, in_cache, src, dst) -> cost,
 * where flow_id, src, and dst are strings, in_cache is a bool, and cost
 * is a double. Here, "in_cache" indicates whether this the decicions
 * variable corresponds to a V_{cch} or V_{mem} node.
 *
 * The data-structure is implemented as follows:
 * 1. We map a (flow_id, in_cache) tuple to a bimap of src, dst multisets
 * 2. To sum across either srcs/dsts, i.e. (flow_id, in_cache, *, dst) or
 *    (flow_id, in_cache, src, *), we perform a range-based query on the
 *    value mapped to the corresponding (flow_id, in_cache) pair.
 *
 * To interface with Gurobi, we map the full Edgename (flow_id, in_cache,
 * src, dst) to the approporiate index in Gurobi's internal vars array.
 * The underlying Gurobi variables should not be modified directly!
 */
class FlowDict {
private:
    GRBModel& model_; // Reference to the Gurobi model
    int num_variables_; // Number of decision variables
    std::unordered_map<Edgename, int> edge_to_idx_; // Edge -> Idx
    bool is_loaded_from_file_; // Whether the flow mappings are loaded from file
    std::unordered_map<Keyname, Bimap> flow_to_edges_; // (Flow ID, In Cache) ->
                                                       //           (Src <-> Dst)

    inline Keyname keyname(const std::string& a, const bool b)
                           const { return std::make_tuple(a, b); }

    inline Edgename edgename(const std::string& a, const bool b,
                             const std::string& c, const std::string& d)
                             const { return std::make_tuple(a, b, c, d); }
public:
    FlowDict(GRBModel& model) : model_(model), num_variables_(0),
                                is_loaded_from_file_(false) {}

    // Load/store flow mappings to disk
    void loadFlowMappings(const std::string& file_path);
    void saveFlowMappings(const std::string& file_path) const;

    // Mutator
    int addVariable(const std::string& flow_id, const bool in_cache,
                    const std::string& src, const std::string& dst,
                    const double cost);

    // Accessors
    GRBVar getVariableAt(const int idx) const;
    size_t numVariables() const { return num_variables_; }
    int getVariableIdx(const std::string& flow_id, const bool in_cache,
                       const std::string& src, const std::string& dst) const;

    GRBVar getVariable(const std::string& flow_id, const bool in_cache,
                       const std::string& src, const std::string& dst) const;

    GRBLinExpr sumAcrossSrcNodes(const std::string& flow_id,
                                 const bool in_cache,
                                 const std::string& dst) const;

    GRBLinExpr sumAcrossDstNodes(const std::string& flow_id,
                                 const bool in_cache,
                                 const std::string& src) const;

    GRBLinExpr sumAcrossSrcNodes(const std::string& flow_id,
                                 const std::string& dst) const;

    GRBLinExpr sumAcrossDstNodes(const std::string& flow_id,
                                 const std::string& src) const;
};

/**
 * Given a file path, loads the Edge -> Idx mappings from it. Note: Once
 * flow mappings are loaded from file, the underlying model should *not*
 * be modified. To enforce this, we disallow access to Gurobi's internal
 * variables through the addVariable() and getVariable() methods.
 */
void FlowDict::loadFlowMappings(const std::string& file_path) {

    // Sanity checks
    assert(!is_loaded_from_file_);
    assert(num_variables_ == 0);
    is_loaded_from_file_ = true;

    // Load the edge_to_idx_ map
    {
        std::ifstream ifs(file_path);
        cereal::BinaryInputArchive ar(ifs);
        ar(edge_to_idx_);
    }
    // Next, populate the flow_to_edges_ map
    for (const auto& elem : edge_to_idx_) {
        const Edgename& edge = elem.first;
        Keyname key = keyname(std::get<0>(edge), std::get<1>(edge));
        flow_to_edges_[key].insert(Bimap::value_type(std::get<2>(edge),
                                                     std::get<3>(edge)));
    }
    // Finally, update the number of decision variables
    num_variables_ = edge_to_idx_.size();
}

/**
 * Given a file path, saves the Edge -> Idx mappings to it.
 */
void FlowDict::saveFlowMappings(const std::string& file_path) const {
    std::ofstream ofs(file_path);
    cereal::BinaryOutputArchive ar(ofs);
    ar(edge_to_idx_);
}

/**
 * Given a set of edge parameters (flow ID, src node, dst node,
 * and cost), adds a new decision variable to the Gurobi model.
 * Returns the idx corresponding to the new variable.
 */
int FlowDict::addVariable(const std::string& flow_id, const bool in_cache,
                          const std::string& src, const std::string& dst,
                          const double cost) {
    // Sanity check
    assert(!is_loaded_from_file_);

    // First, add the variable to the Gurobi model
    Edgename edge = edgename(flow_id, in_cache, src, dst);
    model_.addVar(0, 1, cost, GRB_CONTINUOUS, "");
    int idx = num_variables_++;

    // Store the bi-directional mapping
    Keyname key = keyname(flow_id, in_cache);
    flow_to_edges_[key].insert(Bimap::value_type(src, dst));

    // Sanity check: Ensure that this edge doesn't already exist
    assert(edge_to_idx_.find(edge) == edge_to_idx_.end());
    edge_to_idx_[edge] = idx;
    return idx;
}

/**
 * Returns the decision variable at the given idx.
 */
GRBVar FlowDict::getVariableAt(const int idx) const {
    assert(idx < num_variables_); // Sanity check
    return model_.getVar(idx);
}

/**
 * Given a set of edge parameters, returns the
 * idx of the corresponding decision variable.
 */
int FlowDict::
getVariableIdx(const std::string& flow_id, const bool in_cache,
               const std::string& src, const std::string& dst) const {
    // Fetch the corresponding idx (ensuring that it exists)
    Edgename edge = edgename(flow_id, in_cache, src, dst);
    return edge_to_idx_.at(edge);
}

/**
 * Given a set of edge parameters, returns the corresponding decision variable.
 */
GRBVar FlowDict::
getVariable(const std::string& flow_id, const bool in_cache,
            const std::string& src, const std::string& dst) const {
    assert(!is_loaded_from_file_); // Sanity check
    return getVariableAt(getVariableIdx(flow_id, in_cache, src, dst));
}

/**
 * Given a flow ID and dst node, returns a linear expression corresponding to
 * the sum of in/out-of-cache edges containing the given parameters. That is,
 * it returns: sum(variables_[flow_id, in_cache, X, dst]) for all X.
 */
GRBLinExpr FlowDict::sumAcrossSrcNodes(const std::string& flow_id,
                                       const bool in_cache,
                                       const std::string& dst) const {
    GRBLinExpr sum;
    const Keyname key = keyname(flow_id, in_cache);
    const auto& bimap_iter = flow_to_edges_.find(key);

    // Find the decision variables corresponding to the edges
    // containing this flow ID and dst, and compute their sum.
    if (bimap_iter != flow_to_edges_.end()) {
        const auto& bimap = bimap_iter->second;
        for (const auto& edge : boost::make_iterator_range(
            bimap.right.equal_range(dst))) {
            sum += getVariable(flow_id, in_cache, edge.second, dst);
        }
    }
    return sum;
}

/**
 * Given a flow ID and dst node, returns a linear expression corresponding
 * to the sum of all edges containing the given parameters. In other words,
 * returns: sum(variables_[flow_id, X, Y, dst]) for all X, Y.
 */
GRBLinExpr FlowDict::sumAcrossSrcNodes(const std::string& flow_id,
                                       const std::string& dst) const {
    return (sumAcrossSrcNodes(flow_id, true, dst) +
            sumAcrossSrcNodes(flow_id, false, dst));
}

/**
 * Given a flow ID and src node, returns a linear expression corresponding to
 * the sum of in/out-of-cache edges containing the given parameters. That is,
 * it returns: sum(variables_[flow_id, in_cache, src, X]) for all X.
 */
GRBLinExpr FlowDict::sumAcrossDstNodes(const std::string& flow_id,
                                       const bool in_cache,
                                       const std::string& src) const {
    GRBLinExpr sum;
    const Keyname key = keyname(flow_id, in_cache);
    const auto& bimap_iter = flow_to_edges_.find(key);

    // Find the decision variables corresponding to the edges
    // containing this flow ID and dst, and compute their sum.
    if (bimap_iter != flow_to_edges_.end()) {
        const auto& bimap = bimap_iter->second;
        for (const auto& edge : boost::make_iterator_range(
            bimap.left.equal_range(src))) {
            sum += getVariable(flow_id, in_cache, src, edge.second);
        }
    }
    return sum;
}

/**
 * Given a flow ID and src node, returns a linear expression corresponding
 * to the sum of all edges containing the given parameters. In other words,
 * returns: sum(variables_[flow_id, X, src, Y]) for all X, Y.
 */
GRBLinExpr FlowDict::sumAcrossDstNodes(const std::string& flow_id,
                                       const std::string& src) const {
    return (sumAcrossDstNodes(flow_id, true, src) +
            sumAcrossDstNodes(flow_id, false, src));
}

} // namespace belatedly

#endif // flow_dict_hpp
