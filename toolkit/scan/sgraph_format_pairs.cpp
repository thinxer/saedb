// for reading the pairs files from the original SCAN java version.

#include <string>
#include <iostream>
#include <algorithm>
#include <map>
#include <vector>
#include "streaming/sgraph.hpp"


namespace sae {
namespace streaming {

struct Pairs : public StreamingGraph {
    std::istream& is;
    std::map<std::string, vid_t> id_map;
    std::vector<std::pair<vid_t, vid_t>> edges;

    Pairs(std::istream& is) : is(is) {
    }

    void process(const Trigger<Graph>& onGraph, const Trigger<Vertex>& onVertex, const Trigger<Edge>& onEdge) {
        std::string a, b;
        while (is >> a >> b) {
            vid_t id_a = get_id(a), id_b = get_id(b);
            edges.emplace_back(id_a, id_b);
        }
        onGraph(Graph{vid_t(id_map.size()), eid_t(edges.size())});
        for (auto& v : id_map) {
            onVertex(Vertex{v.second, 0, ""});
        }
        eid_t eid = 0;
        for (auto& e : edges) {
            onEdge(Edge{eid++, e.first, e.second, 0, ""});
        }
    }

    vid_t get_id(std::string s) {
        auto it = id_map.find(s);
        if (it == id_map.end()) {
            vid_t id = id_map.size();
            id_map.emplace(s, id);
            return id;
        }
        return it->second;
    }
};

} // namespace streaming
} // namespace sae

REGSITER_GRAPH_FORMAT(pairs, sae::streaming::Pairs);
