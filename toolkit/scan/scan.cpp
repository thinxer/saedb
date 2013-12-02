// Simple edge counting program
#include <iostream>
#include <random>
#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <atomic>

#include "gflags/gflags.h"
#include "streaming/streaming.hpp"

using namespace std;
using namespace sae::streaming;

DEFINE_bool(weighted, false, "If not set, will use 1.0 for weight.");

enum VertexType {
    UNDEFINED = 0,
    UNCLASSIFIED = -1,
    NONMEMBER = -2,
    OUTLINER = -3,
    HUB = -4
};

namespace {
    template<typename K, typename V>
    double sparse_dot(const vector<pair<K, V>>& v1,
                      const vector<pair<K, V>>& v2) {
        double dot = 0;
        auto it1 = v1.begin();
        auto it2 = v2.begin();
        while (it1 != v1.end() && it2 != v2.end()) {
            if (it1->first == it2->first) {
                dot += it1->second * it2->second;
                it1++;
                it2++;
            } else if (it1->first < it2->first) {
                it1++;
            } else {
                it2++;
            }
        }
        return dot;
    }
}

struct SCAN {

    // Have to define SCANContext nested. Otherwise will have circular reference problem.
    struct SCANContext : public Context<SCAN> {
        // the next available cluster id
        atomic<int> cluster_id;

        SCANContext() : cluster_id(0) {
        }
    };

    int cluster_id;
    vector<pair<vid_t, double>> neighbors;
    map<vid_t, double> sims;
    double l2_weights = 0;

    void init(const SCANContext& context, const Vertex& v) {}

    void reset() {
        cluster_id = UNCLASSIFIED;
    }

    void edge(const SCANContext& context, vid_t id, const Edge& e) {
        // TODO enable bidirectional as well
        if (id == e.source) {
            double weight = FLAGS_weighted ? stod(e.data) : 1;
            neighbors.emplace_back(e.target, weight);
            l2_weights += weight * weight;
        }
    }

    void finalize() {
        sort(neighbors.begin(), neighbors.end());
        l2_weights = sqrt(l2_weights);
    }

    void calc_sims(const SCANContext& context) {
        // get the cosine of the neighbors
        for (auto& n : neighbors) {
            double dot = sparse_dot(neighbors, context.vertices[n.first].neighbors);
            double sim = dot / (l2_weights * context.vertices[n.first].l2_weights);
            if (isnan(sim))
                sim = 0;
            sims.emplace(n.first, sim);
        }
    }

    void run(SCANContext& context, vid_t id, double eps, int mu) {
        if (cluster_id != UNCLASSIFIED) return;

        auto eps_neigh = get_eps_neighbors(eps);
        DLOG(INFO) << id << ": eps_neigh size: " << eps_neigh.size();
        if (eps_neigh.size() >= mu) {
            // this vertex is a core
            // BFS to spread
            // TODO rewrite to elimate the BFS code using scheduler of context
            cluster_id = context.cluster_id.fetch_add(1);
            DLOG(INFO) << id << " is core, cluster_id = " << cluster_id;
            deque<vid_t> queue(eps_neigh.begin(), eps_neigh.end());
            while (queue.size() > 0) {
                auto y_vertex_id = queue[0];
                queue.pop_front();
                auto y_vertex = context.vertices[y_vertex_id];
                y_vertex.cluster_id = cluster_id;
                auto y_eps_neigh = y_vertex.get_eps_neighbors(eps);
                if (y_eps_neigh.size() >= mu) {
                    // y is a core
                    for (auto y_neigh_id : y_eps_neigh) {
                        auto& y_neigh = context.vertices[y_neigh_id];
                        if (y_neigh.cluster_id == UNCLASSIFIED)
                            queue.emplace_back(y_neigh_id);
                        if (y_neigh.cluster_id == UNCLASSIFIED || y_neigh.cluster_id == NONMEMBER)
                            y_neigh.cluster_id = cluster_id;
                    }
                }
            }
        } else {
            cluster_id = NONMEMBER;
        }
    }

    // determine hubs and outliers
    void determine(const SCANContext& context) {
        if (cluster_id == NONMEMBER) {
            set<int> neighbor_clusters;
            for (auto& n : neighbors) {
                neighbor_clusters.insert(context.vertices[n.first].cluster_id);
            }
            if (neighbors.size() > 10 && neighbor_clusters.size() > 2) {
                cluster_id = HUB;
            } else {
                cluster_id = OUTLINER;
            }
        }
    }

    void output(const SCANContext& context, vid_t id, ostream& os) const {
        os << id << " " << cluster_id << "\n";
    }

    vector<vid_t> get_eps_neighbors(double eps) {
        vector<vid_t> v;
        for (auto& n : sims) {
            if (n.second >= eps)
                v.push_back(n.first);
        }
        return v;
    }
};

int sgraph_main(StreamingGraph* g) {
    SCAN::SCANContext context;
    SinglePassRun(context, g);
    context.run("finalize", &SCAN::finalize);
    context.run("calc sims", &SCAN::calc_sims, ref(context));

    int mu = 2;
    for (double eps = 0.1; eps <= 1.0; eps += 0.1) {
        context.cluster_id = 0;
        context.run("reset", &SCAN::reset);
        context.run("run " + to_string(eps), &SCAN::run, ref(context), _vertex_id, eps, mu);
        context.run("determine " + to_string(eps), &SCAN::determine, ref(context));
        cout << "\nResult for eps=" << eps << ", mu=" << mu << "\n";
        Output(context, ref(cout));
        // TODO caclulate directed newman modularity
        // Evaluate.java calculateDirectedModularity
    }
    return 0;
}
