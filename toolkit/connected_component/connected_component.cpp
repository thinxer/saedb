// For connected component
#include <iostream>
#include <random>
#include <algorithm>

#include "gflags/gflags.h"
#include "streaming/streaming.hpp"

using namespace std;
using namespace sae::streaming;

DEFINE_bool(directed, false, "Whether the graph is directed.");

// Use Union-Find-Set for undirected connected component
struct UndirectedConnectedComponent {
    vid_t component_id;

    void init(const Context<UndirectedConnectedComponent>& context, const Vertex& v) {
        component_id = v.id;
    }

    void edge(Context<UndirectedConnectedComponent>& context, vid_t id, const Edge& e) {
        if (id == e.source) {
            auto x = find(context, e.source);
            auto y = find(context, e.target);
            context.vertices[x].component_id = y;
            DLOG(INFO) << e << ", x=" << x << ", find(x)=" << find(context, e.source) << ", find(y)=" << find(context, e.target);
        }
    }

    void output(Context<UndirectedConnectedComponent>& context, vid_t id, std::ostream& os) {
        os << id << " " << find(context, component_id) << "\n";
    }

private:
    vid_t find(Context<UndirectedConnectedComponent>& context, vid_t vid) {
        vid_t x, y = vid;
        do {
            x = y;
            y = context.vertices[x].component_id;
        } while (x != y);
        x = vid;
        while (x != y) {
            auto z = context.vertices[x].component_id;
            context.vertices[x].component_id = y;
            x = z;
        }
        return x;
    }
};

int sgraph_main(StreamingGraph* g) {
    if (FLAGS_directed) {
        LOG(FATAL) << "Strongly connected component on directed graph is unimplemented yet.";
    } else {
        Context<UndirectedConnectedComponent> context;
        SinglePassRun(context, g);
        Output(context, cout);
    }
    return 0;
}
