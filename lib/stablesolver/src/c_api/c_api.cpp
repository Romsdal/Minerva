#include "stablesolver/c_api/c_api.h"
#include "stablesolver/stable/instance_builder.hpp"
#include "stablesolver/stable/algorithms/greedy.hpp"
#include "stablesolver/stable/algorithms/local_search.hpp"
#include "stablesolver/stable/algorithms/milp_cbc.hpp"

#include <memory>
#include <vector>
#include <string>

using namespace stablesolver;

StableSolverResult parse_result(const stable::Output& output);
void read_args(stable::Parameters& parameters);

extern "C" {

// Stable set solver implementation
InstanceBuilderHandle create_instance_builder() {
    try {
        return new stable::InstanceBuilder();
        
    } catch (...) {
        return nullptr;
    }
}

int instance_add_vertices(InstanceBuilderHandle handle, int number_of_vertices) {
    if (!handle) return -1;
    try {
        static_cast<stable::InstanceBuilder*>(handle)->add_vertices(number_of_vertices);
        return 0;
    } catch (...) {
        return -1;
    }
}

int instance_add_vertex(InstanceBuilderHandle handle, long weight) {
    if (!handle) return -1;
    try {
        static_cast<stable::InstanceBuilder*>(handle)->add_vertex(weight);
        return 0;
    } catch (...) {
        return -1;
    }
}

int instance_set_weight(InstanceBuilderHandle handle, int vertex_id, long weight) {
    if (!handle) return -1;
    try {
        static_cast<stable::InstanceBuilder*>(handle)->set_weight(vertex_id, weight);
        return 0;
    } catch (...) {
        return -1;
    }
}

int instance_add_edge(InstanceBuilderHandle handle, int vertex_id_1,
            int vertex_id_2,
            int check_duplicate) {
    if (!handle) return -1;
    try {
        static_cast<stable::InstanceBuilder*>(handle)->add_edge(
            vertex_id_1,
            vertex_id_2,
            check_duplicate);
        return 0;
    } catch (...) {
        return -1;
    }
}

int instance_set_unweighted(InstanceBuilderHandle handle) {
    if (!handle) return -1;
    try {
        static_cast<stable::InstanceBuilder*>(handle)->set_unweighted();
        return 0;
    } catch (...) {
        return -1;
    }
}

InstanceHandler instance_build(InstanceBuilderHandle handle) {
    if (!handle) return nullptr;
    try {
        stable::Instance instance = static_cast<stable::InstanceBuilder*>(handle)->build();
        return new stable::Instance(std::move(instance));
    } catch (...) {
        return nullptr;
    }
}

StableSolverResult solve(InstanceHandler instance, int type) {

    // convert instance to stable::Instance pointer
    stable::Instance* instance_ptr = static_cast<stable::Instance*>(instance);


    if (type == 1 || type == 2 || type == 3 || type == 4) {
        stable::GreedyParameters parameters;
        parameters.timer.set_sigint_handler();
        parameters.messages_to_stdout = false;

        if (type == 1) {
            auto result = stable::greedy_gwmin(*instance_ptr, parameters);
            return parse_result(result);
        } else if (type == 2) {
            auto result = stable::greedy_gwmax(*instance_ptr, parameters);
            return parse_result(result);
        } else if (type == 3) {
            auto result = stable::greedy_gwmin2(*instance_ptr, parameters);
            return parse_result(result);
        } else if (type == 4) {
            auto result = stable::greedy_strong(*instance_ptr, parameters);
            return parse_result(result);
        }
        
    } else if (type == 5) {
        stable::MilpCbcParameters parameters;
        parameters.timer.set_sigint_handler();
        parameters.messages_to_stdout = false;

        auto result = stable::milp_1_cbc(*instance_ptr, parameters);
        return parse_result(result);
    } 

    return {0, 0, nullptr, 0.0};

}

void cleanup_graph(InstanceBuilderHandle handle, InstanceHandler instance) {
    delete static_cast<stable::InstanceBuilder*>(handle);
    delete static_cast<stable::Instance*>(instance);
}

void cleanup_result(StableSolverResult result) {
    delete[] result.vertices; // Free the vertices array
    result.vertices = nullptr; // Avoid dangling pointer

} 

}// extern "C"

StableSolverResult parse_result(const stable::Output& output) {
    StableSolverResult result;
    result.is_feasible = output.solution.feasible() ? 1 : 0;
    result.solve_time = output.time;
    result.number_of_vertices = output.solution.number_of_vertices();
    result.vertices = new int[result.number_of_vertices];
    int i = 0;
    for (int v : output.solution.vertices()) {
        result.vertices[i] = v; // Mark vertex as included in the solution
        i++;
    }
    return result;
}