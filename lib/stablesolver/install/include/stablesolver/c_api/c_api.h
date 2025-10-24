#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Export macros for shared library
#ifdef _WIN32
    #ifdef STABLESOLVER_EXPORTS
        #define STABLESOLVER_API __declspec(dllexport)
    #else
        #define STABLESOLVER_API __declspec(dllimport)
    #endif
#else
    #define STABLESOLVER_API __attribute__((visibility("default")))
#endif

// Opaque handles
typedef void* InstanceBuilderHandle;
typedef void* InstanceHandler;
typedef void* SolverHandler;


// Result structure
typedef struct {
    int is_feasible;
    int number_of_vertices;
    int *vertices;
    double solve_time;
} StableSolverResult;

// Stable set solver functions
STABLESOLVER_API InstanceBuilderHandle create_instance_builder();
STABLESOLVER_API int instance_add_vertices(InstanceBuilderHandle handle, int number_of_vertices);
STABLESOLVER_API int instance_add_vertex(InstanceBuilderHandle handle, long weight = 1);
STABLESOLVER_API int instance_set_weight(InstanceBuilderHandle handle, int vertex_id, long weight);
STABLESOLVER_API int instance_add_edge(InstanceBuilderHandle handle, int vertex_id_1,
            int vertex_id_2,
            int check_duplicate = 0);
STABLESOLVER_API int instance_set_unweighted(InstanceBuilderHandle handle);
STABLESOLVER_API InstanceHandler instance_build(InstanceBuilderHandle handle);

STABLESOLVER_API StableSolverResult solve(InstanceHandler instance, int type);
STABLESOLVER_API void cleanup_graph(InstanceBuilderHandle handle, InstanceHandler instance);
STABLESOLVER_API void cleanup_result(StableSolverResult result);



#ifdef __cplusplus
}
#endif