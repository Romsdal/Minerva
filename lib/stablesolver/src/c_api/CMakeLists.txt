# Create shared library for C API
add_library(StableSolver_c_api SHARED
    c_api.cpp)

# Set library properties
set_target_properties(StableSolver_c_api PROPERTIES
    OUTPUT_NAME "stablesolver"
    VERSION 1.0.0
    SOVERSION 1
    PUBLIC_HEADER "${PROJECT_SOURCE_DIR}/include/stablesolver/c_api/c_api.h")

# Include directories
target_include_directories(StableSolver_c_api PUBLIC
    ${PROJECT_SOURCE_DIR}/include)

# Link to stable solver libraries
target_link_libraries(StableSolver_c_api PUBLIC
    StableSolver_stable
    StableSolver_stable_greedy
    StableSolver_stable_local_search)

# Add optional solver dependencies if enabled
if(STABLESOLVER_USE_CBC)
    target_link_libraries(StableSolver_c_api PUBLIC
        StableSolver_stable_milp_cbc)
endif()

if(STABLESOLVER_USE_CPLEX)
    target_link_libraries(StableSolver_c_api PUBLIC
        StableSolver_stable_milp_cplex)
endif()

# Define export macro for Windows DLL
target_compile_definitions(StableSolver_c_api PRIVATE 
    STABLESOLVER_EXPORTS)

# Set symbol visibility
set_target_properties(StableSolver_c_api PROPERTIES
    C_VISIBILITY_PRESET hidden
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES)

# Create alias for consistent naming
add_library(StableSolver::c_api ALIAS StableSolver_c_api)

# Install targets
install(TARGETS StableSolver_c_api
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
    PUBLIC_HEADER DESTINATION include/stablesolver/c_api)