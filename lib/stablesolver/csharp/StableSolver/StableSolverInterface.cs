using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace StableSolver;

// Struct to match the C API result structure
[StructLayout(LayoutKind.Sequential)]
public struct StableSolverResult
{
    public int IsFeasible;
    public int NumberOfVertices;
    public IntPtr Vertices;  // int* in C
    public double SolveTime;
}

public static partial class StableSolverInterface
{
    private const string LibraryName = "libstablesolver.so.1.0.0";

    // Instance builder functions
    [LibraryImport(LibraryName)]
    public static partial IntPtr create_instance_builder();

    [LibraryImport(LibraryName)]
    public static partial int instance_add_vertices(IntPtr handle, int number_of_vertices);

    [LibraryImport(LibraryName)]
    public static partial int instance_add_vertex(IntPtr handle, long weight = 1);

    [LibraryImport(LibraryName)]
    public static partial int instance_set_weight(IntPtr handle, int vertex_id, long weight);

    /// <summary>
    /// Adds an edge between two vertices in the instance.
    /// </summary>
    /// <param name="handle"></param>
    /// <param name="vertex_id_1"></param>
    /// <param name="vertex_id_2"></param>
    /// <param name="check_duplicate"> 0 for no check at all; 1 for check and do nothing if there is duplicate; 2 for throw error if there is duplicate</param>
    /// <returns></returns>
    [LibraryImport(LibraryName)]
    public static partial int instance_add_edge(IntPtr handle, int vertex_id_1, int vertex_id_2, int check_duplicate = 0);

    [LibraryImport(LibraryName)]
    public static partial int instance_set_unweighted(IntPtr handle);

    [LibraryImport(LibraryName)]
    public static partial IntPtr instance_build(IntPtr handle);

    // Solver functions
    [LibraryImport(LibraryName)]
    public static partial StableSolverResult solve(IntPtr instance, int type);

    [LibraryImport(LibraryName)]
    public static partial void cleanup_graph(IntPtr handle, IntPtr instance);

    [LibraryImport(LibraryName)]
    public static partial void cleanup_result(StableSolverResult result);
}


