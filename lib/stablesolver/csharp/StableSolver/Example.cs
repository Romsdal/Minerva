using StableSolver;
using System;
using System.Runtime.InteropServices;

public class SolverExample
{
    // Add <OutputType>Exe</OutputType> in .csproj to run the example as a console application
    public static void Main(string[] args)
    {
        Console.WriteLine("Running low-level example...");
        RunLowLevelExample();
    }

    private static void RunLowLevelExample()
    {
        try
        {
            // Create an instance builder
            IntPtr instanceBuilder = StableSolverInterface.create_instance_builder();
            if (instanceBuilder == IntPtr.Zero)
            {
                Console.WriteLine("Failed to create instance builder");
                return;
            }

            Console.WriteLine("Instance builder created successfully");

            // Create a simple graph with 4 vertices
            // Graph structure:
            //   0 --- 1
            //   |     |
            //   2 --- 3

            // Add 4 vertices
            int result = StableSolverInterface.instance_add_vertices(instanceBuilder, 4);
            if (result != 0)
            {
                Console.WriteLine($"Failed to add vertices, error code: {result}");
                return;
            }
            Console.WriteLine("Added 4 vertices");

            // Set weights for vertices (optional, default is 1)
            StableSolverInterface.instance_set_weight(instanceBuilder, 0, 10);
            StableSolverInterface.instance_set_weight(instanceBuilder, 1, 20);
            StableSolverInterface.instance_set_weight(instanceBuilder, 2, 15);
            StableSolverInterface.instance_set_weight(instanceBuilder, 3, 25);
            Console.WriteLine("Set vertex weights: v0=10, v1=20, v2=15, v3=25");

            // Add edges to form a 4-cycle
            StableSolverInterface.instance_add_edge(instanceBuilder, 0, 1, 0);
            StableSolverInterface.instance_add_edge(instanceBuilder, 1, 3, 0);
            StableSolverInterface.instance_add_edge(instanceBuilder, 3, 2, 0);
            StableSolverInterface.instance_add_edge(instanceBuilder, 2, 0, 0);
            Console.WriteLine("Added edges: (0,1), (1,3), (3,2), (2,0)");

            // Build the instance
            IntPtr instance = StableSolverInterface.instance_build(instanceBuilder);
            if (instance == IntPtr.Zero)
            {
                Console.WriteLine("Failed to build instance");
                return;
            }
            Console.WriteLine("Instance built successfully");

            // Solve the stable set problem
            Console.WriteLine("Solving stable set problem...");
            StableSolverResult result_struct = StableSolverInterface.solve(instance, 1);

            Console.WriteLine($"Solution found in {result_struct.SolveTime:F3} seconds");

            if (result_struct.IsFeasible == 1)
            {
                Console.WriteLine("Feasible solution found!");

                // Note: In a real implementation, you would need to know the size of the solution
                // or have the C API return the size. For this example, we'll assume a maximum
                // stable set size and check for valid vertex IDs.
                Console.WriteLine("Stable set vertices: ");

                // This is a simplified way to read the result - in practice you'd need
                // the actual size of the solution from the C API
                unsafe
                {
                    int* vertices = (int*)result_struct.Vertices;
                    if (vertices != null)
                    {
                        // Read vertices until we hit a sentinel value or invalid vertex
                        // This assumes the C API null-terminates or uses -1 as sentinel
                        for (int i = 0; i < result_struct.NumberOfVertices; i++)
                        {
                            int vertex = vertices[i];
                            if (vertex < 0) break; // Assuming -1 is sentinel
                            Console.WriteLine($"  Vertex {vertex}");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("No feasible solution found.");
            }

            // Cleanup resources
            StableSolverInterface.cleanup_graph(instanceBuilder, instance);
            StableSolverInterface.cleanup_result(result_struct);
            Console.WriteLine("Cleanup completed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}