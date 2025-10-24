using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using StableSolver;

namespace Minerva.DB_Server.ConflictResolver;

public class StableSolverGraph : ConflictGraph
{
    private IntPtr _instanceBuilder = IntPtr.Zero;
    public IntPtr GraphInstance { get; private set;  } = IntPtr.Zero;
    public bool isBuild { get; private set; } = false;

    private bool _disposed;
    private ILogger _logger;

    public StableSolverGraph()
    {
        CheckNotDisposed();
        _instanceBuilder = StableSolverInterface.create_instance_builder();
        if (_instanceBuilder == IntPtr.Zero)
        {
            throw new InvalidOperationException("Failed to create instance builder");
        }
        _logger = LoggerManager.GetLogger();
    }

    public void AddVertices(int count)
    {
        CheckNotDisposed();

        if (count > 3900)
        {
            _logger.LogWarning("Adding a large number of vertices ({Count}) may lead to performance issues with exact solution methods.", count);
        }

        int result = StableSolverInterface.instance_add_vertices(_instanceBuilder, count);
        if (result != 0)
        {
            throw new InvalidOperationException($"Failed to add vertices, error code: {result}");
        }
    }

    public void AddVertex(long weight = 1)
    {
        CheckNotDisposed();
        int result = StableSolverInterface.instance_add_vertex(_instanceBuilder, weight);
        if (result != 0)
        {
            throw new InvalidOperationException($"Failed to add vertex, error code: {result}");
        }
    }

    public void SetWeight(int vertexId, long weight)
    {
        CheckNotDisposed();
        int result = StableSolverInterface.instance_set_weight(_instanceBuilder, vertexId, weight);
        if (result != 0)
        {
            throw new InvalidOperationException($"Failed to set weight for vertex {vertexId}, error code: {result}");
        }
    }

    public void AddEdge(int vertex1, int vertex2, bool checkDuplicate = false)
    {
        CheckNotDisposed();
        int result = StableSolverInterface.instance_add_edge(_instanceBuilder, vertex1, vertex2, checkDuplicate ? 1 : 0);
        if (result != 0)
        {
            throw new InvalidOperationException($"Failed to add edge ({vertex1}, {vertex2}), error code: {result}");
        }
    }

    public void SetUnweighted()
    {
        CheckNotDisposed();
        int result = StableSolverInterface.instance_set_unweighted(_instanceBuilder);
        if (result != 0)
        {
            throw new InvalidOperationException($"Failed to set unweighted, error code: {result}");
        }
    }

    public void Build()
    {
        CheckNotDisposed();
        GraphInstance = StableSolverInterface.instance_build(_instanceBuilder);
        if (GraphInstance == IntPtr.Zero)
        {
            throw new InvalidOperationException("Failed to build instance");
        }
        isBuild = true;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            if (_instanceBuilder != IntPtr.Zero && GraphInstance != IntPtr.Zero)
            {
                StableSolverInterface.cleanup_graph(_instanceBuilder, GraphInstance);
            }
            else if (_instanceBuilder != IntPtr.Zero)
            {   
                throw new InvalidOperationException("Attempted to dispose an instance that was not built!");
            }
        }
        _disposed = true;
        GC.SuppressFinalize(this);
    }
    

    private void CheckNotDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
    
    ~StableSolverGraph()
    {
        Dispose();
    }
}



public class StableSolver : IDisposable
{    
    private StableSolverResult _result;
    private bool solved = false;
    private bool _disposed = false;
    
    /// <summary>
    /// Types:
    /// 1 - greedy_gwmax
    /// 2 - greedy_gwmin
    /// 3 - greedy_gwmin2
    /// 4 - greedy_strong
    /// 5 - milp_1_cbc
    /// </summary>
    /// <param name="IsFeasible"></param>
    /// <param name="Vertices"></param>
    /// <param name="graph"></param>
    /// <param name="solverType"></param>
    /// <returns></returns>

    public (bool IsFeasible, int[] Vertices, double SolveTime) Solve(StableSolverGraph graph, int solverType = 1)
    {
        CheckNotDisposed();
        IntPtr graphInstance = graph.GraphInstance;

        if (graphInstance == IntPtr.Zero)
        {
            throw new InvalidOperationException("Instance not built. Call Build() first.");
        }

        _result = StableSolverInterface.solve(graphInstance, solverType);

        List<int> vertices = [];
        unsafe
        {
            if (_result.Vertices != IntPtr.Zero)
            {
                int* vertexPtr = (int*)_result.Vertices;
                for (int i = 0; i < _result.NumberOfVertices; i++)
                {
                    vertices.Add(vertexPtr[i]);
                }
            }
        }

        solved = true;
        return (_result.IsFeasible == 1, vertices.ToArray(), _result.SolveTime);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            if (solved == true && _result.Vertices != IntPtr.Zero)
            {
                StableSolverInterface.cleanup_result(_result);
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    private void CheckNotDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    ~StableSolver()
    {
        Dispose();
    }
}