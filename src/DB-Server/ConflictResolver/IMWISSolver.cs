using System;
using System.Collections.Generic;
using StableSolver;

namespace Minerva.DB_Server.ConflictResolver;

// This is to match the interface provided by StableSolver
public interface ConflictGraph : IDisposable 
{
    bool isBuild { get; }

    void AddVertex(long weight = 1);
    void AddVertices(int count);
    void AddEdge(int source, int destination, bool checkDuplicate);
    void SetWeight(int vertexId, long weight);
    void Build();
}