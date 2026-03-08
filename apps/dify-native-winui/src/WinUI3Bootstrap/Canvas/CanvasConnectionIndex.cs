namespace AIWF.Native.CanvasRuntime;

internal sealed class CanvasConnectionIndex<TNode, TEdge>
    where TNode : notnull
    where TEdge : notnull
{
    private readonly Dictionary<TNode, HashSet<TEdge>> _index;

    public CanvasConnectionIndex()
        : this(null)
    {
    }

    public CanvasConnectionIndex(IEqualityComparer<TNode>? comparer)
    {
        _index = new Dictionary<TNode, HashSet<TEdge>>(comparer);
    }

    public void Add(TNode source, TNode target, TEdge edge)
    {
        AddOne(source, edge);
        if (!EqualityComparer<TNode>.Default.Equals(source, target))
        {
            AddOne(target, edge);
        }
    }

    public void Remove(TNode source, TNode target, TEdge edge)
    {
        RemoveOne(source, edge);
        if (!EqualityComparer<TNode>.Default.Equals(source, target))
        {
            RemoveOne(target, edge);
        }
    }

    public IReadOnlyCollection<TEdge> Get(TNode node)
    {
        return _index.TryGetValue(node, out var edges)
            ? edges
            : Array.Empty<TEdge>();
    }

    public void Clear()
    {
        _index.Clear();
    }

    private void AddOne(TNode node, TEdge edge)
    {
        if (!_index.TryGetValue(node, out var edges))
        {
            edges = new HashSet<TEdge>();
            _index[node] = edges;
        }

        edges.Add(edge);
    }

    private void RemoveOne(TNode node, TEdge edge)
    {
        if (!_index.TryGetValue(node, out var edges))
        {
            return;
        }

        edges.Remove(edge);
        if (edges.Count == 0)
        {
            _index.Remove(node);
        }
    }
}
