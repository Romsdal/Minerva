namespace Minerva.DB_Server;

public interface IKey { }

public struct YCSBKey : IKey
{
    public int Shard;
    public string Key;

    public YCSBKey(int shard, string key)
    {
        Shard = shard;
        Key = key;
    }

    public override string ToString()
    {
        return $"[Shard: {Shard}, Key: {Key}]";
    }
}