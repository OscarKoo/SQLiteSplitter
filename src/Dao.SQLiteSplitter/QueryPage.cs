using System.Collections.Generic;

namespace Dao.SQLiteSplitter
{
    public class QueryPage
    {
        public int Index { get; set; }
        public int Size { get; set; }
    }

    public class QueryResult
    {
        public long Total { get; set; }
    }

    public class QueryResult<T> : QueryResult
    {
        public List<T> Data { get; set; }
    }
}