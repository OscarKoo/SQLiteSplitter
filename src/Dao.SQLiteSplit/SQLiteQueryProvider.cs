using System.Collections.Generic;

namespace Dao.SQLiteSplit
{
    public interface ISQLiteQueryProvider
    {
        OrderBy GetDBsOrderBy { get; }

        void FindAffectedDBs(IEnumerable<DBFileInfo> files, QueryPage page);
    }

    public class SQLiteQueryProviderDefault : ISQLiteQueryProvider
    {
        public OrderBy GetDBsOrderBy => OrderBy.Descending;

        public void FindAffectedDBs(IEnumerable<DBFileInfo> files, QueryPage page)
        {
            long skip = (page.Index - 1) * page.Size;
            var size = page.Size;
            long total = 0;
            var foundStart = false;

            foreach (var f in files)
            {
                total += f.Rows.Count;
                if (!foundStart && total > skip)
                {
                    f.SkipRows = f.Rows.Count - (total - skip);
                    foundStart = true;
                }

                if (foundStart)
                {
                    var remain = f.Rows.Count - f.SkipRows;
                    if (remain < size)
                    {
                        f.TakeRows = (int)remain;
                        size -= f.TakeRows;
                    }
                    else
                    {
                        f.TakeRows = size;
                        break;
                    }
                }
            }
        }
    }
}