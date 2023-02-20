using System.Collections.Generic;

namespace Dao.SQLiteSplit
{
    public class DBFileInfo
    {
        public DBFileInfo() { }

        public DBFileInfo(string file)
        {
            File = file;
        }

        public string File { get; set; }

        public CountMax Rows { get; set; }

        public long SkipRows { get; set; }
        public int TakeRows { get; set; }
        public bool IsAffected => Rows?.Count > 0 && TakeRows > 0;
    }

    public class DBFileInfo<TResult> : DBFileInfo
    {
        public DBFileInfo() : base() { }

        public DBFileInfo(string file) : base(file) { }

        public List<TResult> Data { get; set; }
    }

    public class CountMax
    {
        public long Count { get; set; }
        public long? Max { get; set; }
    }
}