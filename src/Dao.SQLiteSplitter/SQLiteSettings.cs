namespace Dao.SQLiteSplitter
{
    public class SQLiteSettings
    {
        public volatile string Folder = "sqlite";
        public volatile int KeepDays = -1;
        public volatile DeleteMode DeleteMode;
    }

    public enum DeleteMode
    {
        Logical = 0,
        Physical = 1
    }

    public enum OrderBy
    {
        Ascending = 0,
        Descending = 1
    }
}