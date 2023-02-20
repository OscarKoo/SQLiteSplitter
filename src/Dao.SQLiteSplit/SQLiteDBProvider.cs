using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dao.ConcurrentDictionaryLazy;
using Dao.IndividualReadWriteLocks;
using Nito.AsyncEx;

namespace Dao.SQLiteSplit
{
    public abstract class SQLiteDBProvider
    {
        public readonly AsyncReaderWriterLock DBDeletionLocks = new AsyncReaderWriterLock();
        public readonly IndividualReadWriteLocks<string> DBLocks = new IndividualReadWriteLocks<string>(StringComparer.OrdinalIgnoreCase);

        #region RowCountCache

        protected readonly ConcurrentDictionary<string, ConcurrentDictionaryLazy<string, CountMax>> rowCountCache = new ConcurrentDictionary<string, ConcurrentDictionaryLazy<string, CountMax>>(StringComparer.OrdinalIgnoreCase);

        public async Task<CountMax> GetOrAddRowCountCache(string file, string key, Func<string, Task<CountMax>> queryCount)
        {
            return await this.rowCountCache
                .GetOrAdd(file, k => new ConcurrentDictionaryLazy<string, CountMax>(StringComparer.OrdinalIgnoreCase))
                .GetOrAddAsync(key, async k => await queryCount(k).ConfigureAwait(false)).ConfigureAwait(false);
        }

        void ClearRowCountCache()
        {
            if (this.rowCountCache.Count <= 0)
                return;

            var caches = this.rowCountCache.Values;
            Parallel.ForEach(caches, c => c.Clear());
            this.rowCountCache.Clear();
        }

        public void ClearRowCountCache(string file)
        {
            if (this.rowCountCache.TryRemove(file, out var cache))
                cache.Clear();
        }

        #endregion

        protected abstract string GenerateDBFileName(DateTime now);

        public string GenerateDBFile(DateTime now)
        {
            return Path.Combine(SQLiteSplitter.SQLiteSettings.Folder, GenerateDBFileName(now));
        }

        public abstract string GenerateConnectionString(string file);

        #region CreateDB

        public abstract Task CreateDBSchema(IDbConnection connection);

        public async Task CreateDB(DateTime now)
        {
            var file = GenerateDBFile(now);

            Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] CreateDB Require Lock");
            using (await this.DBDeletionLocks.ReaderLockAsync().ConfigureAwait(false))
            {
                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] CreateDB Got DBDeletionLocks.ReaderLock");

                using (await this.DBLocks.WriterLockAsync(file).ConfigureAwait(false))
                {
                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] CreateDB Got DBLocks.WriterLock");

                    if (!File.Exists(file))
                    {
                        var dir = Path.GetDirectoryName(file);
                        if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
                            Directory.CreateDirectory(dir);

                        try
                        {
                            using (File.Create(file)) { }

                            using (var conn = new SQLiteConnection(GenerateConnectionString(file)))
                            {
                                await CreateDBSchema(conn).ConfigureAwait(false);
                            }
                        }
                        catch (Exception ex)
                        {
                            if (File.Exists(file))
                                File.Delete(file);
                            throw;
                        }
                    }

                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] CreateDB Release DBLocks.WriterLock");
                }

                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] CreateDB Release DBDeletionLocks.ReaderLock");
            }
        }

        #endregion

        public abstract bool ShouldDeleteDBFile(string file, DateTime beforeDate);

        #region DeleteDBs

        DateTime lastDeleted;

        bool RequireDeletion(DateTime now)
        {
            return this.lastDeleted.Date < now.Date;
        }

        static DateTime CalculateBeforeDate(DateTime now)
        {
            return now.AddDays(-SQLiteSplitter.SQLiteSettings.KeepDays).Date;
        }

        public virtual IEnumerable<string> GetDBs(DateTime now, bool showAll, OrderBy? orderBy = null)
        {
            if (!Directory.Exists(SQLiteSplitter.SQLiteSettings.Folder))
                return Enumerable.Empty<string>();

            var files = Directory.EnumerateFiles(SQLiteSplitter.SQLiteSettings.Folder, "*.db");

            if (!showAll && SQLiteSplitter.SQLiteSettings.KeepDays >= 0)
            {
                var beforeDate = CalculateBeforeDate(now);
                files = files.Where(w => !ShouldDeleteDBFile(w, beforeDate));
            }

            if (orderBy != null)
            {
                switch (orderBy)
                {
                    case OrderBy.Ascending:
                        files = files.OrderBy(o => o, StringComparer.OrdinalIgnoreCase);
                        break;
                    case OrderBy.Descending:
                        files = files.OrderByDescending(o => o, StringComparer.OrdinalIgnoreCase);
                        break;
                }
            }

            return files;
        }

        public void DeleteExpiredDBs(DateTime now)
        {
            if (SQLiteSplitter.SQLiteSettings.KeepDays < 0)
                return;

            if (this.lastDeleted.Date >= now.Date)
                return;

            lock (this.DBDeletionLocks)
            {
                if (this.lastDeleted.Date >= now.Date)
                    return;

                try
                {
                    var beforeDate = CalculateBeforeDate(now);

                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] DeleteExpiredDBs Require Lock");
                    using (this.DBDeletionLocks.WriterLock())
                    {
                        Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] DeleteExpiredDBs Got DBDeletionLocks.WriterLock");

                        ClearRowCountCache();

                        if (SQLiteSplitter.SQLiteSettings.DeleteMode == DeleteMode.Physical)
                            Parallel.ForEach(GetDBs(now, true).Where(w => ShouldDeleteDBFile(w, beforeDate)), File.Delete);

                        Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] DeleteExpiredDBs Release DBDeletionLocks.WriterLock");
                    }
                }
                finally
                {
                    this.lastDeleted = now.Date;
                }
            }
        }

        #endregion
    }
}