using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace Dao.SQLiteSplit
{
    public class SQLiteSplitter
    {
        public static volatile SQLiteSettings SQLiteSettings = new SQLiteSettings();
        public static volatile SQLiteDBProvider SQLiteDBProvider;
        static readonly ISQLiteQueryProvider defaultQueryProvider = new SQLiteQueryProviderDefault();

        void CheckDependency()
        {
            if (SQLiteSettings == null)
                throw new ArgumentNullException(nameof(SQLiteSettings));

            if (SQLiteDBProvider == null)
                throw new ArgumentNullException(nameof(SQLiteDBProvider));
        }

        #region Insert

        public async Task<ICollection<TEntity>> InsertAsync<TEntity>(string sql, ICollection<TEntity> entities, Action<TEntity, long> actionUpdateInsertedId = null, DateTime? now = null)
        {
            if (entities == null || entities.Count <= 0)
                return entities;

            if (now == null)
                now = DateTime.Now;

            CheckDependency();
            SQLiteDBProvider.DeleteExpiredDBs(now.Value);
            await SQLiteDBProvider.CreateDB(now.Value).ConfigureAwait(false);

            var file = SQLiteDBProvider.GenerateDBFile(now.Value);

            Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] InsertAsync Require Lock");
            using (await SQLiteDBProvider.DBDeletionLocks.ReaderLockAsync().ConfigureAwait(false))
            {
                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] InsertAsync Got DBDeletionLocks.ReaderLockAsync");

                using (await SQLiteDBProvider.DBLocks.WriterLockAsync(file).ConfigureAwait(false))
                {
                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] InsertAsync Got DBLocks.WriterLockAsync");

                    if (actionUpdateInsertedId != null)
                        sql += ";select last_insert_rowid()";

                    var inserted = false;
                    using (var conn = new SQLiteConnection(SQLiteDBProvider.GenerateConnectionString(file)))
                    {
                        if (conn.State == ConnectionState.Closed)
                            await conn.OpenAsync().ConfigureAwait(false);
                        var tran = conn.BeginTransaction();
                        try
                        {
                            foreach (var entity in entities)
                            {
                                if (actionUpdateInsertedId != null)
                                {
                                    var lastId = await conn.ExecuteScalarAsync<long>(sql, entity).ConfigureAwait(false);
                                    inserted = true;
                                    actionUpdateInsertedId(entity, lastId);
                                }
                                else
                                {
                                    await conn.ExecuteAsync(sql, entity).ConfigureAwait(false);
                                    inserted = true;
                                }
                            }

                            tran?.Commit();
                        }
                        catch (Exception ex)
                        {
                            tran?.Rollback();
                            throw;
                        }
                        finally
                        {
                            if (inserted)
                                SQLiteDBProvider.ClearRowCountCache(file);
                        }
                    }

                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] InsertAsync Release DBLocks.WriterLockAsync");
                }

                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] InsertAsync Release DBDeletionLocks.ReaderLockAsync");
            }

            return entities;
        }

        #endregion

        #region Query

        static async Task QueryCountMax<TParameter>(ICollection<DBFileInfo> files, SQLQuery<TParameter> sql)
        {
            await files.ParallelForEachAsync(async f =>
            {
                using (await SQLiteDBProvider.DBLocks.ReaderLockAsync(f.File).ConfigureAwait(false))
                {
                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryCountMax Got DBLocks.ReaderLockAsync");

                    f.Rows = await SQLiteDBProvider.GetOrAddRowCountCache(f.File, sql.ToSymbol(), async k =>
                    {
                        using (var conn = new SQLiteConnection(SQLiteDBProvider.GenerateConnectionString(f.File)))
                        {
                            return await conn.QuerySingleAsync<CountMax>(sql.ToCountMaxSQL(), sql.Parameter).ConfigureAwait(false);
                        }
                    }).ConfigureAwait(false);

                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryCountMax Release DBLocks.ReaderLockAsync");
                }
            }).ConfigureAwait(false);
        }

        static async Task<List<TResult>> QueryAffectedDBs<TParameter, TResult>(IEnumerable<DBFileInfo<TResult>> files, SQLQuery<TParameter> query)
        {
            var affected = files.Where(w => w.IsAffected).ToList();
            await affected.ParallelForEachAsync(async f =>
            {
                using (await SQLiteDBProvider.DBLocks.ReaderLockAsync(f.File).ConfigureAwait(false))
                {
                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryAffectedDBs Got DBLocks.ReaderLockAsync");

                    using (var conn = new SQLiteConnection(SQLiteDBProvider.GenerateConnectionString(f.File)))
                    {
                        f.Data = (await conn.QueryAsync<TResult>(query.ToQuerySQL(f.Rows.Max, f.TakeRows, f.SkipRows), query.Parameter).ConfigureAwait(false)).AsList();
                    }

                    Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryAffectedDBs Release DBLocks.ReaderLockAsync");
                }
            }).ConfigureAwait(false);

            var result = affected.SelectMany(s => s.Data).ToList();
            return result;
        }

        public async Task<QueryResult<TResult>> QueryAsync<TParameter, TResult>(SQLQuery<TParameter> query, QueryPage page, ISQLiteQueryProvider queryProvider = null)
        {
            var now = DateTime.Now.Date;

            CheckDependency();
            SQLiteDBProvider.DeleteExpiredDBs(now);

            if (queryProvider == null)
                queryProvider = defaultQueryProvider;

            Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryAsync Require Lock");
            using (await SQLiteDBProvider.DBDeletionLocks.ReaderLockAsync().ConfigureAwait(false))
            {
                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryAsync Got DBDeletionLocks.ReaderLockAsync");

                var files = SQLiteDBProvider.GetDBs(now, false, queryProvider.GetDBsOrderBy).Select(s => new DBFileInfo<TResult>(s)).ToList();

                await QueryCountMax(files.Cast<DBFileInfo>().ToList(), query).ConfigureAwait(false);

                queryProvider.FindAffectedDBs(files, page);

                var data = await QueryAffectedDBs(files, query).ConfigureAwait(false);

                var result = new QueryResult<TResult>
                {
                    Data = data,
                    Total = files.Sum(s => s.Rows.Count)
                };

                Debug.WriteLine($"[{DateTime.Now:HH:mm:ss.fffffff} ({Thread.CurrentThread.ManagedThreadId})] QueryAsync Release DBDeletionLocks.ReaderLockAsync");
                return result;
            }
        }

        #endregion
    }
}