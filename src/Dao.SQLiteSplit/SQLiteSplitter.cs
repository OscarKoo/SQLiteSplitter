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

        #region Execute

        public async Task ExecuteAsync<TEntity>(string sql, ICollection<TEntity> entities, Action<TEntity, long> actionUpdateInsertedId = null, DateTime? now = null)
        {
            if (entities == null || entities.Count <= 0)
                return;

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
                        var tran = entities.Count > 1 ? conn.BeginTransaction() : null;
                        try
                        {
                            foreach (var entity in entities)
                            {
                                if (actionUpdateInsertedId != null)
                                {
                                    var lastId = await conn.ExecuteScalarAsync<long>(sql, entity, tran).ConfigureAwait(false);
                                    inserted = true;
                                    actionUpdateInsertedId(entity, lastId);
                                }
                                else
                                {
                                    await conn.ExecuteAsync(sql, entity, tran).ConfigureAwait(false);
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
        }

        public async Task ExecuteAsync<TEntity>(string sql, TEntity entity, Action<TEntity, long> actionUpdateInsertedId = null, DateTime? now = null)
        {
            await ExecuteAsync(sql, new[] { entity }, actionUpdateInsertedId, now).ConfigureAwait(false);
        }

        public async Task ExecuteAsync(string sql, DateTime? now = null)
        {
            await ExecuteAsync(sql, (object)null, null, now).ConfigureAwait(false);
        }

        public void Execute<TEntity>(string sql, ICollection<TEntity> entities, Action<TEntity, long> actionUpdateInsertedId = null, DateTime? now = null)
        {
            ExecuteAsync(sql, entities, actionUpdateInsertedId, now).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Execute<TEntity>(string sql, TEntity entity, Action<TEntity, long> actionUpdateInsertedId = null, DateTime? now = null)
        {
            ExecuteAsync(sql, entity, actionUpdateInsertedId, now).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Execute(string sql, DateTime? now = null)
        {
            ExecuteAsync(sql, now).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        #endregion

        #region Query

        static async Task QueryCountMax<TParameter>(ICollection<DBFileInfo> files, SQLQuery<TParameter> sql)
        {
            await files.ParallelForEachAsync(async f =>
            {
                try
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
                }
                catch (Exception ex)
                {
                    throw new Exception($"Counting ({f.File}) failed", ex);
                }
            }).ConfigureAwait(false);
        }

        static async Task<List<TResult>> QueryAffectedDBs<TParameter, TResult>(IEnumerable<DBFileInfo<TResult>> files, SQLQuery<TParameter> query)
        {
            var affected = files.Where(w => w.IsAffected).ToList();
            await affected.ParallelForEachAsync(async f =>
            {
                try
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
                }
                catch (Exception ex)
                {
                    throw new Exception($"Querying ({f.File}) failed", ex);
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

        public QueryResult<TResult> Query<TParameter, TResult>(SQLQuery<TParameter> query, QueryPage page, ISQLiteQueryProvider queryProvider = null)
        {
            return QueryAsync<TParameter, TResult>(query, page, queryProvider).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        #endregion
    }
}