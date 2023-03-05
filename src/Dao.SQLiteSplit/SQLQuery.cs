using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Dao.SQLiteSplit
{
    public class SQLQuery
    {
        protected static readonly ConcurrentDictionary<string, Regex> regexCache = new ConcurrentDictionary<string, Regex>(StringComparer.Ordinal);
    }

    public class SQLQuery<TParameter> : SQLQuery
    {
        public string Select { get; set; }
        public string MaxIdColumnName { get; set; }
        public string From { get; set; }
        public string Where { get; set; }
        public string OrderBy { get; set; }

        public TParameter Parameter { get; set; }

        #region ToSQL

        static Regex CreateParameterRegex(string name)
        {
            return regexCache.GetOrAdd(name, k => new Regex("\\B@" + name + "\\b", RegexOptions.Compiled));
        }

        #region Symbol

        readonly object syncSymbol = new object();
        string symbol;

        string WhereToSymbol()
        {
            if (string.IsNullOrWhiteSpace(Where) || Parameter == null || Where.IndexOf("@", StringComparison.Ordinal) < 0)
                return Where;

            var where = Parameter.GetType().GetProperties()
                .Where(w => w.CanRead && CreateParameterRegex(w.Name).IsMatch(Where))
                .Aggregate(Where, (current, pi) => CreateParameterRegex(pi.Name).Replace(current, pi.GetValue(Parameter).ToString()));
            return where;
        }

        public string ToSymbol()
        {
            if (!string.IsNullOrWhiteSpace(this.symbol))
                return this.symbol;

            lock (this.syncSymbol)
            {
                if (!string.IsNullOrWhiteSpace(this.symbol))
                    return this.symbol;

                var sb = new StringBuilder();
                sb.Append($"{nameof(From)} {From} ");
                if (!string.IsNullOrWhiteSpace(Where))
                    sb.Append($"{nameof(Where)} {WhereToSymbol()} ");

                this.symbol = sb.ToString();
                return this.symbol;
            }
        }

        #endregion

        #region CountMax

        readonly ConcurrentDictionary<bool, string> countMax = new ConcurrentDictionary<bool, string>();

        string ToCountMaxSQLCore(bool queryMax)
        {
            var sb = new StringBuilder();
            sb.Append($"{nameof(Select)} (");
            sb.Append($"{nameof(Select)} count(*) ");
            sb.Append($"{nameof(From)} {From} ");
            if (!string.IsNullOrWhiteSpace(Where))
                sb.Append($"{nameof(Where)} {Where} ");
            sb.Append(") as Count");

            if (queryMax && !string.IsNullOrWhiteSpace(MaxIdColumnName))
            {
                sb.Append(", (");
                sb.Append($"{nameof(Select)} max({MaxIdColumnName}) ");
                sb.Append($"{nameof(From)} {From} ");
                sb.Append(") as Max");
            }

            return sb.ToString();
        }

        public string ToCountMaxSQL(bool queryMax)
        {
            return this.countMax.GetOrAdd(queryMax, ToCountMaxSQLCore);
        }

        #endregion

        public string ToQuerySQL(long? maxId = null, int takeRows = 0, long skipRows = 0)
        {
            var sb = new StringBuilder();
            sb.Append($"{nameof(Select)} {Select} ");
            sb.Append($"{nameof(From)} {From} ");

            var where = Where;
            if (!string.IsNullOrWhiteSpace(MaxIdColumnName) && maxId > 0)
            {
                if (!string.IsNullOrWhiteSpace(Where))
                    where = $"({Where}) AND ";
                where += $"{MaxIdColumnName} <= {maxId}";
            }

            if (!string.IsNullOrWhiteSpace(where))
                sb.Append($"{nameof(Where)} {where} ");

            if (!string.IsNullOrWhiteSpace(OrderBy))
                sb.Append($"ORDER BY {OrderBy} ");

            if (takeRows > 0)
            {
                sb.Append($"LIMIT {takeRows} ");

                if (skipRows > 0)
                    sb.Append($"OFFSET {skipRows}");
            }

            return sb.ToString();
        }

        #endregion
    }
}