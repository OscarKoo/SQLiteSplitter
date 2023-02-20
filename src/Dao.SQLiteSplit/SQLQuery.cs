using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Dao.SQLiteSplit
{
    public class SQLQuery
    {
        protected static readonly ConcurrentDictionary<Type, PropertyInfo[]> propertyInfos = new ConcurrentDictionary<Type, PropertyInfo[]>();
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

            var pis = propertyInfos.GetOrAdd(Parameter.GetType(), k => k.GetProperties());

            var where = pis
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

        readonly object syncCountMax = new object();
        string countMax;

        public string ToCountMaxSQL()
        {
            if (!string.IsNullOrWhiteSpace(this.countMax))
                return this.countMax;

            lock (this.syncCountMax)
            {
                if (!string.IsNullOrWhiteSpace(this.countMax))
                    return this.countMax;

                var sb = new StringBuilder();

                var select = new List<string>();
                select.Add("count(*) as Count");
                if (!string.IsNullOrWhiteSpace(MaxIdColumnName))
                    select.Add($"max({MaxIdColumnName}) as Max");

                sb.Append($"{nameof(Select)} {string.Join(", ", select)} ");
                sb.Append($"{nameof(From)} {From} ");
                if (!string.IsNullOrWhiteSpace(Where))
                    sb.Append($"{nameof(Where)} {Where} ");

                this.countMax = sb.ToString();
                return this.countMax;
            }
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

            sb.Append($"{nameof(Where)} {where} ");
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