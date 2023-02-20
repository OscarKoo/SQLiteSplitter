using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Dao.SQLiteSplitter
{
    public static class Extensions
    {
        public static Task ParallelForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body, int maxDegreeOfParallelism = DataflowBlockOptions.Unbounded, TaskScheduler scheduler = null)
        {
            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };
            if (scheduler != null)
                options.TaskScheduler = scheduler;

            var block = new ActionBlock<T>(body, options);

            foreach (var item in source)
                block.Post(item);

            block.Complete();
            return block.Completion;
        }

        public static async Task ParallelForEachAsync<TSource>(this ICollection<TSource> items, Func<TSource, Task> action, int maxDegreeOfParallelism = DataflowBlockOptions.Unbounded, TaskScheduler scheduler = null)
        {
            switch (items.Count)
            {
                case 0: return;
                case 1:
                    await action(items.First()).ConfigureAwait(false);
                    return;
                default:
                    await ParallelForEachAsync((IEnumerable<TSource>)items, action, maxDegreeOfParallelism).ConfigureAwait(false);
                    break;
            }
        }
    }
}