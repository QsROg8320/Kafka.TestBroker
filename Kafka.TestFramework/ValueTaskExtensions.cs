using System.Threading.Tasks;

namespace Kafka.TestFramework
{
    internal static class ValueTaskExtensions
    {
        internal static ValueTask AsValueTask<T>(this ValueTask<T> valueTask) =>
            valueTask.IsCompletedSuccessfully ? default : new ValueTask(valueTask.AsTask());
    }
}