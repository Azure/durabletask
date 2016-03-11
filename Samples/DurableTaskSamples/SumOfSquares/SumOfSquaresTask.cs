using System;

namespace DurableTaskSamples.SumOfSquares
{
    using DurableTask;

    public sealed class SumOfSquaresTask : TaskActivity<int, int>
    {
        public SumOfSquaresTask()
        {
        }

        protected override int Execute(DurableTask.TaskContext context, int chunk)
        {
            Console.WriteLine($"Square::{chunk}::{chunk * chunk}");
            return chunk * chunk;
        }
    }

}