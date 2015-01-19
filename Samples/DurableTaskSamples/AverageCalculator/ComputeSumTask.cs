namespace DurableTaskSamples.AverageCalculator
{
    using System;
    using DurableTask;

    public sealed class ComputeSumTask : TaskActivity<int[], int>
    {
        public ComputeSumTask()
        {
        }

        protected override int Execute(DurableTask.TaskContext context, int[] chunk)
        {
            if (chunk == null || chunk.Length != 2)
            {
                throw new ArgumentException("chunk");
            }

            Console.WriteLine("Compute Sum for " + chunk[0] + "," + chunk[1]);
            int sum = 0;
            int start = chunk[0];
            int end = chunk[1];
            for (int i = start; i <= end; i++)
            {
                sum += i;
            }

            Console.WriteLine("Total Sum for Chunk '" + chunk[0] + "," + chunk[1] + "' is " + sum.ToString());

            return sum;
        }
    }

}
