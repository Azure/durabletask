using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Core
{
    using System.Linq;

    /// <summary>
    /// Extension methods for Stack
    /// </summary>
    public static class StackExtensions
    {
        /// <summary>
        /// Clone the Stack instance with the right order.
        /// </summary>
        /// <typeparam name="T">Type of the Stack</typeparam>
        /// <param name="original">Stack instance</param>
        /// <returns></returns>
        public static Stack<T> Clone<T>(this Stack<T> original)
        {
            return new Stack<T>(original.Reverse());
        }
    }
}
