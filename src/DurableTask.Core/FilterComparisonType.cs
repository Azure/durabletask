using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Core
{
    /// <summary>
    /// The kind of comparison to be performed in the State Filter.
    /// </summary>
    public enum FilterComparisonType
    {
        /// <summary>
        /// Equality Comparison
        /// </summary>
        Equals = 0,

        /// <summary>
        /// In-Equality Comparison
        /// </summary>
        NotEquals = 1
    }
}
