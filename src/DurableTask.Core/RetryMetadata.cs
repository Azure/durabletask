// Copyright (c) Microsoft Corporation. All rights reserved.

#nullable enable
namespace DurableTask.Core
{
    using System;
    using System.Runtime.Serialization;
    using Newtonsoft.Json;

    /// <summary>
    /// Identifies an activity attempt scheduled by a durable retry policy.
    /// </summary>
    [DataContract]
    public sealed class RetryMetadata
    {
        /// <summary>
        /// Initializes validated metadata for an activity attempt.
        /// </summary>
        /// <param name="attemptNumber">The one-based durable attempt number.</param>
        /// <param name="maxAttempts">The maximum number of attempts, including the initial attempt.</param>
        /// <param name="retryGroupId">The replay-stable identifier shared by all attempts of the logical activity.</param>
        [JsonConstructor]
        public RetryMetadata(int attemptNumber, int maxAttempts, string retryGroupId)
        {
            this.AttemptNumber = attemptNumber;
            this.MaxAttempts = maxAttempts;
            this.RetryGroupId = retryGroupId;
            this.Validate();
        }

        /// <summary>
        /// Gets the one-based durable attempt number, independent of message redelivery.
        /// </summary>
        [DataMember]
        public int AttemptNumber { get; private set; }

        /// <summary>
        /// Gets the policy's maximum number of attempts, including the initial attempt.
        /// </summary>
        [DataMember]
        public int MaxAttempts { get; private set; }

        /// <summary>
        /// Gets the identifier shared by attempts of the logical activity across replay.
        /// </summary>
        [DataMember]
        public string RetryGroupId { get; private set; }

        [OnDeserialized]
        void OnDeserialized(StreamingContext context) => this.Validate();

        void Validate()
        {
            if (this.MaxAttempts <= 0)
            {
                throw new ArgumentOutOfRangeException("maxAttempts", "The maximum attempt count must be positive.");
            }

            if (this.AttemptNumber <= 0 || this.AttemptNumber > this.MaxAttempts)
            {
                throw new ArgumentOutOfRangeException("attemptNumber", "The attempt must be between one and the maximum attempt count.");
            }

            if (string.IsNullOrWhiteSpace(this.RetryGroupId))
            {
                throw new ArgumentException("A retry group identifier is required.", "retryGroupId");
            }
        }
    }
}
