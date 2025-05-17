// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;

    [TestClass]
    public class ScheduleTaskOptions2Tests
    {
        [TestMethod]
        public void CreateBuilder_ShouldReturnBuilderInstance2()
        {
            // Act
            ScheduleTaskOptions.Builder builder = ScheduleTaskOptions.CreateBuilder();

            // Assert
            Assert.IsNotNull(builder);
            Assert.IsInstanceOfType(builder, typeof(ScheduleTaskOptions.Builder));
        }
    }
}