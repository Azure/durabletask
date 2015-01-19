//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTaskSamplesUnitTests
{
    using System;
    using DurableTask;

    class MockObjectCreator<T> : ObjectCreator<T>
    {
        Func<T> creator;

        public MockObjectCreator(string name, string version, Func<T> creator)
        {
            this.Name = name;
            this.Version = version;
            this.creator = creator;
        }

        public override T Create()
        {
            return this.creator();
        }
    }
}


