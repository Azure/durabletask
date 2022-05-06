// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core
{
    using DurableTask.Core.Serializing;

    internal interface IDataConverterConfigurable
    {
        DataConverter DataConverter { set; }
    }
}