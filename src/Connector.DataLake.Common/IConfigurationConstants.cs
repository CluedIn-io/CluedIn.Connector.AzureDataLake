﻿using CluedIn.Core.Providers;
using System;

namespace CluedIn.Connector.DataLake.Common
{
    /// <summary>
    ///     Base interface for configuration constants.
    ///     Connectors should have specific (even empty) interfaces in each implementation.
    /// </summary>
    public interface IConfigurationConstants : IExtendedProviderMetadata
    {
        Guid ProviderId { get; }
        IProviderMetadata CreateProviderMetadata();
    }
}
