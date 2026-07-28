using System.Collections.Generic;

using Xunit;

namespace CluedIn.Connector.FabricOpenMirroring.Tests.Unit;

public class OpenMirroringConnectorConfigurationTests
{
    [Fact]
    public void WorkspaceName_ShouldReturnTrimmedValue()
    {
        var configurations = new Dictionary<string, object>
        {
            { OpenMirroringConfigurationConstants.WorkspaceName, "  MyWorkspace  " },
        };

        var sut = new OpenMirroringConnectorConfiguration(configurations);

        Assert.Equal("MyWorkspace", sut.WorkspaceName);
    }

    [Fact]
    public void WorkspaceName_ShouldReturnNull_WhenNotProvided()
    {
        var configurations = new Dictionary<string, object>();

        var sut = new OpenMirroringConnectorConfiguration(configurations);

        Assert.Null(sut.WorkspaceName);
    }

    [Fact]
    public void MirroredDatabaseName_ShouldReturnTrimmedValue()
    {
        var configurations = new Dictionary<string, object>
        {
            { OpenMirroringConfigurationConstants.MirroredDatabaseName, "  MyDatabase  " },
        };

        var sut = new OpenMirroringConnectorConfiguration(configurations);

        Assert.Equal("MyDatabase", sut.MirroredDatabaseName);
    }

    [Fact]
    public void MirroredDatabaseName_ShouldReturnEmptyString_WhenNotProvided()
    {
        var configurations = new Dictionary<string, object>();

        var sut = new OpenMirroringConnectorConfiguration(configurations);

        Assert.Equal(string.Empty, sut.MirroredDatabaseName);
    }
}
