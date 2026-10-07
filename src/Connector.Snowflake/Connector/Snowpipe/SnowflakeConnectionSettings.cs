namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal record SnowflakeConnectionSettings(
    string Account,
    string User,
    string PrivateKeyPem,
    string PrivateKeyPassphrase,
    string Database,
    string Schema,
    string Warehouse,
    string Role)
{
    public static SnowflakeConnectionSettings FromConfiguration(SnowflakeConnectorConfiguration configuration)
    {
        return new SnowflakeConnectionSettings(
            configuration.Account,
            configuration.User,
            configuration.PrivateKey,
            configuration.PrivateKeyPassphrase,
            configuration.Database,
            configuration.Schema,
            configuration.Warehouse,
            configuration.Role);
    }
}
