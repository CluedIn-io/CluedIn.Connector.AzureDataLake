namespace CluedIn.Connector.Snowflake.Connector;

// The transient/landing table's column shape. Kept schema-independent (a single VARIANT
// payload column) rather than one column per CluedIn property, because the target table's
// real schema is user-managed and not known at DDL time - see docs/snowflake-connector-plan.md.
internal static class TransientTableColumns
{
    public const string EntityId = "ENTITY_ID";
    public const string ChangeType = "CHANGE_TYPE";
    public const string PersistVersion = "PERSIST_VERSION";
    public const string RowData = "ROW_DATA";
}
