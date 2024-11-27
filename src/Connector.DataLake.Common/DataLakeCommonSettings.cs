using System;
using System.Collections.Generic;
using CluedIn.Core.Settings;

namespace CluedIn.Connector.DataLake.Common
{
    internal class DataLakeCommonSettings : IHasOrganizationSettings
    {
        public IEnumerable<IOrganizationSetting> GetOrganizationSettings()
        {
            yield return new EnableIncludeDataParts();
        }
    }

    internal class EnableIncludeDataParts : IOrganizationSetting3
    {
        public bool IsValid(object obj, List<OrganizationSettingValidationError> errors)
        {
            throw new NotImplementedException();
        }

        public string Key { get; set; } = "datalake.enableIncludeDataParts";
        public Type DataType { get; set; } = typeof(bool);
        public string DefaultValue { get; set; } = "false";
        public string Group { get; set; } = "Export Targets";

        public string DisplayName { get; set; } = "Enable DataPart streaming for DataLake/OneLake/Databricks/AzureAIStudio";
        public string Description { get; set; } = "Enabling sending of addition file when in sync mode that contains the data parts. WARNING this will slow down streams and consume more resource";
        public bool IsSecret { get; set; }
        public string[] FeatureFlags { get; set; }
    }
}
