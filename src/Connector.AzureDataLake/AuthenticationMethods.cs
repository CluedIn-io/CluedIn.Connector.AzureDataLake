using System.ComponentModel.DataAnnotations;

namespace CluedIn.Connector.AzureDataLake;

internal enum AuthenticationMethods
{
    [Display(Name = "Access Key or Shared Access Signature Token")]
    AccessKeyOrSasToken = 1,

    [Display(Name = "Service Principal")]
    ServicePrincipal = 2,

    [Display(Name = "Workload Identity")]
    WorkloadIdentity = 3,
}
