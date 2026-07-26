using System.ComponentModel.DataAnnotations;

namespace CluedIn.Connector.AzureDataLake;

internal enum AuthenticationMethods
{
    [Display(Name = "Shared Access Key")]
    SharedKey = 1,

    [Display(Name = "Service Principal")]
    ServicePrincipal = 2,

    [Display(Name = "Workload Identity")]
    WorkloadIdentity = 3,
}
