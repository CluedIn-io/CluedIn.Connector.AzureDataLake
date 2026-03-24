using CluedIn.Core.Bootstrap;
using Microsoft.Extensions.DependencyInjection;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.S3
{
    public static class InstallComponents
    {
        public static void Register(IServiceCollection services)
        {
            services.AddSingleton<S3.Connector.S3Client>();
            services.AddSingleton<IExternalStorageClient, S3.Connector.S3Client>();
            services.AddSingleton<S3.Connector.S3Connector>();
            // Register S3JobDataFactory, S3Constants, etc. as needed
        }
    }
}
