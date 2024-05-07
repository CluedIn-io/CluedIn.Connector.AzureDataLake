using System;
using System.Reflection;

namespace CluedIn.Connector.AzureDataLake.Extensions;

internal static class TypeExtension
{
    public static Type GetNonNullable(this Type type)
    {
        var ti = type.GetTypeInfo();

        if (ti.IsClass)
            return type;

        if (ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>))
            return ti.GenericTypeArguments[0];
        else
            return type;
    }
}
