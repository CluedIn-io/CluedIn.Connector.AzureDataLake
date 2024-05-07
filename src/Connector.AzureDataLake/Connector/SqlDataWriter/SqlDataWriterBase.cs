﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter
{
    internal abstract class SqlDataWriterBase : ISqlDataWriter
    {
        protected static object GetValue(string key, SqlDataReader reader)
        {
            var value = reader.GetValue(key);

            if (value == DBNull.Value)
            {
                return null;
            }

            return value;
        }

        public abstract Task WriteAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader);
    }
}