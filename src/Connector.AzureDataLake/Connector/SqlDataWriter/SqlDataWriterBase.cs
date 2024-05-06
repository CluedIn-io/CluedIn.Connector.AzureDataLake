﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

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

            return null;
        }

        public abstract Task WriteAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader);
    }
}
