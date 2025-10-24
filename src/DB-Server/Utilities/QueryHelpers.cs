using System;
using System.Collections.Generic;
using System.Text;

namespace Minerva.DB_Server;

public static class QueryHelper
{
    /// <summary>
    /// Query args are always separated by semi-colon following the "1:"  1:xx,xx,xx...;xx,xx;xx;...
    /// With variable number of fields separate by comma
    /// </summary>
    /// <param name="queryArgs">The query arguments string.</param>
    /// <param name="_argsStartingIndex">The index in the query string where the arguments start (usually after the command name).</param>
    /// <returns>A list of lists, where each inner list contains byte arrays representing the fields of each argument.</returns>
    
    public static List<List<byte[]>> ParseQueryArgs(string queryArgs, int _argsStartingIndex)
    {
        List<List<byte[]>> args = [];

        if (string.IsNullOrEmpty(queryArgs))
        {
            return args;
        }

        // Convert string to bytes for processing
        var queryBytes = Encoding.UTF8.GetBytes(queryArgs);
        
        // Skip to the starting index, accounting for UTF-16 encoding (2 bytes per character)
        var query = new ReadOnlySpan<byte>(queryBytes, _argsStartingIndex, queryBytes.Length - _argsStartingIndex );

        while (!query.IsEmpty)
        {
            // Find the next semicolon (argument separator)
            int semicolonIndex = query.IndexOf((byte)';');
            
            ReadOnlySpan<byte> currentArg;
            if (semicolonIndex == -1)
            {
                // Last argument (no more semicolons)
                currentArg = query;
                query = ReadOnlySpan<byte>.Empty;
            }
            else
            {
                // Extract current argument and advance query
                currentArg = query[..semicolonIndex];
                query = query[(semicolonIndex + 1)..];
            }

            // Parse fields within the current argument (separated by commas)
            List<byte[]> fields = [];
            
            while (!currentArg.IsEmpty)
            {
                int commaIndex = currentArg.IndexOf((byte)',');
                
                ReadOnlySpan<byte> field;
                if (commaIndex == -1)
                {
                    // Last field (no more commas)
                    field = currentArg;
                    currentArg = ReadOnlySpan<byte>.Empty;
                }
                else
                {
                    // Extract current field and advance currentArg
                    field = currentArg[..commaIndex];
                    currentArg = currentArg[(commaIndex + 1)..];
                }

                // Convert field to byte array and add to fields list
                if (!field.IsEmpty)
                {
                    fields.Add(field.ToArray());
                }
            }

            // Add the parsed fields to the result
            if (fields.Count > 0)
            {
                args.Add(fields);
            }
        }

        return args;
    }   

}