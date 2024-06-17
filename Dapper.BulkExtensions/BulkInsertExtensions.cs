namespace Dapper.BulkExtensions.Postgres;

using System.Data;
using System.Reflection;
using System.Text;

public static class BulkInsertExtensions
{
    public static Task<string> GenerateBulkInsertSql<T>(List<T> collection, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        if (collection == null || collection.Count() == 0)
            return Task.FromResult(string.Empty);

        Type classType = typeof(T);
        PropertyInfo[] propertyInfo = classType.GetProperties();
        var columnNamesList = propertyInfo.Select(x => x.Name).ToArray();

        var inserColumnsCollection = propertyInfo
            .Where(x => propertyShouldBeEditable(x) == true)
            .Select(x => $"\"{x.Name}\"");

        var col =
            $"insert into {tableName} ({string.Join(',', inserColumnsCollection)}) {Environment.NewLine}values{Environment.NewLine}";

        StringBuilder sbMain = new StringBuilder(col);

        foreach (T item in collection)
        {
            List<string> valuesSqlList = new List<string>();
            foreach (var columnName in columnNamesList)
            {
                var property = item?.GetType().GetProperty(columnName);
                if (property != null)
                {
                    if (!propertyShouldBeEditable(property))
                        continue;
                }
                valuesSqlList.Add($"{FormatInsertSqlColumnDataBasedOnDataType<T>(property, item)}");
            }
            sbMain.AppendLine($"({string.Join(',', valuesSqlList)}),");
        }
        return Task.FromResult(sbMain.ToString().TrimEnd('\n').TrimEnd('\r').TrimEnd(','));
    }

    private static bool propertyShouldBeEditable(PropertyInfo property) =>
        !(
            property.CustomAttributes is not null
            && property.CustomAttributes.Count() > 0
            && property.CustomAttributes.First().AttributeType.FullName
                == "System.ComponentModel.DataAnnotations.EditableAttribute"
            && property.CustomAttributes.First().ConstructorArguments.First().ArgumentType.FullName
                == "System.Boolean"
            && Convert.ToBoolean(
                property.CustomAttributes.First().ConstructorArguments.First().Value
            ) == false
        );

    private static string FormatInsertSqlColumnDataBasedOnDataType<T>(
        PropertyInfo? propertyInfo,
        T item
    )
    {
        if (propertyInfo is not null)
        {
            var itemValue = propertyInfo.GetValue(item);
            return propertyInfo.PropertyType.FullName switch
            {
                "System.String"
                    => itemValue == null
                        ? "''"
                        : $"'{Convert.ToString(itemValue)?.Replace("'", "''")}'", //We are here replacing ' with '' to prevent SQL injection attack and also ensure that SQL doesn't break if we get `'` as input in text
                "System.Guid" => itemValue == null ? "null" : $"'{Convert.ToString(itemValue)}'",
                "System.DateTime"
                    => itemValue == null
                        ? "null"
                        : "'" + Convert.ToDateTime(itemValue).ToString("yyyy-MM-dd HH:mm:ss") + "'",
                "System.Int16"
                or "System.Int32"
                or "System.Int64"
                or "System.Decimal"
                or "System.Double"
                    => itemValue == null ? "null" : $"{Convert.ToString(itemValue)}",
                _
                    => itemValue == null
                        ? "null"
                        : $"'{Convert.ToString(itemValue)?.Replace("'", "''")}'"
            };
        }
        else
        {
            return "null";
        }
    }
}
