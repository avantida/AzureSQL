#r "System.Configuration"
#r "System.Data"

using System;
using System.Linq;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Diagnostics;

/* Backlog

    * run AzureSqlMaintenance.sql on the database if it does not exist
    * test the version of AzureSqlMaintenance, and update if needed
*/

public static async Task Run(TimerInfo myTimer, TraceWriter log)
{
    var connectionStrings = ConfigurationManager.ConnectionStrings
                        .Cast<ConnectionStringSettings>()
                        .Where(cs => !cs.Name.StartsWith("Local"));

    foreach (var cs in connectionStrings)
    try {
        using (SqlConnection conn = new SqlConnection(cs.ConnectionString))
        {
            conn.InfoMessage += (object sender, SqlInfoMessageEventArgs e) =>
            {
                log.Info(e.Message,"AzureSQLMaintenance");
            };

            var sqlBuilder = new SqlConnectionStringBuilder(cs.ConnectionString);
            log.Info($"Opening connection to {cs.Name}");
            conn.Open();
            var text = "if object_id('AzureSQLMaintenance') is null select -1 else select 0";
            using (SqlCommand cmd = new SqlCommand(text, conn))
            {
                var result = (int) await cmd.ExecuteScalarAsync();
                switch (result)
                {
                    case -1:
                        log.Info($"AzureSQLMaintenance Procedure not installed on {sqlBuilder.InitialCatalog}");
                        continue;
                    case 1:
                        log.Info($"AzureSQLMaintenance Procedure not initialized on {sqlBuilder.InitialCatalog}");
                        continue;
                    default:
                        break;
                }
            }
            var action = "statistics";
            if (DateTime.Now.DayOfWeek == DayOfWeek.Saturday) action = "all";
            text = $"exec AzureSQLMaintenance '{action}'";
            using (SqlCommand cmd = new SqlCommand(text, conn))
            {
                cmd.CommandTimeout = 5 * 60 * 60; // 5 hours
                log.Info($"AzureSQLMaintenance Procedure Started");
                var timer = Stopwatch.StartNew();
                var result = await cmd.ExecuteScalarAsync();
                log.Info($"AzureSQLMaintenance Procedure Completed {timer.Elapsed}");
            }
        }
    }
    catch (Exception ex)
    {
        log.Warning($"Error running on {cs.Name}");
        log.Warning(ex.Message);
    }
}