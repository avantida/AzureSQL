#r "System.Data"
#r "System.Configuration"
#load "../ScaleAzureSQLMaintenance/Connection.csx"

using System;
using System.Linq;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Diagnostics;

public static async Task Run(Connection myQueueItem, TraceWriter log)
{
    var cs = myQueueItem;
    try {
        using (SqlConnection conn = new SqlConnection(cs.ConnectionString))
        {
            conn.InfoMessage += (object sender, SqlInfoMessageEventArgs e) =>
            {
                log.Info(e.Message,"AzureSQLMaintenance");
            };
            var sqlBuilder = new SqlConnectionStringBuilder(cs.ConnectionString);
            log.Info($"Opening connection to {cs.Name}, {sqlBuilder.InitialCatalog}");
            conn.Open();

            await VerifyProcedure(conn, log);
            await RunAzureSqlMaintenance(conn, log);
        }
    }
    catch (Exception ex)
    {
        log.Warning($"Error running on {cs.Name}");
        log.Warning(ex.ToString());
    }
}

private static async Task VerifyProcedure(SqlConnection conn, TraceWriter log)
{
        var text = "if object_id('AzureSQLMaintenance') is null select -1 else select 0";
        using (SqlCommand cmd = new SqlCommand(text, conn))
        {
            var result = (int) await cmd.ExecuteScalarAsync();
            switch (result)
            {
                case -1:
                    log.Info($"AzureSQLMaintenance Procedure not installed");
                    return;
                case 1:
                    log.Info($"AzureSQLMaintenance Procedure not initialized");
                    return;
                default:
                    break;
            }
        }
}

private static async Task RunAzureSqlMaintenance(SqlConnection conn, TraceWriter log)
{
        var action = "statistics";
        if (DateTime.Now.DayOfWeek == DayOfWeek.Saturday) action = "all";
        var text = $"exec AzureSQLMaintenance '{action}'";
        using (SqlCommand cmd = new SqlCommand(text, conn))
        {
            cmd.CommandTimeout = 5 * 60 * 60; // 5 hours
            log.Info($"AzureSQLMaintenance Procedure Started");
            var timer = Stopwatch.StartNew();
            var result = await cmd.ExecuteScalarAsync();
            log.Info($"AzureSQLMaintenance Procedure Completed {timer.Elapsed}");
        }
}