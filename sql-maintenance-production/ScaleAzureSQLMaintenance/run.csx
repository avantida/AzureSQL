#r "System.Configuration"
#r "System.Data"

#load "Connection.csx"

using System;
using System.Linq;
using System.Configuration;
using System.Data.SqlClient;

using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Sql.Fluent.Models;

public static void Run(TimerInfo myTimer, ICollector<Connection> outputQueueItem, TraceWriter log)
{
    // Use the Azure management libraries for .NET
    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/sql?view=azure-dotnet#management-library
    // to fetch all SQL Servers and SQL Databases
    // then attempt to run the Azure SQL Maintenance procedure
    // If the procedure has not been installed, it will fail immediately

    // Set up the Authentication: https://docs.microsoft.com/en-us/dotnet/azure/dotnet-sdk-azure-authenticate?view=azure-dotnet#mgmt-auth
    // Created using PowerShell - note that we have only given the user the "SQL DB Contributor" role
    /*
    Login-AzureRmAccount
     => capture TenantId "e4479931-..."
    $sp = New-AzureRmADServicePrincipal -DisplayName "AzureSqlMaintenance" -Password "Xbbi3ZAv6ywfw5UsMoKFiXcLHxRolwjK"
    New-AzureRmRoleAssignment -ServicePrincipalName $sp.ApplicationId -RoleDefinitionName "SQL DB Contributor"
    $sp | Select DisplayName, ApplicationId
     => capture ApplicationId "29182e12-..."
    */
    var credentials = SdkContext.AzureCredentialsFactory
        .FromServicePrincipal("",   // clientId
        "",                         // clientSecret
        "",                         // tenantId
        AzureEnvironment.AzureGlobalCloud);

    var azure = Azure
        .Configure()
        .Authenticate(credentials)
        .WithDefaultSubscription();    

        var s = new SqlConnectionStringBuilder();
        s.ApplicationName = "AzureSqlMaintenance";
        s.UserID = "AzureSqlMaintenance";                               // SQL Database User create when installing the stored procedure
        s.Password = "";

        var sqlServers = azure.SqlServers.List();
        foreach (var ss in sqlServers)
        {
            s.DataSource = ss.FullyQualifiedDomainName;
            foreach (var db in ss.Databases.List())
            {
                if (db.Name.ToLower() == "master") continue;
                // Only run the maintenance on the Primary database
                foreach (var replLink in db.ListReplicationLinks())
                {
                    var repl = replLink.Value;
                    if (repl.Role != ReplicationRole.Primary) continue;
                } 
                s.InitialCatalog = db.Name;
                if (OpenConnection(s.ToString()))
                {
                    outputQueueItem.Add(new Connection { Name = db.Name, ConnectionString = s.ToString()});
                    log.Info($" Queued {db.Name}");
                }
                else log.Info($" - {db.Name} Skipped");
            }
        }
}
private static bool OpenConnection(string cs)
{
    try {
        using (SqlConnection conn = new SqlConnection(cs))
        {
            conn.Open();
            return true;
        }
    }
    catch
    {
        return false;
    }
}