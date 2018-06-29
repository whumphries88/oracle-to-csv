using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Configuration;
using System.Data;
using FileHelpers;
using System.IO;

namespace oracle_test
{
    class Program
    {
        static void Main(string[] args)
        {
            OraTest ot = new OraTest();
            ot.Connect();
        }

        class OraTest
        {        
            public void Connect()
            {
                var constr = new OracleConnectionStringBuilder()
                {
                    DataSource = @"(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.000.000.00)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME = a3a)))",
                    UserID = "aaa",
                    Password = "aaa",
                }.ConnectionString;

                using (var con = new OracleConnection(constr))
                {
                    string prfbalqry = "select * from aaa.bbb where BE = 0";
                    //string prfbalqry = "select * from aaa.bbb";
                    
                    OracleCommand cmd = new OracleCommand(prfbalqry, con);
                    cmd.CommandType = System.Data.CommandType.Text;
                    con.Open();
                    OracleDataReader dr = cmd.ExecuteReader();
                    dr.Read();
                    Console.WriteLine(dr.GetInt32(0));

                    DataTable pbResults = new DataTable();
                    OracleDataAdapter oda = new OracleDataAdapter(cmd);
                    oda.Fill(pbResults);

                    StringBuilder sb = new StringBuilder();
                    IEnumerable<string> columnNames = pbResults.Columns.Cast<DataColumn>().Select(column => column.ColumnName);
                    sb.AppendLine(string.Join(",", columnNames));

                    foreach (DataRow row in pbResults.Rows)
                    {
                        IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                        sb.AppendLine(string.Join(",", fields));
                    }

                    File.WriteAllText(@"C:\Financial Reporting\Output Path\pbresults.csv", sb.ToString());
                }
            }
        }
    }
}
