using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamodbExample
{
    class Program
    {
        static readonly AmazonDynamoDBClient client = new AmazonDynamoDBClient();

        static async Task Main(string[] args)
        {
            //await Insert();
            await Select();

            Console.WriteLine("Hello World!");
        }

        static async Task Insert()
        {
            var request = new ExecuteStatementRequest()
            {
                Statement = "insert into \"sample\" value {'key':'dd'}"
            };
            var res = await client.ExecuteStatementAsync(request);
        }


        static async Task Select()
        {
            
            var request = new Amazon.DynamoDBv2.Model.ExecuteStatementRequest()
            {
                Statement = "select * from \"sample\""
            };
            var res = await client.ExecuteStatementAsync(request);

            foreach(var item in res.Items)
            {

            }
        }
    }
}
