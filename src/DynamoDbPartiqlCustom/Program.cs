using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Model.Internal.MarshallTransformations;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.Runtime.Internal.Transform;

namespace DynamoDbPartiqlCustom
{
    class CustomClient : Amazon.DynamoDBv2.AmazonDynamoDBClient
    {

        //public async Task Select(BatchExecuteStatementRequest request, System.Threading.CancellationToken cancellationToken = default)
        //{
        //    var options = new InvokeOptions();
        //    options.RequestMarshaller = BatchExecuteStatementRequestMarshaller.Instance;
        //    options.ResponseUnmarshaller = BatchExecuteStatementResponseUnmarshaller.Instance;
            
        //    var response = await InvokeAsync<AmazonWebServiceResponse>(request, options, cancellationToken);
        //}

        public virtual Task<PartiqlResponse> Select(ExecuteStatementRequest request, System.Threading.CancellationToken cancellationToken = default)
        {
            var options = new InvokeOptions();
            options.RequestMarshaller = ExecuteStatementRequestMarshaller.Instance;
            options.ResponseUnmarshaller = new PartiqlResponseUnmarshaller();

            return InvokeAsync<PartiqlResponse>(request, options, cancellationToken);
        }


    }

    class PartiqlResponse : AmazonWebServiceResponse
    {
        public string Items { get; set; }
    }

    class PartiqlResponseUnmarshaller : JsonResponseUnmarshaller
    {
        public override AmazonWebServiceResponse Unmarshall(JsonUnmarshallerContext input)
        {
            using var reader = new StreamReader(input.Stream);
            
            var str = reader.ReadToEnd();

            return new PartiqlResponse()
            {
                Items = str
            };
        }

        public override AmazonServiceException UnmarshallException(JsonUnmarshallerContext input, Exception innerException, HttpStatusCode statusCode)
        {
            throw new NotImplementedException();
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var request = new Amazon.DynamoDBv2.Model.ExecuteStatementRequest()
            {
                Statement = "select * from \"sample\""
            };
            var client = new CustomClient();
            var res = await client.Select(request);

            //var awsCredentials = new Amazon.Runtime.AWSCredentials();

            //Amazon.Util.AWSCredentialsProfile.
            //var cc = new Amazon.Runtime.BasicAWSCredentials();
            //Console.WriteLine("Hello World!");
        }
    }
}
