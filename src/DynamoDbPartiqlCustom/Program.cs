using System;
using System.Linq;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Model.Internal.MarshallTransformations;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.Runtime.Internal.Transform;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

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

            var invokeResult = InvokeAsync<PartiqlResponse>(request, options, cancellationToken);
            return invokeResult;
        }
    }

    class PartiqlResponse : AmazonWebServiceResponse
    {
        public Document[] Items { get; set; }

        public T[] GetItems<T>() => this.Items.Select(doc => JsonConvert.DeserializeObject<T>(doc.ToJson())).ToArray();
        public T[] GetItems<T>(T type) => this.Items.Select(doc => JsonConvert.DeserializeObject<T>(doc.ToJson())).ToArray();
    }

    class PartiqlResponseUnmarshaller : JsonResponseUnmarshaller
    {
        public override AmazonWebServiceResponse Unmarshall(JsonUnmarshallerContext context)
        {
            context.Read();
            int targetDepth = context.CurrentDepth;

            List<Dictionary<string, AttributeValue>> items = null;
            string nextToken = null;
            while (context.ReadAtDepth(targetDepth))
            {
                if (context.TestExpression("Items", targetDepth))
                {
                    var unmarshaller = new ListUnmarshaller<Dictionary<string, AttributeValue>, DictionaryUnmarshaller<string, AttributeValue, StringUnmarshaller, AttributeValueUnmarshaller>>(new DictionaryUnmarshaller<string, AttributeValue, StringUnmarshaller, AttributeValueUnmarshaller>(StringUnmarshaller.Instance, AttributeValueUnmarshaller.Instance));
                    items = unmarshaller.Unmarshall(context);
                    continue;
                }
                if (context.TestExpression("NextToken", targetDepth))
                {
                    var unmarshaller = StringUnmarshaller.Instance;
                    nextToken = unmarshaller.Unmarshall(context);
                    continue;
                }
            }

            //using var reader = new StreamReader(context.Stream);

            //var str = reader.ReadToEnd();


            return new PartiqlResponse()
            {
                Items = items.Select(attributeMap => Document.FromAttributeMap(attributeMap)).ToArray()
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

            var a = new { key = default(string), value = default(float?) };
            var type = a.GetType();

            //var list = res.GetItems(a);
            var list =res.GetItems<query1> ();
            
            foreach(var item in list)
            {
                var key = item.key;
            }

            var list2 = res.Items.Select(doc => JsonConvert.DeserializeObject<(string key, string value)>(doc.ToJson())).ToArray();
            

            //var awsCredentials = new Amazon.Runtime.AWSCredentials();

            //Amazon.Util.AWSCredentialsProfile.
            //var cc = new Amazon.Runtime.BasicAWSCredentials();
            //Console.WriteLine("Hello World!");
        }

        
    }

    record query1(string key, string value);
}

