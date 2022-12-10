import boto3
import json
from custom_encoder import CustomEncoder
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# define the dynamoDB table
dynamoDBTableName = 'product-inventory'

# define dynamo client
dynamoDB = boto3.resource('dynamodb')
table = dynamoDB.Table(dynamoDBTableName)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/health'
productPath = '/product'
productsPath = '/products'

# define the handler function now
def lambda_handler(event, context):
    #  will log the request event
    logger.info(event)
    # extract the http method

    # httpMethod = event['requestContext']['httpMethod']
    #httpMethod = event['requestContext']['http']['method']
    # extract the path as well

    httpMethod = event['httpMethod']
    path = event['path']
    # handle the scenarios
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200,'Hey,Hi your API is healthy !!')
    elif httpMethod == getMethod and path == productPath:
        response = getProduct(event['queryStringParameters']['productID'])
    elif httpMethod == getMethod and path == productsPath:
        response = getProducts()
    elif httpMethod == postMethod and path == productPath:
        response = saveProduct(json.loads(event['body'])) # extracting body paramete from event
    elif httpMethod == patchMethod and path == productPath:
        # it needs productID,updateKey,updateValue
        requestBody = json.loads(event['body'])
        response = modifyProduct(requestBody['productID'], requestBody['updateKey'], requestBody['updateValue'])
    elif httpMethod == deleteMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = deleteProduct(requestBody['productID'])
    else:
        response = buildResponse(404, 'Not found')

    return response

def getProduct(productId):
    try:
        response =  table.get_item(
            Key = {
                'productID': productId
            }
        )
        logger.info(response)
        if 'Item' in response:
            return buildResponse(200,response['Item'])
        else:
            return buildResponse(404, {'Message': 'ProductID: %s not found' % productId})
    except:
        logger.exception('Displaying the error from getProduct function')

def getProducts():
    try:
        response = table.scan()
        result = response['Items']

        #Keep rolling till last item
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartkey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        body = {
            'products': result
        }
        return buildResponse(200, body)
    except:
        logger.exception('Displaying the error from getProducts function')

def saveProduct(requestbody):
    try:
        table.put_item(Item=requestbody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestbody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Displaying the error from saveProduct function')
def modifyProduct(productId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key = {
                'productID': productId
            },
            UpdateExpression='set %s = :value' %updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'updatedAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Displaying the error from modifyProduct function')

def deleteProduct(productId):
    try:
        response =  table.delete_item(
            Key = {
                'productID': productId
            },
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'deletedItem': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Displaying the error from deleteProduct function')
def buildResponse(statusCode, body=None):
    response = {
        'statusCode' : statusCode,
        'headers' : {
            'Content-Type' : 'application/json',
            'Access-Control-Allow-origin': '*'
            # Allow cross region access
        }
    }
    if body is not None: # then you can pass the custom object
        response['body'] = json.dumps(body, cls=CustomEncoder)
        # since object get from dynamodb we need to crete custom encoder
        return response