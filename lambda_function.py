import boto3
import json
import logging
import uuid
import hashlib
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Tasks"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

createTaskRouteKey="POST /tasks"
getTasksRouteKey="GET /tasks"
getTaskRouteKey= "GET /tasks/{id}"
editTaskRouteKey= "PUT /tasks/{id}"
deleteTaskRouteKey= "DELETE /tasks/{id}"


def lambda_handler(event, context):
    logger.info(event)
    if event['routeKey'] == createTaskRouteKey:
        request_body = json.loads(event["body"])
        response = saveTask(request_body)
    elif event['routeKey'] == getTasksRouteKey:
        response = getTasks()
    elif event['routeKey'] == getTaskRouteKey:
        response = getTask(event["pathParameters"]["id"])
    elif event['routeKey'] == editTaskRouteKey:
        request_body = json.loads(event["body"])
        response = saveTask(request_body)
        # response = editTask(event["pathParameters"]["id"], request_body)
    elif event['routeKey'] == deleteTaskRouteKey:
        response = deleteTask(event["pathParameters"]["id"])
    else:
        response = buildResponse(404, "Not Found")
    return response

def saveTask(request_body):
    try:
        request_body["id"] = str(create_uuid_from_string(request_body["taskName"]))
        table.put_item(Item=request_body)
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        }
        return buildResponse(200, body)
    except:
        logger.exception("Unable to save task!!")

def getTask(id):
    try:
        response = table.get_item(
            Key={
                "id": id
            }
        )
        if "Item" in response:
            return buildResponse(200, response["Item"])
        else:
            return buildResponse(404, {"Message": "TaskName: {0} not found".format(taskName)})
    except:
        logger.exception("Unable to get task")

def getTasks():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluateKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        body = {
            "tasks": response["Items"]
        }
        return buildResponse(200, body)
    except:
        logger.exception("Unable to get tasks!!")


def editTask(id, request_body):
    try:
        request_body["id"] = id;
        response = table.update_item(
            Key={"id": id},
            UpdateExpression="set Table=:value",
            ExpressionAttributeValues={":value": request_body},
            ReturnValues="UPDATED_NEW",
        )
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        }
        return buildResponse(200, body)
    except:
        logger.exception("Unable to edit task!!Attempt 1")

def deleteTask(id):
    try:
        response = table.delete_item(
            Key={
                "id": id
            },
            ReturnValues="ALL_OLD"
        )
        body = {
            "Operation": "DELETE",
            "Message": "SUCCESS",
        }
        return buildResponse(200, body)
    except:
        logger.exception("Unable to delete task!!")


def buildResponse(statusCode, body=None):
    response = {
        "statusCode": statusCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }

    if body is not None:
        response["body"] = json.dumps(body)
    return response

def create_uuid_from_string(val: str):
    hex_string = hashlib.md5(val.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)
