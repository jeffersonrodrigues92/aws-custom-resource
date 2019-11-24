import boto3
import uuid
import requests
import json

client = boto3.client('s3')

def handler(event, context):

    print(event)

    request_type = event['RequestType']
    data = event['ResourceProperties']
    data_old = {}

    response = { 
            'StackId': event['StackId'],
            'RequestId': event['RequestId'],
            'LogicalResourceId': event['LogicalResourceId'],
            'Status': 'SUCCESS',
            'Data': {}
        }

    try:

        if 'OldResourceProperties' in event:
            data_old = event['ResourceProperties']['OldResourceProperties']

        if 'PhysicalResourceId' in event:
            response['PhysicalResourceId'] = event['PhysicalResourceId']
        else:
            response['PhysicalResourceId'] = str(uuid.uuid4())

        verify_event(request_type, data, data_old)
        callback(event, response)

    except Exception as err:
        exception_type = err.__class__.__name__
        exception_message = str(err)

        api_exception_obj = {
            "isError": True,
            "type": exception_type,
            "message": exception_message
        }

        print(event)

        response = {
            'StackId': event['StackId'],
            'RequestId': event['RequestId'],
            'LogicalResourceId': event['LogicalResourceId'],
            'Status': 'FAILED',
            'Reason': ""
        }

        print(response)

        if 'PhysicalResourceId' in event:
            response['PhysicalResourceId'] = event['PhysicalResourceId']
        else:
            response['PhysicalResourceId'] = str(uuid.uuid4())

        callback(event, response)

        api_exception_json = json.dumps(api_exception_obj)
        raise CustomResource(api_exception_json)


def verify_event(request_type, data, data_old):
    if request_type == 'Create':
        create_folder(data)
    
    elif request_type == 'Update':
        update_folder(data, data_old)

    else:
        delete_folder(data)


def create_folder(data):
    bucket_name = data['Bucket']
    folders = data["Key"]

    for key in folders:
        print ('Create folder: ' + key)
        response = client.put_object(
            Bucket=bucket_name,
            Key=(key + '/')
        )

def update_folder(data, data_old):
    delete_folder(data_old)
    create_folder(data)

def delete_folder(data):
    bucket_name = data['Bucket']
    folders = data["Key"]

    for key in folders:
        print ('Delete folder: ' + key)
        response = client.delete_object(
            Bucket=bucket_name,
            Key=(key + '/')
        )

def callback(request, response):
    if 'ResponseURL' in request and request['ResponseURL']:
        try:
            body = json.dumps(response)
            requests.put(request['ResponseURL'], data=body)
            print(response)
        except:
            print("Failed to send the response to the provdided URL")
    return response

class CustomResource(Exception):
    pass