import boto3
import argparse, sys
import time
import requests

description = \
"""Query ACS CloudWatch Logs"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument("-e", "--execution", default='local', help="determines if the script is running in AWS or on local machine: local / aws", required=False)
parser.add_argument('-t', '--tier', help='specify other SecOps user tier default: ProdOpsTier4', default='ProdOpsTier4', required=False)
parser.add_argument('-p', '--profile',help='specify AWS account: prod / preprod default: prod', default='prod', required=False)
args=parser.parse_args()

slack_message = []

if args.execution == 'local':
    accounts = {
        'prod': 853581745927,
        'preprod': 290745908312
    }
    sts_client = boto3.client('sts')
    Assumed_Role_object = sts_client.assume_role(
        RoleArn = f"arn:aws:iam::{accounts[args.profile.lower()]}:role/{args.tier}",
        RoleSessionName = f"acslog-query-{args.tier}"
    )
    credentials = Assumed_Role_object['Credentials']
    client = boto3.client('logs', 'us-east-1',
        aws_access_key_id = credentials['AccessKeyId'],
        aws_secret_access_key = credentials['SecretAccessKey'],
        aws_session_token = credentials['SessionToken'],
    )
else:
    client = boto3.client('logs', 'us-east-1',
        aws_access_key_id = credentials['AccessKeyId'],
        aws_secret_access_key = credentials['SecretAccessKey'],
        aws_session_token = credentials['SessionToken'],
    )

def acsRestartTriageSlack(message_text):
    url = 'https://hooks.slack.com/services/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'
    response = requests.post(url, json = message_text, headers = {"Content-type": "application/json"})
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

def main():
    t = time.time()
    # 35 mins ago
    start_time = int(t * 1000) - 2100000
    # 5 mins ago
    end_time = int(t * 1000) - 300000

    handling_query = client.start_query(
        logGroupName = '/ecs/sis-production-inf-service-active-content',
        startTime = start_time,
        endTime = end_time,
        queryString = """fields @message | filter @message like 'Handling expired channel timeline' | parse data '"channelID":"*"' as channel_id | stats count_distinct(channel_id) as Handling"""
    )
    new_timeline_query = client.start_query(
        logGroupName = '/ecs/sis-production-inf-service-active-content',
        startTime = start_time,
        endTime = end_time,
        queryString = """fields @message | filter @message like 'New timeline found' | parse data '"channelID":"*"' as channel_id | stats count_distinct(channel_id) as NewTimeline"""
    )
    DEADLINE_EXCEEDED_query = client.start_query(
        logGroupName = '/ecs/sis-production-inf-service-active-content',
        startTime = start_time,
        endTime = end_time,
        queryString = "fields @message | filter @message like 'DEADLINE_EXCEEDED' | stats count(@message) as DeadlineExceeded"
    )

    time.sleep(5)

    new_timeline_results = client.get_query_results(queryId = str(new_timeline_query['queryId']))['results'][0][0]['value']
    handling_results = client.get_query_results(queryId = str(handling_query['queryId']))['results'][0][0]['value']
    DEADLINE_EXCEEDED_results = client.get_query_results(queryId = str(DEADLINE_EXCEEDED_query['queryId']))['results']
    if new_timeline_results != handling_results:
        print('TIMELINES ARE OUT OF SYNC RESTART ACS')
        print(f'NewTimeline:        {new_timeline_results}')
        print(f'ExpiredHandling:    {handling_results}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_results}')
        slack_message.append('TIMELINES ARE OUT OF SYNC RESTART ACS')
        slack_message.append(f'NewTimeline:        {new_timeline_results}')
        slack_message.append(f'ExpiredHandling:    {handling_results}')
        slack_message.append(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_results}')
        slack_message_json = {"text": '\n'.join([str(item) for item in slack_message])}
        acsRestartTriageSlack(slack_message_json)
    elif DEADLINE_EXCEEDED_results != []:
        print('DEADLINE_EXCEEDED RESTART ACS')
        print(f'NewTimeline:        {new_timeline_results}')
        print(f'ExpiredHandling:    {handling_results}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_results}')
        slack_message.append('DEADLINE_EXCEEDED RESTART ACS')
        slack_message.append(f'NewTimeline:        {new_timeline_results}')
        slack_message.append(f'ExpiredHandling:    {handling_results}')
        slack_message.append(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_results}')
        slack_message_json = {"text": '\n'.join([str(item) for item in slack_message])}
        acsRestartTriageSlack(slack_message_json)
    else:
        print('ACS timelines in sync')
        print(f'NewTimeline:        {new_timeline_results}')
        print(f'ExpiredHandling:    {handling_results}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_results}')

if __name__ == '__main__':
    main()
