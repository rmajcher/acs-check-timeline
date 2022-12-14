import boto3
import argparse, sys
import time
import requests
import json

description = \
"""Query ACS CloudWatch Logs"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument("-e", "--execution", help="determines if the script is running in AWS or Lambda function or on local machine: local / lambda", default='local', required=False)
parser.add_argument('-r', '--restartacs', help='restart acs if Deadline Exceeded errors are greater than 1000 in 15 mins: yes / no', default='no', required=False)
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
    lambda_client = boto3.client('lambda', 'us-east-1',
        aws_access_key_id = credentials['AccessKeyId'],
        aws_secret_access_key = credentials['SecretAccessKey'],
        aws_session_token = credentials['SessionToken'],
    )
else:
    client = boto3.client('logs', 'us-east-1')
    lambda_client = boto3.client('lambda', 'us-east-1')

def trigger_incident(ALERT_SUMMARY, new_timeline, handling, DEADLINE_EXCEEDED):
    ROUTING_KEY = "2fd0cc1d72304d09c0e4766561995e7c" # ENTER EVENTS V2 API INTEGRATION KEY HERE
    # Triggers a PagerDuty incident without a previously generated incident key
    # Uses Events V2 API - documentation: https://v2.developer.pagerduty.com/docs/send-an-event-events-api-v2

    header = {
        "Content-Type": "application/json"
    }

    payload = { # Payload is built with the least amount of fields required to trigger an incident
        "routing_key": ROUTING_KEY,
        "event_action": "trigger",
        "payload": {
            "summary": ALERT_SUMMARY,
            "source": "aws-check-timline python script",
            "severity": "critical",
            "custom_details": {
              "NewTimeline": new_timeline,
              "ExpiredHandling": handling,
              "DeadlineExceeded": DEADLINE_EXCEEDED
            }
        }
    }

    response = requests.post('https://events.pagerduty.com/v2/enqueue',
                            data=json.dumps(payload),
                            headers=header)

    if response.json()["status"] == "success":
        print('Incident created with with dedup key (also known as incident / alert key) of ' + '"' + response.json()['dedup_key'] + '"')
    else:
        print(response.text) # print error message if not successful

def acsRestartTriageSlack(message_text):
    url = 'https://hooks.slack.com/services/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'
    response = requests.post(url, json = message_text, headers = {"Content-type": "application/json"})
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

def queryLogs():
    t = time.time()
    # 20 mins ago
    start_time = int(t * 1000) - 1200000
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
    LILO_LCT_query = client.start_query(
        logGroupNames = [
            '/ecs/production/lilo-blue/service-lilo-lct',
            '/ecs/production/lilo-green/service-lilo-lct'
        ],
        startTime = start_time,
        endTime = end_time,
        queryString = "fields @message | stats count(@message)"
    )

    time.sleep(5)
    new_timeline_results = client.get_query_results(queryId = str(new_timeline_query['queryId']))['results'][0][0]['value']
    handling_results = client.get_query_results(queryId = str(handling_query['queryId']))['results'][0][0]['value']
    DEADLINE_EXCEEDED_results = client.get_query_results(queryId = str(DEADLINE_EXCEEDED_query['queryId']))['results']
    LILO_LCT_query_results = client.get_query_results(queryId = str(LILO_LCT_query['queryId']))['results'][0][0]['value']
    return new_timeline_results, handling_results, DEADLINE_EXCEEDED_results, LILO_LCT_query_results

def main():
    new_timeline, handling, DEADLINE_EXCEEDED, LILO_LCT = queryLogs()
    try:
        DEADLINE_EXCEEDED_format = DEADLINE_EXCEEDED[0][0]["value"]
    except:
        DEADLINE_EXCEEDED_format = 0

    if new_timeline != handling:
        print('timelines are out of sync, trying again in 2 mins')
        print(f'NewTimeline:        {new_timeline}')
        print(f'ExpiredHandling:    {handling}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format}')
        print(f'LILO processing:   {LILO_LCT}')
        time.sleep(120)
        new_timeline_retry, handling_retry, DEADLINE_EXCEEDED_retry, LILO_LCT_retry = queryLogs()
        try:
            DEADLINE_EXCEEDED_format_retry = DEADLINE_EXCEEDED_retry[0][0]["value"]
        except:
            DEADLINE_EXCEEDED_format_retry = 0

        # NewTimeline and ExpiredHandling don't equal
        if new_timeline_retry != handling_retry:
            print(f'TIMELINES ARE OUT OF SYNC')
            print(f'NewTimeline:        {new_timeline_retry}')
            print(f'ExpiredHandling:    {handling_retry}')
            print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format_retry}')
            print(f'LILO processing:   {LILO_LCT_retry}')
            slack_message.append('TIMELINES ARE OUT OF SYNC')
            slack_message.append(f'NewTimeline:        {new_timeline_retry}')
            slack_message.append(f'ExpiredHandling:    {handling_retry}')
            slack_message.append(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format_retry}')
            slack_message_json = {"text": '\n'.join([str(item) for item in slack_message])}
            trigger_incident('ACS Timelines Out of Sync', new_timeline_retry, handling_retry, DEADLINE_EXCEEDED_format_retry)
            #acsRestartTriageSlack(slack_message_json)
            exit()
        else:
            print(f'timelines recovered')
            print(f'NewTimeline:        {new_timeline_retry}')
            print(f'ExpiredHandling:    {handling_retry}')
            print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format_retry}')
            print(f'LILO processing:   {LILO_LCT_retry}')
    # deadline exceeded greater than 1000 for 15 min period
    elif int(DEADLINE_EXCEEDED_format) > 1000:
        print('DEADLINE_EXCEEDED')
        print(f'NewTimeline:        {new_timeline}')
        print(f'ExpiredHandling:    {handling}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format}')
        print(f'LILO processing:   {LILO_LCT}')
        slack_message.append('DEADLINE_EXCEEDED')
        slack_message.append(f'NewTimeline:        {new_timeline}')
        slack_message.append(f'ExpiredHandling:    {handling}')
        slack_message.append(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format}')
        slack_message_json = {"text": '\n'.join([str(item) for item in slack_message])}

        # send alert to pager duty Ops 24/7 group
        trigger_incident('ACS Deadline Exceeded', new_timeline, handling, DEADLINE_EXCEEDED_format)

        # send message to #acs-alerts slack channel webhook is currently borken
#        acsRestartTriageSlack(slack_message_json)

        if args.restartacs.lower() == 'yes':
            # restart ACS with sis-production_inf-service-active-content_automated-restart lambda function
            print('restarting ACS with sis-production_inf-service-active-content_automated-restart lambda function')
            response = lambda_client.invoke(
                FunctionName='sis-production_inf-service-active-content_automated-restart',
                InvocationType='Event',
                LogType='None'
            )
            print(response)
        exit()
    elif int(LILO_LCT) < 5000:
        print(f'LILO processing depressed:   {LILO_LCT}')
        trigger_incident('Lilo Proccessing Depressed', LILO_LCT, "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:logs-insights$3FqueryDetail$3D$257E$2528end$257E0$257Estart$257E-900$257EtimeType$257E$2527RELATIVE$257Eunit$257E$2527seconds$257EeditorString$257E$2527fields*20*40message*0a*7c*20stats*20count*28*2a*29*20$257EisLiveTail$257Efalse$257EqueryId$257E$2527e764f145-befb-4c33-893a-fda0aea95de7$257Esource$257E$2528$257E$2527*2fecs*2fproduction*2flilo-blue*2fservice-lilo-lct$257E$2527*2fecs*2fproduction*2flilo-green*2fservice-lilo-lct$2529$2529")
    else:
        print('ACS is like Fonzie')
        print(f'NewTimeline:        {new_timeline}')
        print(f'ExpiredHandling:    {handling}')
        print(f'DeadlineExceeded:   {DEADLINE_EXCEEDED_format}')
        print(f'LiloProccessing:    {LILO_LCT}')

# lambda handler
def lambda_handler(event, context):
    main()

if __name__ == '__main__':
    if 'lambda' in args.execution:
        lambda_handler()
    else:
        main()
