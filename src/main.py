from mcstatus import MinecraftServer
import boto3
import sys
import os
import time
import datetime
from subprocess import call

### REQUIRED Env Vars ###
ASG_NAME = os.environ.get('ASG_NAME')
R53_RECORD_SET_NAME = os.environ.get('R53_RECORD_SET_NAME')
R53_HOSTED_ZONE_ID = os.environ.get('R53_HOSTED_ZONE_ID')

### Optional. Only change if doing something non-standard ###
MC_SERVER_ADDRESS = os.environ.get('MC_SERVER_ADDRESS', 'localhost')
MC_SERVER_RCON_PORT = os.environ.get('MC_SERVER_RCON_PORT', '25565')
ECS_CLUSTER_NAME = os.environ.get('ECS_CLUSTER_NAME', 'ecs-minecraft')
PULL_TASK_NAME = os.environ.get('PULL_TASK_NAME', 's3-pull')
PUSH_TASK_NAME = os.environ.get('PUSH_TASK_NAME', 's3-push')
MS_SERVER_TASK_NAME = os.environ.get('MS_SERVER_TASK_NAME', 'mc-server')

def log(line):
    i = datetime.datetime.now()
    print("[{}] {}".format(i.isoformat(), line))

def player_watch_loop():
    ticks = 0
    server = MinecraftServer.lookup(
        "{}:{}".format(MC_SERVER_ADDRESS, MC_SERVER_RCON_PORT)
    )
    while True:
        status = server.status()
        player_count = status.players.online
        log("Player Count: {}".format(player_count))
        if player_count < 1:
            ticks += 1
        else:
            ticks = 0
        if ticks > 60:
            log("Player count has been 0 for 60 consecutive ticks.")
            return
        time.sleep(10)

def scale_asg(count):
    client = boto3.client('autoscaling')
    response = client.update_auto_scaling_group(
        AutoScalingGroupName=ASG_NAME,
        DesiredCapacity=count
    )
    log(response)

def wait_for_desired_capacity(count):
    client = boto3.client('ecs')
    while True:
        response = client.describe_clusters(
            clusters=[
                ECS_CLUSTER_NAME,
            ]
        )
        log("DEBUG: {}".format(response))
        current_count = response['clusters'][0]['registeredContainerInstancesCount']
        log("The cluster currently has '{}' registered instances".format(current_count))
        if current_count == count:
            return
        time.sleep(10)

def run_task(task_name):
    client = boto3.client('ecs')
    response = client.run_task(
        cluster=ECS_CLUSTER_NAME,
        taskDefinition=task_name
    )
    task_arn = response['tasks'][0]['taskArn']
    log("Task ARN is '{}".format(task_arn))
    return task_arn

def wait_for_task(task_arn):
    client = boto3.client('ecs')
    while True:
        response = client.describe_tasks(
            cluster=ECS_CLUSTER_NAME,
            tasks=[
                task_arn,
            ]
        )
        last_status = response['tasks'][0]['lastStatus']
        log("Status for task '{}' is '{}'".format(task_arn, last_status))
        if last_status == 'STOPPED':
            return
        time.sleep(1)

def stop_task(task_arn):
    client = boto3.client('ecs')
    response = client.stop_task(
        cluster=ECS_CLUSTER_NAME,
        task=task_arn
    )

def get_cluster_instance_public_ip():
    asg = boto3.client('autoscaling')
    response = asg.describe_auto_scaling_groups(
        AutoScalingGroupNames=[
            ASG_NAME,
        ]
    )
    instance_id = response['AutoScalingGroups'][0]['Instances'][0]['InstanceId']
    ec2 = boto3.resource('ec2')
    instance = ec2.Instance(instance_id)
    return instance.public_ip_address

    

def create_record_set():
    client = boto3.client('route53')
    public_ip = get_cluster_instance_public_ip()
    response = client.change_resource_record_sets(
        HostedZoneId=R53_HOSTED_ZONE_ID,
        ChangeBatch={
            'Changes': [
                {
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': R53_RECORD_SET_NAME,
                        'Type': 'A',
                        'TTL': 60,
                        'ResourceRecords': [
                            {
                                'Value': public_ip
                            }
                        ]
                    }
                }
            ]
        }
    )
    log(response)

def delete_record_set():
    client = boto3.client('route53')
    public_ip = get_cluster_instance_public_ip()
    response = client.change_resource_record_sets(
        HostedZoneId=R53_HOSTED_ZONE_ID,
        ChangeBatch={
            'Changes': [
                {
                    'Action': 'DELETE',
                    'ResourceRecordSet': {
                        'Name': R53_RECORD_SET_NAME,
                        'Type': 'A',
                        'TTL': 60,
                        'ResourceRecords': [
                            {
                                'Value': public_ip
                            }
                        ]
                    }
                }
            ]
        }
    )
    log(response)

if __name__ == "__main__":
    create_record_set()
    pull_task_arn = run_task(PULL_TASK_NAME)
    wait_for_task(pull_task_arn)
    mc_task_arn = run_task(MS_SERVER_TASK_NAME)
    player_watch_loop()
    stop_task(mc_task_arn)
    wait_for_task(mc_task_arn)
    push_task_arn = run_task(PUSH_TASK_NAME)
    wait_for_task(push_task_arn)
    delete_record_set()
    scale_asg(0)
    sys.exit(0)