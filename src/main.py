from mcstatus import MinecraftServer
import boto3
import sys
import os
import time
import socket
import datetime
from subprocess import call

### REQUIRED Env Vars ###
R53_RECORD_SET_NAME = os.environ.get('R53_RECORD_SET_NAME')
R53_HOSTED_ZONE_ID = os.environ.get('R53_HOSTED_ZONE_ID')
ECS_CLUSTER = os.environ.get('ECS_CLUSTER')

### Optional. Only change if doing something non-standard ###
MC_SERVER_ADDRESS = os.environ.get('MC_SERVER_ADDRESS', R53_RECORD_SET_NAME)
MC_SERVER_RCON_PORT = os.environ.get('MC_SERVER_RCON_PORT', '25565')
ECS_CLUSTER_NAME = ECS_CLUSTER
PULL_TASK_NAME = os.environ.get('PULL_TASK_NAME', 's3-pull')
PUSH_TASK_NAME = os.environ.get('PUSH_TASK_NAME', 's3-push')
MS_SERVER_TASK_NAME = os.environ.get('MS_SERVER_TASK_NAME', 'mc-server')
TICK_MAX = os.environ.get('MS_SERVER_TASK_NAME', 60)

def log(line):
    i = datetime.datetime.now()
    print("[{}] {}".format(i.isoformat(), line))


def port_is_listening(ip, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        success = None
        try:
                s.connect((ip, int(port)))
                s.shutdown(socket.SHUT_RDWR)
                success = True
        except:
                success = False
        finally:
                s.close()
        
        return success

def wait_wait_for_server():
    while True:
        try:
            if not port_is_listening(MC_SERVER_ADDRESS, MC_SERVER_RCON_PORT):
                time.sleep(1)
                continue
            server = MinecraftServer.lookup(
                "{}:{}".format(MC_SERVER_ADDRESS, MC_SERVER_RCON_PORT)
            )
            status = server.status()
            status.players.online
        except:
            time.sleep(1)
            continue
        return

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
        if ticks > TICK_MAX:
            log("Player count has been 0 for 60 consecutive ticks.")
            return
        time.sleep(10)

def scale_asg(asg_name, count):
    client = boto3.client('autoscaling')
    response = client.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
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
    log("DEBUG: {}".format(response))
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

def get_cluster_instance_public_ip(asg_name):
    asg = boto3.client('autoscaling')
    response = asg.describe_auto_scaling_groups(
        AutoScalingGroupNames=[
            asg_name,
        ]
    )
    instance_id = response['AutoScalingGroups'][0]['Instances'][0]['InstanceId']
    ec2 = boto3.resource('ec2')
    instance = ec2.Instance(instance_id)
    return instance.public_ip_address

    

def create_record_set(asg_name):
    client = boto3.client('route53')
    public_ip = get_cluster_instance_public_ip(asg_name)
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

def delete_record_set(asg_name):
    client = boto3.client('route53')
    public_ip = get_cluster_instance_public_ip(asg_name)
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

def get_asg_name():
    ecs_client = boto3.client('ecs')
    response = ecs_client.list_container_instances(
        cluster=ECS_CLUSTER,
    )
    instance_arn = response['containerInstanceArns'][0]
    response = ecs_client.describe_container_instances(
        cluster=ECS_CLUSTER,
        containerInstances=[
            instance_arn,
        ]
    )
    instance_id = response['containerInstances'][0]['ec2InstanceId']
    ec2_client = boto3.client('ec2')
    response = ec2_client.describe_instances(
        InstanceIds=[
            instance_id,
        ]
    )
    tags = response['Reservations'][0]['Instances'][0]['Tags']
    idx = next((index for (index, d) in enumerate(tags) if d["Key"] == "aws:autoscaling:groupName"), None)
    asg = tags[idx]['Value']
    return asg


if __name__ == "__main__":
    asg_name = get_asg_name()
    create_record_set(asg_name)
    pull_task_arn = run_task(PULL_TASK_NAME)
    wait_for_task(pull_task_arn)
    mc_task_arn = run_task(MS_SERVER_TASK_NAME)
    wait_wait_for_server()
    player_watch_loop()
    stop_task(mc_task_arn)
    wait_for_task(mc_task_arn)
    push_task_arn = run_task(PUSH_TASK_NAME)
    wait_for_task(push_task_arn)
    delete_record_set(asg_name)
    scale_asg(asg_name=asg_name, count=0)
    sys.exit(0) 