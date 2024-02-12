import boto3
from datetime import datetime, timedelta, timezone

# CHANGE THESE VARIABLES
region_name = 'YOUR_REGION_NAME'
account_id = 'YOUR_ACCOUNT_ID'
state_machine_name = 'YOUR_STATE_MACHINE_NAME'

# ARN of the Step Functions state machine
state_machine_arn = 'arn:aws:states:{}:{}:stateMachine:{}'.format(region_name, account_id, state_machine_name)

# Create a Step Functions client
client = boto3.client('stepfunctions', region_name=region_name)

def list_failed_executions(state_machine_arn):
    """List failed executions for a given state machine."""
    failed_executions = []
    next_token = None
    try:
        while True:
            params = {
                'stateMachineArn': state_machine_arn,
                'statusFilter': 'FAILED'
            }
            if next_token:
                params['nextToken'] = next_token
                
            response = client.list_executions(**params)
            failed_executions.extend(response['executions'])
            next_token = response.get('nextToken')
            if not next_token:
                break
    except Exception as e:
        print(f"Error listing executions: {e}")
    
    return failed_executions

def filter_executions_by_start_time(executions, start_time):
    """Filter executions by their start time."""
    filtered_executions = [
        execution for execution in executions
        if execution['startDate'].replace(tzinfo=timezone.utc) >= start_time
    ]
    return filtered_executions

def redrive_failed_executions(executions):
    """Start a new execution for each failed execution provided."""
    for execution in executions:
        try:
            failedExecutionArn = execution['executionArn']
            
            print(f"Starting new execution of {failedExecutionArn}")
            client.redrive_execution(
                executionArn=failedExecutionArn
            )
        except Exception as e:
            print(f"Error redriving execution {failedExecutionArn} => {e}")

if __name__ == "__main__":
    # Calculate the timestamp for 24 hours ago in UTC
    start_time = datetime.now(timezone.utc) - timedelta(days=1)
    
    print(f"Redriving failed executions for state machine {state_machine_arn} started after {start_time}")
    
    # List failed executions
    failed_executions = list_failed_executions(state_machine_arn)
    
    # Filter failed executions by start time
    failed_executions_filtered = filter_executions_by_start_time(failed_executions, start_time)
    
    # Redrive failed executions
    redrive_failed_executions(failed_executions_filtered)
