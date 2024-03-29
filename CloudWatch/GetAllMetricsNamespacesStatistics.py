# This is an inefficient way to determine what CloudWatch namespaces are receiving the most events over a given time. Used to understand what could be contributing to PutMetricData cost.
# ListMetrics and GetMetricStatistics each cost $0.01 per 1,000 requests. Running this script can cost in excess of $100. Best to query only the namespaces you think are most active.

import boto3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize a CloudWatch client
cw_client = boto3.client('cloudwatch')

def fetch_metric_data(namespace, metric_name):
    """
    Fetch metric data for a given namespace and metric name.
    """
    try:
        response = cw_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            StartTime=datetime.utcnow() - timedelta(hours=1),  # Last 1 hour
            EndTime=datetime.utcnow(),
            Period=600,  # Example: Adjust based on your needs
            Statistics=['SampleCount']
        )
        sample_count = sum(datapoint.get('SampleCount', 0) for datapoint in response['Datapoints'])
        return namespace, sample_count
    except Exception as e:
        print(f"Error fetching data for {namespace}/{metric_name}: {e}")
        return namespace, 0

def get_namespace_put_metric_data_calls():
    # Dictionary to hold PutMetricData call counts for each namespace
    namespace_put_calls = {}

    # Use ThreadPoolExecutor to parallelize API calls
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Prepare future tasks for metrics data fetching
        future_tasks = []
        paginator = cw_client.get_paginator('list_metrics')
        for page in paginator.paginate():
            for metric in page['Metrics']:
                future_tasks.append(executor.submit(fetch_metric_data, metric['Namespace'], metric['MetricName']))
        
        # Process as tasks complete
        for future in as_completed(future_tasks):
            namespace, count = future.result()
            if count > 0:  # Only consider namespaces with PutMetricData calls
                namespace_put_calls[namespace] = namespace_put_calls.get(namespace, 0) + count

    return namespace_put_calls

namespace_put_calls = get_namespace_put_metric_data_calls()
print("PutMetricData calls per namespace:")
for namespace, count in namespace_put_calls.items():
    print(f"{namespace}: {count} calls")
