import boto3
import re
import time

class CloudWatchMetrics(object):
    VALID = "[a-zA-Z0-9_-]+(&[a-zA-Z0-9_-]+=[^&]*)*"

    #metrics aggregation time period in seconds
    def __init__(self, config,aggregation_time_period=59):
        cfg = config["cloudwatch"]
        self.aggregation_time_period = aggregation_time_period
        self.last_sent_timestamp = time.time() #last time metrics were sent to CloudWatch
        self.events_count = 0 #number of events in current aggregation_time_period
        if not re.match(CloudWatchMetrics.VALID, cfg):
            e = ValueError("Bad CloudWatch configuration {0}.  Must match {1}."
                           .format(cfg, CloudWatchMetrics.VALID))
            logger.error(str(e))
            raise e
        cfg = cfg.split('&')
        self._namespace = cfg[0]
        self._dims = []
        for dim in cfg[1:]:
            nv = dim.split('=')
            self._dims.append({ 'Name': nv[0], 'Value': nv[1] })
        self._client = boto3.client('cloudwatch', region_name='us-east-1')
        pass

    def events(self, count):
        time_now = time.time()
        self.events_count += count
        # send put_metric_data if last_sent_timestamp not within aggregation time period
        if(time_now - self.last_sent_timestamp >= self.aggregation_time_period):
            self._client.put_metric_data(
                Namespace = self._namespace,
                MetricData = [
                    {
                        'MetricName': 'EventCount',
                        'Dimensions': self._dims,
                        'Value': self.events_count,
                        'Unit': 'Count'
                    }
                ]
            )
            #update last_sent timestamp and events_count
            self.last_sent_timestamp = time_now
            self.events_count = 0
