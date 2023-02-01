import boto3
glue = boto3.client('glue')


def handler(event, context):
    '''
    Starts the glue crawler in order to generate schema

    :param event: - information regarding the output to pull from
    :param context:
    :return event["input"]: - Returns add all additional fields into existing event input field
        
    '''
    # start crawler
    print("crawl() ", event["crawler_name"])
    glue.start_crawler(Name=event["crawler_name"])
    response = glue.get_crawler_metrics(
    CrawlerNameList=[
        event["crawler_name"]
    ])
    # Estimate runtime to pass to wait
    event["input"]["estimatedTime"] = round(response["CrawlerMetricsList"][0]["MedianRuntimeSeconds"]) + 20
    print(event["input"])
    return event["input"]