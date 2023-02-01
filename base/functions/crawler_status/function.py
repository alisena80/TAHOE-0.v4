import boto3
from tahoelogger import log
glue = boto3.client('glue')
logger = log.TahoeLogger().getStandardLogger()


def handler(event, context):
    """
    Check the status of the crawler to see if completed or requires additional waiting.

    :param event: - information regarding the output to pull from
    :param context:
    :return: - Returns add all additional fields into existing event input field
    """
    print("status()", event["crawler_name"])

    response = glue.get_crawler(Name=event["crawler_name"])

    if (response["Crawler"]["State"] == "RUNNING"):
        # Crawler not complete determine remaining time to be pushed to wait state again
        event["input"]["crawler_status"] = "PENDING"
        metrics = glue.get_crawler_metrics(
            CrawlerNameList=[
                event["crawler_name"]
            ])

        if metrics["CrawlerMetricsList"][0]["StillEstimating"] == False:
            event["input"]["estimatedTime"] = round(
                metrics["CrawlerMetricsList"][0]["TimeLeftSeconds"]) + 20
            logger.info('Estimated Time', )
    else:
        print("Crawler State", response["Crawler"]["State"])
        # Crawler completes or fails and forward to next step or error state
        event["input"]["crawler_status"] = "COMPLETE"
        if response["Crawler"]["State"] != "STOPPING" and "LastCrawl" in response["Crawler"]:
            print("Crawler Completed State",
                  response["Crawler"]["LastCrawl"]["Status"])
            event["input"]["crawler_result"] = response["Crawler"]["LastCrawl"]["Status"]

            if event["input"]["crawler_result"] == "FAILED" or event["input"]["crawler_result"] == "CANCELLED":
                raise response["Crawler"]["LastCrawl"]["ErrorMessage"] if response[
                    "Crawler"]["LastCrawl"]["ErrorMessage"] else "Unknown failure of crawler"
        data_prefix = None
        if "name" in event["input"]:
            data_prefix = event["input"]["name"]
        elif "data" in event["input"]:
            data_prefix = event["input"]["data"]["type"]
        else:
            data_prefix = event["input"]["data_prefix"]
        if data_prefix is not None:
            logger.dblog(log.StatusLog(data_prefix, log.Status.CRAWLED))
        else:
            print("Could not log to tahoe metastore")

    return event["input"]
