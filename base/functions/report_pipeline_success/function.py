import boto3
import os

sns = boto3.client('sns')
email_topic = os.environ['SNS_TOPIC']


def handler(event, context):
    """
    Sends an email informing the user of the job's success
    :param event:
    :return:
    """

    job_name = find_key(event, "JobName")
    execution_time = find_key(event, "ExecutionTime")

    messages = [f"Successful completion of job {job_name}.",
                f"Total job time: {execution_time}ms."]
    send_email("\n".join(messages))


def find_key(dictionary, key):
    """
    Searches for a nested key and returns the value if found.
    This notification job can be called from several jobs with different input parameters,
    so this helps normalize the needed values.

    :param dictionary: The event object of the job
    :param key: The key we're trying to find
    :return: The first instance of the key we find
    """
    # If we've found the key, return its value
    if key in dictionary:
        return dictionary[key]

    # Otherwise, look further into the dict
    for curr in filter(dict.__instancecheck__, dictionary.values()):
        found = find_key(curr, key)
        if found is not None:
            return found

    # If we couldn't find anything in the end, return None
    return None


def send_email(message):
    # Publish the email message to the existing SNS topic
    sns.publish(
        TopicArn=email_topic,
        Message=message
    )
