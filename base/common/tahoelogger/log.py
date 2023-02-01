from enum import Enum, auto
import logging
import boto3
import queue
from queue import Empty
import threading
import time
import json
import sys
from sys import getsizeof
import time
import io
import boto3
from botocore.exceptions import ClientError
import time


class Log():
    def __str__(self):
        """
        String representation which is dictonary of class attributes

        :return string representation of attributes: 
        """
        return str(self.__dict__)


class DbLog(Log):
    def __init__(self, data_prefix):
        """
        Create db log.
        :param data_prefix: datasource prefix
        """
        self.data_prefix = data_prefix
        self.transactional = False
        self.client = None

    def dynamo(self, timestamp):
        """
        Dynamo representation.
        :param timestamp: timestamp of dynamo
        """
        pass

    def from_dict(cls, definition):
        """
        Class generator from python dictionary.

        :param cls: Class to translate to
        :param definition: dictionary of strings
        """
        pass

    def is_transactional(self):
        """
        If the db log uses transactions.

        :returns: self.transactional
        """
        return self.transactional

    def get_data_prefix(self):
        """
        Get data prefix.

        :returns: self.data_prefix
        """
        return self.data_prefix

    def set_client(self, client):
        """
        Set client for transactional requirement.

        :param client: Client for database calls
        """
        self.client = client


class DbLogLock(DbLog):
    def __init__(self, data_prefix):
        """
        Create db log lock.

        :param data_prefix: datasource prefix

        """
        self.data_prefix = data_prefix
        self.transactional = True

    def lock(self, table, key, owner):
        """
        Create or update lock to lock resource from other tasks.

        :param table: lock table
        :param key: Item to lock 
        :param owner: Lock owner
        """
        is_new = False
        try:
            # Try creating lock if first occurrence of key
            new_lock = self.client.transact_write_items(TransactItems=[{
                'Put': {
                    'TableName': table,
                    'Item': {
                        'PK': {
                            'S': key,
                        },
                        'SK': {
                            'S': 'LOCK'
                        },
                        'RecordLock': {
                            'BOOL': True
                        },
                        'LockCount': {
                            'N': "0"
                        },
                        'LockOwner': {
                            'S': owner
                        }
                    },
                    'ConditionExpression': 'attribute_not_exists(PK)'
                }

            }])
            print("locking successfull")
            is_new = True
        except ClientError as e:
            print("client_error", e)
            if 'ConditionalCheckFailedException' in str(e):
                print("Lock already exists")
        except Exception as e:
            print("generic_error", str(e))

        backoff = 2
        wait_time = 10
        while not is_new:
            try:
                response = self.client.transact_write_items(TransactItems=[
                    {
                        'Update': {
                            'TableName': table,
                            'Key': {
                                'PK': {
                                    'S': key,
                                },
                                'SK': {
                                    'S': 'LOCK',
                                }
                            },
                            'UpdateExpression': 'ADD LockCount :inc SET RecordLock = :stat',
                            'ConditionExpression': 'RecordLock = :false',

                            'ExpressionAttributeValues': {
                                ':inc': {'N': '1'},
                                ':stat': {'BOOL': True},
                                ':false': {'BOOL': False}
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    }
                ])
                print("locking update successfull")
                break
            except ClientError as e:
                print("client error", e)
                if 'ConditionalCheckFailedException' in str(e) or "TransactionConflict" in str(e):
                    print(f"Lock in use backoff for {wait_time} seconds")
                    time.sleep(wait_time)
                    wait_time = wait_time * backoff
                else:
                    raise e

    def unlock(self, table, key):
        """
        Unlock lock to allow other tasks to use lock.

        :param table: lock table
        :param key: Item to lock 
        """
        backoff = 2
        wait_time = 10
        while True:
            try:
                # Unlock if key is locked
                self.client.transact_write_items(TransactItems=[
                    {
                        'Update': {
                            'TableName': table,
                            'Key': {
                                'PK': {
                                    'S': key,
                                },
                                'SK': {
                                    'S': 'LOCK'
                                }
                            },
                            'UpdateExpression': 'SET RecordLock = :stat, LockOwner = :na',
                            'ConditionExpression': 'RecordLock = :true',

                            'ExpressionAttributeValues': {
                                ':na': {'S': 'NA'},
                                ':stat': {'BOOL': False},
                                ':true': {'BOOL': True}
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    }
                ])
                print("unlocking")
                break
            except ClientError as e:
                print("client error", e)
                if 'ConditionalCheckFailed' in str(e) or "TransactionConflict" in str(e):
                    print(f"Lock in use backoff for {wait_time} seconds")
                    time.sleep(wait_time)
                    wait_time = wait_time * backoff
                else: 
                    raise e


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        """
        Create value from name.
        """
        return name


class Status(AutoName):
    CRAWLED = auto()
    VALIDATED = auto()
    RETRIVED = auto()
    FROZEN = auto()
    DOWNLOADED = auto()
    RELATIONALIZED = auto()
    PREPROCESSED = auto()


class StatusLog(DbLog):
    def __init__(self, data_prefix: str, status: Status, token=None):
        """
        Status log.

        :param data_prefix: datasource prefix
        :param status: Status of datasource
        :param token: Waiting token to send
        """
        super().__init__(data_prefix)
        self.status = status.value
        self.token = token

    @classmethod
    def from_dict(cls, definition):
        obj = cls.__new__(cls)
        super(StatusLog, obj).__init__(definition["data_prefix"])
        obj.status = Status[definition["status"]].value
        obj.token = definition["token"] if 'token' in definition else None
        return obj

    def dynamo(self, timestamp):
        dynamo_record = {}
        dynamo_record["PK"] = self.data_prefix
        dynamo_record["SK"] = f"LOG#{timestamp}"
        dynamo_record["token"] = self.token
        dynamo_record["datasource"] = self.data_prefix
        dynamo_record["status"] = self.status
        return dynamo_record


class IndexLog(DbLogLock):
    def __init__(self, data_prefix: str, name: str, description: str, hash: str, query: str, index_location: str):
        super().__init__(data_prefix)
        self.name = name
        self.description = description
        self.hash = hash
        self.query = query
        self.index_location = index_location

    @classmethod
    def from_dict(cls, definition):
        obj = cls.__new__(cls)
        super(IndexLog, obj).__init__(definition["data_prefix"])
        obj.name = definition["name"]
        obj.description = definition["description"]
        obj.hash = definition["hash"]
        obj.query = definition["query"]
        obj.index_location = definition["index_location"]
        return obj

    def lock(self, table):
        print("locking")
        super().lock(table, self.index_location[5:].replace("/", "_"), self.hash)

    def unlock(self, table):
        super().unlock(table, self.index_location[5:].replace("/", "_"))

    def lock_task(self):
        """Task to do while record is locked."""
        split_s3_url = self.index_location[5:].split("/", 1)
        bucket = split_s3_url[0]
        key = split_s3_url[1]
        print("bucket", bucket, "key", key)
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, key)
        body = []
        try:
            file = obj.get()["Body"]
        except ClientError:
            obj.put(Body="".encode('ascii'))
            file = obj.get()["Body"]
            print("Creating new file")
        with io.BytesIO(file.read()) as f:
            for line in f.readlines():
                if line.decode() == "\n":
                    continue
                json_line = json.loads(line.decode())
                if self.hash == json_line['hash']:
                    hash_exists = True
                    return
                body.append(line.decode())
            body.append(json.dumps({
                "name": self.name,
                "description": self.description,
                "hash": self.hash,
                "query": self.query
            }))
            obj.put(Body="\n".join(body).encode('ascii'))


class NotifyLog(Log):
    def __init__(self, data_prefix: str, message: str, subject: str, sns: str = None):
        self.data_prefix = data_prefix
        self.sns = sns
        self.message = message
        self.subject = subject

    def format(self):
        return {"TopicArn": self.sns, "Message": self.message, "Subject": self.subject}

    @classmethod
    def from_dict(cls, definition):
        obj = cls.__new__(cls)
        obj.data_prefix = definition["data_prefix"]  
        obj.message = definition["message"]
        obj.sns = definition["sns"]
        obj.subject = definition["subject"]
        return obj


class NotifyQueryLog(NotifyLog):
    def __init__(self, data_prefix: str, query, additional_details, sns: str = None):
        """
        Formats Notify with Query details

        :param data_prefix: datasource prefix
        :param query:   The query to notify about
        :param additional_details: Query metadata 
        :param sns: Email sns 
        """
        subject = f"Query {data_prefix}: {additional_details['hash']}"
        message = f"The query {query} is completed and { 'persisted' if additional_details['persist'] else 'not persisted'} under the location {additional_details['location']}"
        super().__init__(data_prefix, message, subject, sns)


# # required for pyshell jobs which cannot post cloudwatch logs to its own log group
class CloudwatchLogger(logging.Handler):
    """
    Enables python logging with cloudwatch with services that don't inherently support it
    """
    def _cFilter(record):
        if not record.name.startswith("TahoeLogger"):
            return False
        return True

    def __init__(self, log_group_name: str = None, stream_name: str = None, session=None):
        """
        Create cloudwatch logging. Assumes log group is present by name creates a single log stream.

        :param log_group_name: Log group name to send logs to
        :param stream_name: Stream name to create usually job run id
        """
        super().__init__()
        self.log_streaming = True
        self.log_group_name = log_group_name
        self.queue, self.token = None, None
        self.threads = []
        self._defaultFormatter = ""
        self.addFilter(CloudwatchLogger._cFilter)
        self.done = 1
        self.flushed = 2
        if session:
            self.client = session.client("logs")
        else:
            self.client = boto3.client("logs")
        self.stream_name = stream_name
        self.is_creating_log_stream = True
        self._create_log_stream()
        self.is_creating_log_stream = False

        self.createLock()

    def createLock(self):
        super().createLock()

    def _create_log_stream(self):
        # creates log stream
        try:
            self.client.create_log_stream(logGroupName=self.log_group_name, logStreamName=self.stream_name)
            print("creating")
        except Exception as e:
            print("Failed creating log stream")
            raise ValueError("Failed to create log stream")

    def _log(self, queue, stream_name, log_group_name):
        """
        Manage logging queue
        """

        while True:
            events_batch = []
            batch_size = 0
            # wait 60 seconds for thread
            thread_start_time = time.time() + 10

            while True:
                try:
                    message = self.queue.get(block=True, timeout=thread_start_time - time.time())
                except Empty:
                    message = None

                if message is not None or message != self.done or message != self.flushed:
                    batch_size_temp = batch_size + getsizeof(message)

                # deliver cases
                if (message is None or message == self.done or message == self.flushed or batch_size_temp > 640000):
                    if len(events_batch) != 0:
                        self._deliver_logs(
                            sorted(events_batch, key=lambda x: x["timestamp"]),
                            stream_name, log_group_name)
                    if batch_size_temp > 640000 and message != self.done and message != self.flushed:
                        self.queue.put(message)
                    elif message is not None:
                        self.queue.task_done()
                    break
                if message is not None:
                    # add message to batch if avaiable
                    events_batch.append(message)
                    self.queue.task_done()
                    batch_size += getsizeof(message)
            if message == self.done:
                break

    def emit(self, record: logging.LogRecord):
        # emit
        if record.getMessage() != "":
            if self.queue is None:
                self.queue = queue.Queue()
                # multiply by 1000 to get linux cloudwatch standard
                # create thread
                thread = threading.Thread(group=None, target=self._log, args=(
                    self.queue, self.stream_name, self.log_group_name))
                thread.daemon = True
                thread.start()
            message = {"timestamp": int(record.created * 1000), "message": self.format(record)}
            self.queue.put(message)
        return

    def _format_log_api(self, batch, stream_name, log_group_name, token):
        """
        Format the logs api

        :param batch: Batch to send
        :param stream_name: Stream to send to
        :param log_group_name: Log group to send to 
        :param token: Next token
        """
        if token is None:
            return {"logGroupName": log_group_name, "logStreamName": stream_name, "logEvents": batch}
        return {"logGroupName": log_group_name, "logStreamName": stream_name, "logEvents": batch,  "sequenceToken": token}

    def _deliver_logs(self, batch, stream_name, log_group_name):
        """
        Sends logs to cloudwatch as a non blocking batch
        """
        token = self.token
        formatted = self._format_log_api(batch, stream_name, log_group_name, token)
        retry = 3
        error = False
        error_reason = None
        for x in range(retry):
            error = False
            try:
                rv = self.client.put_log_events(**formatted)
                if 'nextSequenceToken' in rv:
                    self.token = rv['nextSequenceToken']
                if 'rejectedLogEventsInfo' in rv:
                    print(rv['rejectedLogEventsInfo'])
                break
            except Exception as e:
                error = True
                error_reason = e
        if error:
            raise error_reason

    def flush(self):
        if self.queue is not None:
            self.queue.put(self.flushed)
            self.queue.join()
        return

    def close(self):
        if self.queue is not None:
            self.queue.put(self.done)
            self.queue.join()
        super().close()
        return


class JSONFormat(logging.Formatter):
    def __init__(self, fDict):
        """
        Sets log to use json formatting for easy serialization/deserialization

        :param fDict: formatting of log
        """
        super().__init__()
        self.fmt = fDict

    def usesTime(self) -> bool:
        return "asctime" in self.fmt.values()

    def formatMessage(self, record):
        """
        Formats record using fDict

        :param record: record to format
        """
        return {key: record.__dict__[val] for key, val in self.fmt.items()}

    def format(self, record: logging.LogRecord):
        """
        Formats record for logger

        :param record: Record to format
        """

        # add class type for serialize deserialize rules
        record.__dict__["type"] = type(record.msg).__name__
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        message_dict = self.formatMessage(record)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message_dict["exc_text"] = record.exc_text
        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(message_dict)


class Log4jForwardLogger(logging.Handler):
    def _cFilter(record):
        """
        Prevent propogation of non TahoeLogger generated logs.

        :param record: Record to log
        """
        if not record.name.startswith("TahoeLogger"):
            return False
        return True

    def __init__(self, log4jLogger):
        """
        Propagate to spark loggers.

        :param log4jLogger: Logger to propagate to 
        """
        super().__init__()
        self.logger4j = log4jLogger
        self.addFilter(Log4jForwardLogger._cFilter)

    def emit(self, record):
        """
        Emit record to log4j. All log levels from record INFO to ERROR are treated as warn for log4j.

        :param record: Record to log
        """
        if record.levelno >= logging.CRITICAL:
            self.logger4j.fatal(self.formatter.format(record))
        elif record.levelno >= logging.ERROR:
            self.logger4j.error(self.formatter.format(record))
        elif record.levelno >= logging.WARNING:
            self.logger4j.warn(self.formatter.format(record))
        elif record.levelno >= logging.INFO:
            self.logger4j.warn(self.formatter.format(record))
        elif record.levelno >= logging.DEBUG:
            self.logger4j.debug(self.formatter.format(record))


class TahoeLogger:
    def __init__(self):
        '''
        Create additional tahoe level and formats common with all handlers.
        '''
        self._addAfterInfoLevel(1, "DBLOG")
        self._addAfterInfoLevel(2, "NOTIFY")
        self.logger = logging.getLogger("TahoeLogger")
        self.logger.handlers = []
        self.logger.propagate = False
        self.logger.setLevel(logging.DBLOG)
        self.formatter = JSONFormat({"level": "levelname", "type": "type",
                                    "message": "message", "timestamp": "asctime"})

    def getSparkLogger(self, externalLogger):
        """
        Spark logger.

        :param externalLogger: pyspark based log4j logger
        """
        ch = Log4jForwardLogger(externalLogger)
        ch.setFormatter(self.formatter)
        self.logger.addHandler(ch)
        return self.logger

    def getPyshellLogger(self, group_name, stream_name, session=None):
        """
        Pyshell logger.

        :param group_name: Group name of cloudwatch
        :param stream_name: Stream to send to
        :param session: add boto3 session for local testing
        """
        ch = CloudwatchLogger(group_name, stream_name, session)
        ch.setFormatter(self.formatter)
        self.logger.addHandler(ch)
        return self.logger

    def getStandardLogger(self) -> logging.Logger:
        """
        Standard logger for lambda, glue etl job.
        """
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(self.formatter)
        self.logger.addHandler(ch)
        return self.logger

    def _addAfterInfoLevel(self, addLevel, levelName):
        """
        Adds log level after the info level.

        :param addLevel: How many levels after info to add level. Once a level is used it cannot be used again.
        :param levelName: Name to add method to logger
        """
        if addLevel < 10 and addLevel > 0:
            method = levelName.lower()
            if hasattr(logging, levelName):
                raise AttributeError('{} already defined in logging module'.format(levelName))

            def logLevel(self, message, *args, **kwargs):
                if self.isEnabledFor(logging.INFO + addLevel):
                    self._log(logging.INFO + addLevel, message, args, **kwargs)

            def log(message, *args, **kwargs):
                logging.log(logging.INFO + addLevel, message, *args, **kwargs)

            logging.addLevelName(logging.INFO + addLevel, levelName)
            setattr(logging, method, log)
            setattr(logging, levelName, logging.INFO + addLevel)
            setattr(logging.getLoggerClass(), method, logLevel)
        else:
            raise ValueError("Cannot have more than 10 log levels")


# TODO Local Unit testing of these classes
# TODO split log file from data classes for better grouping
