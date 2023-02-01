from aws_cdk import Duration
from constructs import Construct
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sft
from aws_cdk import aws_events as events
from stacks.shared.constructs.pyshell_job import Pyshell

from stacks.shared.constructs.sns_config import SnsConfig
from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct


class RetrievalStepFunction(TahoeStepFunctionConstruct):
    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context, sns_config: SnsConfig,
                 sub_path, *, dpu=0.0625, log=True, custom_download_function=None, url=None, email_on_success=False):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)

        if custom_download_function is None:
            download = Pyshell(self, "downloadFn",
                               build_context, data_prefix, base_context, "load", self.sub_path, sub_folder="download",
                               BASE_URL=url, DATA_PREFIX=self.data_prefix,
                               S3_BUCKET=self.base_context.get_raw_bucket().bucket_name, dpu=dpu)
        else:
            download = custom_download_function
        success = sft.SnsPublish(
            self, "sns", topic=sns_config.download_done_topic, message=sf.TaskInput.from_text("$"))
        self.definition = download.definition.add_catch(handler=self.status_function, result_path="$.errors")

        # If success emails are requested, add them here
        if email_on_success:
            success = self.base_context.functions.pipeline_success_notification_task(self, "success_notification",
                                                                                     "success_notification",
                                                                                     self.data_prefix).next(success)

        if log:
            success = self.base_context.functions.pipeline_database_task(self, "download", "DOWNLOADED",
                                                                         self.data_prefix).next(success)

        self.set_custom_definition(download.definition.next(success))

    def build_stepfunction(self):
        super().build_stepfunction()
        self.timer_trigger(events.Schedule.rate(Duration.days(1)))
