from airflow.models import Variable
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
#from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator

SNS_ARN = Variable.get('sns_arn')

def PublishSnsMsg(task_id, message='Error on DAG!',subject='Default Subject'):
    return SnsPublishOperator(
        task_id=task_id,
        target_arn=SNS_ARN,
        message=message,
        subject=subject
    )
