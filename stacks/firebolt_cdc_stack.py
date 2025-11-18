from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_logs as logs,
    CfnOutput,
)
from constructs import Construct


class FireboltCdcStack(Stack):
    """AWS CDK Stack for Firebolt CDC Lambda"""

    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import existing S3 bucket
        bucket = s3.Bucket.from_bucket_name(
            self, "CdcBucket",
            bucket_name=config['s3_bucket_name']
        )

        # Lambda execution role
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"{config['lambda_function_name']}-role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Grant S3 read permissions
        bucket.grant_read(lambda_role)

        # Lambda Layer for dependencies
        layer = lambda_.LayerVersion(
            self, "FireboltSdkLayer",
            code=lambda_.Code.from_asset("lambda-layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="Firebolt SDK and dependencies",
            layer_version_name="firebolt-sdk-layer"
        )

        # Lambda Function
        lambda_function = lambda_.Function(
            self, "CdcProcessor",
            function_name=config['lambda_function_name'],
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            role=lambda_role,
            timeout=Duration.seconds(config['lambda_timeout']),
            memory_size=config['lambda_memory_size'],
            layers=[layer],
            environment={
                "FIREBOLT_ACCOUNT": config.get('firebolt_account', ''),
                "FIREBOLT_DATABASE": config.get('firebolt_database', ''),
                "FIREBOLT_ENGINE": config.get('firebolt_engine', ''),
                "FIREBOLT_USERNAME": config.get('firebolt_username', ''),
                "FIREBOLT_PASSWORD": config.get('firebolt_password', ''),
                "LOCATION_NAME": config.get('location_name', 's3_raw_dms'),
                "TABLE_KEYS_S3_BUCKET": config.get('table_keys_s3_bucket', ''),
                "TABLE_KEYS_S3_KEY": config.get('table_keys_s3_key', ''),
                "CDC_DELETE_COLUMN": config.get('cdc_delete_column', ''),
                "CDC_DELETE_VALUES": config.get('cdc_delete_values', ''),
            },
            log_retention=logs.RetentionDays.TWO_WEEKS,
            description="Processes CDC files from S3 into Firebolt"
        )

        # S3 Event Notification
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_function),
            s3.NotificationKeyFilter(
                prefix=config['s3_prefix'],
                suffix=".parquet"
            )
        )

        # Outputs
        CfnOutput(self, "LambdaFunctionName", value=lambda_function.function_name)
        CfnOutput(self, "LambdaFunctionArn", value=lambda_function.function_arn)
        CfnOutput(self, "S3Trigger", value=f"s3://{config['s3_bucket_name']}/{config['s3_prefix']}*.parquet")
        CfnOutput(self, "LogGroup", value=lambda_function.log_group.log_group_name)



