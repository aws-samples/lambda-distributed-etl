# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as lambda_python,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
)
from constructs import Construct


class TimeSeriesExtractionStack(Stack):
    DATA_LOCATION = "MSG/MDSSFTD/NETCDF/"  # Location of input data in the s3 bucket
    INTERMEDIATE_LOCATION = "output/intermediate/"  # Location of output data from the Lambda process, and input for the Glue job
    OUTPUT_LOCATION = "output/final/"  # Location of output data from the Glue job"

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #########################################
        #
        # S3 bucket
        #
        # to store the data
        #
        #########################################

        # create s3 bucket
        bucket_data = s3.Bucket(self, "DataBucket")

        #########################################
        #
        # Lambda functions
        #
        # to generate the list of days
        # and process each day
        #
        #########################################

        # create lambda function to generate the list of days
        generate_dates_lambda = _lambda.Function(
            self,
            "LambdaFunctionGenerateDates",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="generate-dates.lambda_handler",
            code=_lambda.Code.from_asset("./lambda/generate-dates"),
        )

        # create Lambda function to process a day
        process_day_lambda = lambda_python.PythonFunction(
            self,
            "LambdaFunctionProcessDay",
            entry="./lambda/process-day",
            runtime=_lambda.Runtime.PYTHON_3_10,
            index="process-day.py",
            handler="lambda_handler",
            timeout=Duration.seconds(300),
            memory_size=2048,
            environment={
                "BUCKET_NAME": bucket_data.bucket_name,
                "INPUT_LOCATION": self.DATA_LOCATION,
                "OUTPUT_LOCATION": self.INTERMEDIATE_LOCATION,
            },
        )

        # give the Lambda function permissions to read and write in the bucket
        bucket_data.grant_read_write(process_day_lambda)

        #########################################
        #
        # Step Functions
        #
        # to orchestrate the Lambda functions
        #
        #########################################

        # define the map state as a custom state,
        # because DISTRIBUTED mode is not yet implemented in cdk

        map_json = {
            "Type": "Map",
            "ItemProcessor": {
                "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
                "StartAt": "Process a Date",
                "States": {
                    "Process a Date": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "OutputPath": "$.Payload",
                        "Parameters": {
                            "Payload.$": "$",
                            "FunctionName": f"{process_day_lambda.function_arn}:$LATEST",
                        },
                        "Retry": [
                            {
                                "ErrorEquals": [
                                    "Lambda.ServiceException",
                                    "Lambda.AWSLambdaException",
                                    "Lambda.SdkClientException",
                                    "Lambda.TooManyRequestsException",
                                ],
                                "IntervalSeconds": 2,
                                "MaxAttempts": 6,
                                "BackoffRate": 2,
                            }
                        ],
                        "End": True,
                    }
                },
            },
            "End": True,
            "MaxConcurrency": 365,
            "Label": "Map",
        }

        map_state = sfn.CustomState(self, "Map", state_json=map_json)

        # create the date generation task
        dates_task = tasks.LambdaInvoke(
            self,
            "Generate List of Dates",
            lambda_function=generate_dates_lambda,
            payload_response_only=True,
        )

        # create the Step Function to orchestrate the Lambda functions
        step_function = sfn.StateMachine(
            self,
            "SFOrchestrateLambda",
            definition=sfn.Chain.start(dates_task).next(map_state),
        )

        # grant permission to step_function to start its own execution
        # (required for a distributed map state)
        step_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[
                    f"arn:aws:states:{Stack.of(self).region}:{Stack.of(self).account}:stateMachine:SFOrchestrateLambda*"
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        # grant permission to step_function to invoke the ProcessDay Lambda function
        step_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    process_day_lambda.function_arn,
                    f"{process_day_lambda.function_arn}:*",
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        #########################################
        #
        # Glue Job
        #
        # to repartition the data by point_id
        #
        #########################################

        # create a bucket to upload the code of the Glue job
        bucket_glue = s3.Bucket(self, "GlueBucket")

        # upload the code of the Glue job to the bucket_glue
        s3_deploy.BucketDeployment(
            self,
            "UploadCodeGlue",
            sources=[s3_deploy.Source.asset("./glue_src")],
            destination_bucket=bucket_glue,
            destination_key_prefix="src",
        )

        # create role for the Glue job
        glue_job_role = iam.Role(
            self, "GlueJobRole", assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        # grand read permissions to glue_job_role on src bucket
        bucket_glue.grant_read(glue_job_role)

        # grant read and write permissions to glue_job_role on data bucket
        bucket_data.grant_read_write(glue_job_role)

        # create Glue job to process the data
        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            role=glue_job_role.role_name,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location="s3://{}/src/glue_job.py".format(
                    bucket_glue.bucket_name
                ),
            ),
            description="Glue job to repartition the data",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version="4.0",
            max_retries=0,
            number_of_workers=10,
            timeout=60,
            worker_type="G.1X",
            default_arguments={
                "--bucket_name": bucket_data.bucket_name,
                "--input_location": self.INTERMEDIATE_LOCATION,
                "--output_location": self.OUTPUT_LOCATION,
            },
        )
