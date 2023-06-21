# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from time_series_extraction.time_series_extraction_stack import (
    TimeSeriesExtractionStack,
)


@pytest.fixture
def template():
    """
    Generate a mock stack for testing
    """
    app = core.App()
    stack = TimeSeriesExtractionStack(app, "time-series-extraction")
    template = assertions.Template.from_stack(stack)
    return template


def test_s3_bucket_created(template):
    """
    Test for S3 buckets:
        - Data bucket
        - Glue bucket
    """
    template.resource_count_is("AWS::S3::Bucket", 2)


def test_lambda_created(template):
    """
    Test for Lambdas created:
        - generate-dates
        - process-day
        - custom cdk bucket deployment (generated by cdk)
    """
    template.resource_count_is("AWS::Lambda::Function", 3)


def test_step_functions_created(template):
    """
    Test for Step Functions created
    """
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)


def test_glue_job_created(template):
    """
    Test for Glue Job created
    """
    template.resource_count_is("AWS::Glue::Job", 1)