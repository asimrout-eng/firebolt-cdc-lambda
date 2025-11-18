#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.firebolt_cdc_stack import FireboltCdcStack
from config.config import get_config


app = cdk.App()

# Load configuration
config = get_config()

# Create stack
FireboltCdcStack(
    app, 
    "FireboltCdcStack",
    config=config,
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=config['aws_region']
    ),
    description="Firebolt CDC Lambda - S3 to Firebolt data pipeline"
)

app.synth()



