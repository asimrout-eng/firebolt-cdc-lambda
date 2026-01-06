#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# SNS Topic Setup Script for Firebolt CDC Alerts
# ═══════════════════════════════════════════════════════════════════════════════
#
# This script creates SNS topics for:
# 1. Schema Evolution alerts (new columns detected)
# 2. CDC Processing failures
# 3. General CDC monitoring
#
# Usage:
#   chmod +x setup_sns_alerts.sh
#   ./setup_sns_alerts.sh
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - sns:CreateTopic, sns:Subscribe permissions
#
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Configuration
REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "UNKNOWN")
PREFIX="firebolt-cdc"

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Firebolt CDC - SNS Alert Setup"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo ""

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Schema Evolution Topic
# ═══════════════════════════════════════════════════════════════════════════════

echo "Creating Schema Evolution SNS Topic..."
SCHEMA_TOPIC_ARN=$(aws sns create-topic \
    --name "${PREFIX}-schema-evolution" \
    --region $REGION \
    --query 'TopicArn' \
    --output text)

echo "✓ Created: $SCHEMA_TOPIC_ARN"

# ═══════════════════════════════════════════════════════════════════════════════
# 2. CDC Failures Topic
# ═══════════════════════════════════════════════════════════════════════════════

echo "Creating CDC Failures SNS Topic..."
FAILURES_TOPIC_ARN=$(aws sns create-topic \
    --name "${PREFIX}-failures" \
    --region $REGION \
    --query 'TopicArn' \
    --output text)

echo "✓ Created: $FAILURES_TOPIC_ARN"

# ═══════════════════════════════════════════════════════════════════════════════
# 3. General Monitoring Topic
# ═══════════════════════════════════════════════════════════════════════════════

echo "Creating General Monitoring SNS Topic..."
MONITORING_TOPIC_ARN=$(aws sns create-topic \
    --name "${PREFIX}-monitoring" \
    --region $REGION \
    --query 'TopicArn' \
    --output text)

echo "✓ Created: $MONITORING_TOPIC_ARN"

# ═══════════════════════════════════════════════════════════════════════════════
# 4. Subscribe Email (Interactive)
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "Email Subscription"
echo "═══════════════════════════════════════════════════════════════════════════════"

read -p "Enter email address for alerts (or press Enter to skip): " ALERT_EMAIL

if [ -n "$ALERT_EMAIL" ]; then
    echo ""
    echo "Subscribing $ALERT_EMAIL to all topics..."
    
    # Subscribe to Schema Evolution
    aws sns subscribe \
        --topic-arn $SCHEMA_TOPIC_ARN \
        --protocol email \
        --notification-endpoint $ALERT_EMAIL \
        --region $REGION > /dev/null
    echo "✓ Subscribed to schema evolution alerts"
    
    # Subscribe to Failures
    aws sns subscribe \
        --topic-arn $FAILURES_TOPIC_ARN \
        --protocol email \
        --notification-endpoint $ALERT_EMAIL \
        --region $REGION > /dev/null
    echo "✓ Subscribed to failure alerts"
    
    # Subscribe to Monitoring
    aws sns subscribe \
        --topic-arn $MONITORING_TOPIC_ARN \
        --protocol email \
        --notification-endpoint $ALERT_EMAIL \
        --region $REGION > /dev/null
    echo "✓ Subscribed to monitoring alerts"
    
    echo ""
    echo "⚠️  IMPORTANT: Check your email and confirm the subscription!"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 5. Create CloudWatch Alarms
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "CloudWatch Alarms"
echo "═══════════════════════════════════════════════════════════════════════════════"

read -p "Enter Lambda function name (default: firebolt-cdc-processor): " LAMBDA_NAME
LAMBDA_NAME="${LAMBDA_NAME:-firebolt-cdc-processor}"

echo ""
echo "Creating CloudWatch alarms for $LAMBDA_NAME..."

# Alarm 1: High Error Rate
aws cloudwatch put-metric-alarm \
    --alarm-name "${PREFIX}-high-errors" \
    --alarm-description "Alert when CDC Lambda error rate exceeds threshold" \
    --metric-name "Errors" \
    --namespace "AWS/Lambda" \
    --dimensions Name=FunctionName,Value=$LAMBDA_NAME \
    --statistic Sum \
    --period 300 \
    --threshold 10 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2 \
    --alarm-actions $FAILURES_TOPIC_ARN \
    --region $REGION 2>/dev/null && echo "✓ Created high-errors alarm" || echo "⚠️ Could not create high-errors alarm"

# Alarm 2: Slow Processing
aws cloudwatch put-metric-alarm \
    --alarm-name "${PREFIX}-slow-processing" \
    --alarm-description "Alert when average processing time exceeds 5 minutes" \
    --metric-name "Duration" \
    --namespace "AWS/Lambda" \
    --dimensions Name=FunctionName,Value=$LAMBDA_NAME \
    --statistic Average \
    --period 300 \
    --threshold 300000 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 3 \
    --alarm-actions $MONITORING_TOPIC_ARN \
    --region $REGION 2>/dev/null && echo "✓ Created slow-processing alarm" || echo "⚠️ Could not create slow-processing alarm"

# Alarm 3: Throttles
aws cloudwatch put-metric-alarm \
    --alarm-name "${PREFIX}-throttles" \
    --alarm-description "Alert when Lambda is being throttled" \
    --metric-name "Throttles" \
    --namespace "AWS/Lambda" \
    --dimensions Name=FunctionName,Value=$LAMBDA_NAME \
    --statistic Sum \
    --period 300 \
    --threshold 5 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --alarm-actions $MONITORING_TOPIC_ARN \
    --region $REGION 2>/dev/null && echo "✓ Created throttles alarm" || echo "⚠️ Could not create throttles alarm"

# Alarm 4: No Activity
aws cloudwatch put-metric-alarm \
    --alarm-name "${PREFIX}-no-activity" \
    --alarm-description "Alert when no CDC files processed for 2 hours" \
    --metric-name "Invocations" \
    --namespace "AWS/Lambda" \
    --dimensions Name=FunctionName,Value=$LAMBDA_NAME \
    --statistic Sum \
    --period 7200 \
    --threshold 1 \
    --comparison-operator LessThanThreshold \
    --evaluation-periods 1 \
    --treat-missing-data breaching \
    --alarm-actions $MONITORING_TOPIC_ARN \
    --region $REGION 2>/dev/null && echo "✓ Created no-activity alarm" || echo "⚠️ Could not create no-activity alarm"

# ═══════════════════════════════════════════════════════════════════════════════
# 6. Output Summary
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "SETUP COMPLETE"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "SNS Topics Created:"
echo "  Schema Evolution: $SCHEMA_TOPIC_ARN"
echo "  CDC Failures:     $FAILURES_TOPIC_ARN"
echo "  Monitoring:       $MONITORING_TOPIC_ARN"
echo ""
echo "CloudWatch Alarms Created:"
echo "  - ${PREFIX}-high-errors"
echo "  - ${PREFIX}-slow-processing"
echo "  - ${PREFIX}-throttles"
echo "  - ${PREFIX}-no-activity"
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "LAMBDA ENVIRONMENT VARIABLES"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Add these to your Lambda configuration:"
echo ""
echo "  SCHEMA_EVOLUTION_ENABLED=true"
echo "  SCHEMA_EVOLUTION_SNS_TOPIC=$SCHEMA_TOPIC_ARN"
echo ""
echo "AWS CLI command to update Lambda:"
echo ""
cat << EOF
aws lambda update-function-configuration \\
    --function-name $LAMBDA_NAME \\
    --environment "Variables={
        SCHEMA_EVOLUTION_ENABLED=true,
        SCHEMA_EVOLUTION_SNS_TOPIC=$SCHEMA_TOPIC_ARN,
        FIREBOLT_ACCOUNT=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.FIREBOLT_ACCOUNT' --output text),
        FIREBOLT_DATABASE=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.FIREBOLT_DATABASE' --output text),
        FIREBOLT_ENGINE=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.FIREBOLT_ENGINE' --output text),
        FIREBOLT_CLIENT_ID=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.FIREBOLT_CLIENT_ID' --output text),
        FIREBOLT_CLIENT_SECRET=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.FIREBOLT_CLIENT_SECRET' --output text),
        LOCATION_NAME=\$(aws lambda get-function-configuration --function-name $LAMBDA_NAME --query 'Environment.Variables.LOCATION_NAME' --output text)
    }" \\
    --region $REGION
EOF
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""

# Save config to file
CONFIG_FILE="sns_config_${REGION}.txt"
cat > $CONFIG_FILE << EOF
# Firebolt CDC SNS Configuration
# Generated: $(date)
# Region: $REGION

SCHEMA_EVOLUTION_SNS_TOPIC=$SCHEMA_TOPIC_ARN
CDC_FAILURES_SNS_TOPIC=$FAILURES_TOPIC_ARN
MONITORING_SNS_TOPIC=$MONITORING_TOPIC_ARN

# Lambda Environment Variables:
SCHEMA_EVOLUTION_ENABLED=true
SCHEMA_EVOLUTION_SNS_TOPIC=$SCHEMA_TOPIC_ARN
EOF

echo "Configuration saved to: $CONFIG_FILE"
echo ""
echo "Done! 🎉"

