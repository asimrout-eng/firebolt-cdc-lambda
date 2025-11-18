# 🔐 IAM Permissions Required

This document lists all AWS IAM permissions required to deploy and run the Firebolt CDC Lambda function.

---

## 📋 Quick Summary

The IAM user deploying this solution needs these permissions:

| Service | Permissions | Why |
|---------|-------------|-----|
| **Lambda** | Create, Update, Invoke | Deploy Lambda function |
| **IAM** | Create Role, Attach Policies | Create Lambda execution role |
| **S3** | Read, Notification Config | Configure S3 trigger |
| **CloudWatch Logs** | Create, Write | Lambda logging |

---

## ✅ Option 1: Managed Policies (Simplest)

Attach these AWS managed policies to your IAM user:

```bash
# User running deployment needs these policies
aws iam attach-user-policy \
  --user-name "YOUR_USERNAME" \
  --policy-arn "arn:aws:iam::aws:policy/AWSLambda_FullAccess"

aws iam attach-user-policy \
  --user-name "YOUR_USERNAME" \
  --policy-arn "arn:aws:iam::aws:policy/IAMFullAccess"

aws iam attach-user-policy \
  --user-name "YOUR_USERNAME" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"
```

**Pros:**
- ✅ Simple to apply
- ✅ Covers all scenarios
- ✅ AWS maintains the policies

**Cons:**
- ⚠️ Broader permissions than strictly needed
- ⚠️ May not comply with least-privilege requirements

---

## ✅ Option 2: Custom Policy (Least Privilege - Recommended)

Create a custom policy with only the required permissions:

### JSON Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LambdaDeployment",
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:DeleteFunction",
        "lambda:GetFunction",
        "lambda:GetFunctionConfiguration",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "lambda:TagResource",
        "lambda:UntagResource",
        "lambda:AddPermission",
        "lambda:RemovePermission",
        "lambda:GetPolicy",
        "lambda:InvokeFunction"
      ],
      "Resource": [
        "arn:aws:lambda:ap-south-1:YOUR_ACCOUNT_ID:function:firebolt-cdc-processor"
      ]
    },
    {
      "Sid": "IAMRoleManagement",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:GetRole",
        "iam:DeleteRole",
        "iam:PutRolePolicy",
        "iam:GetRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:ListAttachedRolePolicies",
        "iam:PassRole",
        "iam:TagRole",
        "iam:UntagRole"
      ],
      "Resource": [
        "arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-firebolt-cdc-role"
      ]
    },
    {
      "Sid": "S3BucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetBucketNotification",
        "s3:PutBucketNotification"
      ],
      "Resource": [
        "arn:aws:s3:::fcanalytics",
        "arn:aws:s3:::fcanalytics/*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents",
        "logs:TailLogs"
      ],
      "Resource": [
        "arn:aws:logs:ap-south-1:YOUR_ACCOUNT_ID:log-group:/aws/lambda/firebolt-cdc-processor:*"
      ]
    }
  ]
}
```

### Apply via AWS CLI

```bash
# Save the JSON above to a file, then run:
aws iam put-user-policy \
  --user-name "YOUR_USERNAME" \
  --policy-name "FireboltCDCDeploymentPolicy" \
  --policy-document file://firebolt-cdc-permissions.json
```

**Remember to replace:**
- `YOUR_ACCOUNT_ID` with your AWS account ID
- `YOUR_USERNAME` with the actual IAM username

---

## ✅ Option 3: Via AWS Console (No CLI Access)

If you need to add permissions via AWS Console:

### Step 1: Navigate to IAM

1. Open AWS Console
2. Go to **IAM** service
3. Click **Users** in the left sidebar
4. Click on your username

### Step 2: Add Permissions

Click **Add permissions** → **Attach policies directly**

### Step 3: Attach Policies

**For Managed Policies (Option 1):**
- Search and select: `AWSLambda_FullAccess`
- Search and select: `IAMFullAccess`
- Search and select: `AmazonS3FullAccess`
- Click **Add permissions**

**For Custom Policy (Option 2):**
- Click **Add permissions** → **Create inline policy**
- Click **JSON** tab
- Paste the JSON from Option 2 above
- Replace `YOUR_ACCOUNT_ID` with your account ID
- Click **Review policy**
- Name it: `FireboltCDCDeploymentPolicy`
- Click **Create policy**

---

## 🔍 Verification

After adding permissions, verify the user has access:

```bash
# Test Lambda permissions
aws lambda list-functions --region ap-south-1

# Test IAM permissions
aws iam list-roles --max-items 1

# Test S3 permissions
aws s3 ls s3://fcanalytics/
```

**All commands should succeed without "Access Denied" errors.**

---

## 🚫 Common Permission Errors and Fixes

### Error: `User is not authorized to perform: lambda:CreateFunction`

**Fix:** Add Lambda permissions (Option 1 or 2 above)

### Error: `User is not authorized to perform: iam:CreateRole`

**Fix:** Add IAM role management permissions (Option 1 or 2 above)

### Error: `User is not authorized to perform: s3:PutBucketNotification`

**Fix:** Add S3 bucket notification permissions (Option 1 or 2 above)

### Error: `User is not authorized to perform: ecr:CreateRepository`

**Fix:** This error only appears if using CDK. The manual deployment script does NOT require ECR permissions.

---

## 🔐 Security Best Practices

### 1. Use Least Privilege

Prefer **Option 2 (Custom Policy)** over managed policies when possible.

### 2. Limit Resource Scope

The custom policy is scoped to specific resources:
- Only the `firebolt-cdc-processor` Lambda function
- Only the `lambda-firebolt-cdc-role` IAM role
- Only the `fcanalytics` S3 bucket

### 3. Use IAM Roles Instead of Users

For production deployments, consider using:
- **AWS SSO** with temporary credentials
- **IAM Roles** for cross-account access
- **Service Control Policies (SCPs)** for organization-wide guardrails

### 4. Enable MFA

Require multi-factor authentication (MFA) for IAM users with admin permissions.

### 5. Regular Audits

Periodically review IAM permissions:

```bash
# List policies attached to user
aws iam list-attached-user-policies --user-name "YOUR_USERNAME"

# Review inline policies
aws iam list-user-policies --user-name "YOUR_USERNAME"
```

---

## 📊 Permission Matrix

| Action | Lambda | IAM | S3 | Logs |
|--------|--------|-----|----|----|
| **Deploy Lambda** | ✅ | ✅ | ✅ | ✅ |
| **Update Lambda** | ✅ | ❌ | ❌ | ❌ |
| **Delete Lambda** | ✅ | ✅ | ✅ | ❌ |
| **Monitor Logs** | ❌ | ❌ | ❌ | ✅ |
| **Test Function** | ✅ | ❌ | ❌ | ✅ |

---

## 🆘 Getting Help

### If You Can't Add Permissions Yourself

Contact your AWS account administrator and share this document. Ask them to:

1. **For quick deployment:** Attach the 3 managed policies (Option 1)
2. **For production:** Create the custom policy (Option 2)

### If You're the Administrator

Follow the commands in this document to grant permissions to the deploying user.

---

## ✅ Minimal Permissions Summary

**Absolute minimum permissions needed:**

1. **Lambda:** Create, Update, Configure, AddPermission
2. **IAM:** CreateRole, AttachRolePolicy, PassRole
3. **S3:** GetObject, ListBucket, PutBucketNotification
4. **CloudWatch Logs:** CreateLogGroup, PutLogEvents

**These are all included in the custom policy (Option 2).**

---

## 📋 Quick Copy-Paste Commands

### For Administrator (granting permissions to user)

```bash
# Replace YOUR_USERNAME with actual username
USERNAME="YOUR_USERNAME"

# Option 1: Managed policies (simple)
aws iam attach-user-policy --user-name "$USERNAME" --policy-arn "arn:aws:iam::aws:policy/AWSLambda_FullAccess"
aws iam attach-user-policy --user-name "$USERNAME" --policy-arn "arn:aws:iam::aws:policy/IAMFullAccess"
aws iam attach-user-policy --user-name "$USERNAME" --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"

# Option 2: Custom policy (secure) - requires firebolt-cdc-permissions.json file
aws iam put-user-policy \
  --user-name "$USERNAME" \
  --policy-name "FireboltCDCDeploymentPolicy" \
  --policy-document file://firebolt-cdc-permissions.json
```

### For User (verifying permissions)

```bash
# Check what policies you have
aws iam list-attached-user-policies --user-name "$USER"
aws iam list-user-policies --user-name "$USER"

# Test permissions
aws lambda list-functions --region ap-south-1
aws iam list-roles --max-items 1
aws s3 ls s3://fcanalytics/
```

---

## 🎯 Recommendation

**For initial deployment:**
- Use **Managed Policies (Option 1)** to get running quickly

**For production:**
- Switch to **Custom Policy (Option 2)** for better security

**Both options allow successful deployment of the Firebolt CDC Lambda function!** ✅

---

## 📞 Need Help?

If you encounter permission errors not covered here, please:
1. Copy the exact error message
2. Check which action is denied (e.g., `lambda:CreateFunction`)
3. Add that specific permission using the examples above

**Most permission issues can be resolved by using the Managed Policies (Option 1).**



