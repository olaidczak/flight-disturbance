# Raspberry Pi IAM Setup

The Raspberry Pi publishes only to the existing SNS FIFO topic. It does not
write directly to SQS, S3, Lambda, or DynamoDB.

## 1. Required Policy

In AWS IAM, create a customer-managed policy with this JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublishOpenSkyEvents",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:eu-north-1:592964949672:flight-disruption-events.fifo"
    },
    {
      "Sid": "ReadOpenSkySecret",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:eu-north-1:592964949672:secret:flight-disruption/opensky-*"
    }
  ]
}
```

The policy was checked against the Python AWS calls and the Terraform resource
names. No SQS, S3, Lambda, DynamoDB, or KMS permission is required by the
current Raspberry Pi path.

## 2. AWS Console Steps

1. Open AWS IAM and select **Policies**.
2. Select **Create policy**, then choose the **JSON** editor.
3. Paste the policy above and create it as `flight-disruption-pi-opensky`.
4. Find the IAM identity used by Raspberry Pi.
5. Attach only this policy to that identity.
6. After SNS is verified, remove its old direct-SQS permission.

## 3. Raspberry Pi Check

1. Run `aws sts get-caller-identity`.
2. Confirm that it shows the identity to which the policy was attached.
3. Start one OpenSky poll.
4. A positive `sent` value means SNS accepted the records.
5. An `AccessDenied` error means the policy is attached to a different identity
   or the resource ARN does not match.

Never put AWS access keys or OpenSky credentials in Git. Prefer temporary,
automatically rotated AWS credentials. If permanent IAM user keys are currently
used, store them only in the local AWS credentials file and rotate them.
