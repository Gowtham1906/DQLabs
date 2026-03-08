# GitHub Actions — Required Secrets

Go to: **Repository → Settings → Secrets and variables → Actions → New repository secret**

---

## Testing (current) — GHCR

No external secrets required.
The pipeline uses the built-in `GITHUB_TOKEN` for GHCR authentication.

| Secret | Required | Notes |
|---|---|---|
| `GITHUB_TOKEN` | Auto-provided | Built-in, never configure manually |
| `SEMGREP_APP_TOKEN` | Optional | Semgrep Cloud dashboard; pipeline still runs without it |

---

## Production — Switch to ECR

When ready to push to AWS ECR instead of GHCR:

**1. Add these two secrets:**

| Secret name | Where to get it |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM console → create user `github-actions-ecr` (see policy below) |
| `AWS_SECRET_ACCESS_KEY` | Same IAM user |

**2. In `ci.yml`, update `env:` block:**

```yaml
# Remove:
REGISTRY:   ghcr.io
IMAGE_NAME: dqlabs-airflow

# Replace with:
REGISTRY:   655275087384.dkr.ecr.us-east-1.amazonaws.com
IMAGE_NAME: dqlabs-airflow
AWS_REGION: us-east-1
```

**3. Replace the GHCR login step with ECR steps** (comments labelled `[ECR]` in the workflow file).

---

## Minimum IAM Policy for `github-actions-ecr`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["ecr:GetAuthorizationToken"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload"
      ],
      "Resource": "arn:aws:ecr:us-east-1:655275087384:repository/dqlabs-airflow"
    }
  ]
}
```
