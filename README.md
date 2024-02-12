# What Is This?

A simple python script that uses the [redrive-execution](https://docs.aws.amazon.com/cli/latest/reference/stepfunctions/redrive-execution.html) command to redrive a failed step function execution.

## How To

```bash
python3 -m venv .venv
source .venv/bin/activate.fish
pip install boto3
python script.py --profile YOUR_PROFILE --region YOUR_REGION
```
