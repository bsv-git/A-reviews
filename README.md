# A-reviews
# Local PySpark Development Setup

This directory contains local development scripts that mirror the AWS Glue jobs. These scripts allow you to develop, test, and debug the data pipeline on your local machine before deploying to AWS.

## Why Local First?

- **Cost**: Running PySpark locally is free. AWS Glue testing costs $0.44 per run.
- **Speed**: Local iteration takes 10-60 seconds vs 5+ minutes on AWS.
- **Debugging**: Full Python debugger access, instant feedback.
- **Learning**: Understand each transformation step-by-step.

## Quick Start

### 1. Set Up Virtual Environment

```bash
cd /home/srieubuntu/A-reviews
python3 -m venv .venv
source .venv/bin/activate
pip install -r local-dev/requirements.txt