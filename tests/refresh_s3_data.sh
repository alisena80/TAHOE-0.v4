#!/usr/bin/env bash

echo "******************************"
# List all s3 buckets in a project
echo "List all Tahoe buckets"
aws s3 ls

# move out old files
echo "Moving old data to raw archive"
aws s3 mv s3://tahoeqa-raw-data s3://tahoeqa-raw-archive --recursive
echo "Moving old data to interim archive"
aws s3 mv s3://tahoeqa-interim-data s3://tahoeqa-interim-archive --recursive

# Copy s3 folder from bucket to another s3 bucket
echo "Copying dev raw nvd data to QA bucket"
#-$(date +"%d-%m-%y"
aws s3 cp s3://tahoegarg4uw1-s3-raw/nvd s3://tahoeqa-raw-data/nvd --recursive --exclude "*.meta"
echo "Copying dev interim nvd data to QA bucket"
#-$(date +"%d-%m-%y"
aws s3 cp s3://tahoegarg4uw1-s3-interim/nvd s3://tahoeqa-interim-data/nvd --recursive


# #. govt s3 buckets
# # Copy s3 folder from bucket to another s3 bucket
# echo "Copying dev raw nvd data to QA bucket"
# #-$(date +"%d-%m-%y"
# aws s3 cp s3://tahoegarg4ugw1-s3-raw/nvd s3://tahoeqa-raw-data/nvd --recursive --exclude "*.meta"
# echo "Copying dev interim nvd data to QA bucket"
# #-$(date +"%d-%m-%y"
# aws s3 cp s3://tahoegarg4ugw1-s3-interim/nvd s3://tahoeqa-interim-data/nvd --recursive

echo "Refresh TahoeQA data complete."
echo "******************************"

