# Ubiquiti Video Backup to AWS S3 Bucket

Scripts to backup videos from your UDM pro to AWS S3.

## Walkthrough

1. Install AWS CLI on your UDM pro.  [Follow Amazon's instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

2. Run `aws configure` to enter the credentials necessary to communicate with AWS' APIs in your account.

3. Run `prepare_host.sh` to setup/start the virtual environment, and add a cronjob to run main.py twice daily.

## TODO: 
- why isn't single file uploading
- enable bucket versioning
- upload logger to bucket
- upload all found files of the extension
