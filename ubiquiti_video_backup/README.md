# Ubiquiti Video Backup to AWS S3 Bucket

Scripts to backup videos from your UDM pro to AWS S3.

## Walkthrough

1. Install AWS CLI on your UDM pro.  [Follow Amazon's instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

2. Run `aws configure` to enter the credentials necessary to communicate with AWS' APIs in your account.

3. Setup Python environment
```
    npm install -g aws-cdk@2.51.1
    python3 -m venv .venv
    source .venv/bin/activate
    pip3 install -r requirements.txt
    deactivate
```

4. Set an environment variable for the bucket name that will hold the files.
`export S3_BUCKET_NAME=<your_bucket_name_here>`


5. Set a crontab, place the app in the directory in which you need it, set the DIR where the crontab should be run. This runs the script every hour.
```
HOME=/where/the/app/is
0 1-23/2 * * * /root/ubiquiti_video_backup/run.sh
```