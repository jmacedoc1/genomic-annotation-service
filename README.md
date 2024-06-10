# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in a "Cloud Computing" course capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Archive process

1. When we finish an annotation job and set the `job_status` to "COMPLETED", we send a message to the `josemaria_glacier` SNS topic. We also have a SQS queue with the same name that's subscribed to this SNS topic.

3. The `archive.py` file is running on a tmux session. This file handles the logic or archiving the files of free users:

   a. First, it dequeues the messages in `josemaria_glacier` SQS queue and extracts the neccesary keys from the message, like the job's `complete_time`.  
   b. We use the `complete_time` to estimate when we have to archive the file in Glacier (`free_user_time_limit` variable).  
   c. If the current time is greater or equal than the `free_user_time_limit` variable then we get the file object from the S3 `mpcs-cc-gas-results` bucket, upload it to Glacier's vault and persist the   archive ID as `results_file_archive_id` in our DynamoDB table `josemaria_annotations`.  
   d. Finally, we delete the file object from S3 `mpcs-cc-gas-results` bucket.  

## Restore process

*NOTE: this process didn't seem to be working fully when the last changes were pushed to this repository.*

1. When a free user upgrades to a premium account, a POST request is sent to our `/subscribe` endpoint (which is in the `views.py` script of our web instance):

   a. We get a list of the user's archived jobs by querying the `josemaria_annotations` using the `user_id`as a secondary index and filtering by the `job_status`.  
   b. Once we have the list of archived jobs we iterate over them to send a message to our `josemaria_restore` SNS topic with our `results_file_archive_id` and `s3_key_result_file`.  
   c. We also update the user's profile in our `josemaria_annotations` table.

3. In the `restore.py` script we:  

   a. Dequeue the messages in `josemaria_restore` SQS queue and extract the neccesary keys from the message, like the archive ID and the key of the archived results file.  
   b. We then initiate a job for an "Expedited" archive retrieval. In case it fails, we initiate a job for a "Standard" archive retrieval. In the `Description`of the job parameters we send the `s3_key_result_file` so we're able to upload the file later in the S3 results bucket with that key (during the thaw process). We also set the `SNSTopic` parameter to our `josemaria_thaw` SNS topic so it gets a message with the archive ID once it finishes retrieving the job.  
   c. We persist the `retrieval_job_id` in case we need it to check what's the job's restoration status.
