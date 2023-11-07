## Running this script
To run this script, clone this code from GitHub. You will need to `cd` into the project directory from your terminal (`cd take_home_assignment`) and install from the `requirements.txt`. I would highly recommend doing this in a virtual environment like `pipenv`. You will need to run `pip install -r requirements.txt` or `pipenv install -r requirements.txt` if you're using `pipenv`. Then run `python3 assignment.py`. Please ask me if you have any questions.

## How you might mature the data acquisition
I would look into using the GDELT API in the future. The way my script does it could easily break with any changes to the data provided at the source URL.

## Additional data preparation steps and data quality checks you may want to perform
I would want to set up some logic for using other columns to filter on if lat or long are empty. (Something along the lines of `ifnull` in SQL, using another field to guess if the event took place in the US.) I would also want to make sure to format the data to the correct data types and text cases and string lengths.

## How you might approach triaging or otherwise remediating data with unresolved errors
I would add more error handling into this with many `try...except` blocks in the Python code for various errors including inability to retrieve from the URL, type errors, etc. I didn't have enough time to add more error handling to a few of the functions - specifically for the filtering of data. Also, if there is a faulty value that could not be resolved in the events, I would log that to another table or something like CloudWatch so these errors could be investigated, especially if they come up often. I would also add in logic - either error handling or reporting - to handle incorrect data types before and while inserting into the DB. This might look like a built-in function with the DB that will skip inserting the row and log that malformed row somewhere.

## How you might better stage the pipeline using data warehousing strategies
This is a quick and dirty script so it could improve in many ways to make it more scalable. I could implement this as a DAG in Airflow so the scheduling would be built in. I could also create a Lambda function (or similar) in S3 that would alert to an SQS queue with file names of new files in GDELT. Then, the process could instead read from that queue, removing the file name from the to-be-processed queue once it has been successfully processed. The code could either run in real-time using Kafka or a forever loop that's listening for new uploads or in batches using a tool like Airflow. Then, the event data could be loaded into a data warehouse. I think a star schema would work very well for this. The main event data could go into a fact table. Then, the other data provided (see `mappings.py`) looks like a perfect use-cases for dim tables that could be used to join to the fact table to enrich the event data using the various codes found in the fact table.
I would also make sure to use good version control and code review processes. It would ultimately be good to have a deployment process set up with an automated test suite.

## Performance considerations
The API would hopefully make this more performative. Fortunately, since these files are only 15 minutes worth of data, the processing would just need to take less than 15 minutes in order to keep up without needing to scale. It is currently taking only a few seconds to complete, but the database load would probably increase that to a minute or so.

## How you might mature logging, alerting and notifications
For now, my "logging" is just print statements, but ultimately it would make sense to have a logging table that shows when files have been completed and the event fact table updated. There could also be tables for common errors. Then, I would also add alerting in CloudWatch or similar to send alert notifications to the team so action could be taken.
