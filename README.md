# Fetch_User_Logins
->Approach:
----------------------
1. Setup docker containers using the docker-compose.yml
2. Initialize and start the containers using docker compose -up
3. Initialize producer and consumer using provided configuration.
4. Fetching the data with 10 seconds poll interval as producer sends messages every 10 sec.
5. Inorder to process efficiently, set an interval of 5minutes to transform records in batches 
   with some aggregations to avoid transformation overload as soonas message is consumed.
6. Perform Data Cleansing and validation checks
	-Remove Duplicates
   	-Check for nulls
   	-Adjust timestamp field for further processing
7. Perform desired analysis:
	- Timely usage analysis
	- Distribution by local
	- Distribution by App version
	- Distribution by device types
	- Single Device usage analysis by multiple users

-> Key things to consider while moving to production:
----------------------
1. Data Validation and Data Quality checks
	- Make sure records are complete with no missing information
	- Make sure data is consistent with expected values/formats 
	- Data DeDuplication
2. Error Handling
3. Access Control Security Measures for kafka cluster for secured data exchange.
3. Monitoring and Alerts for any delays or failures (Eg: Grafana)
4. Unit Testing
5. Using Kubernetes to utilize its distributed computation power and process efficiently
6. Orchestration
 
->Key things to consider for growing data set:
--------------------
1. Adjust Cluster config to process the growing data set.
2. Implementing offset, current exercise doesn't provide with GroupId to do so. This would enable to process all the records
from kafka producer without missing any or reprocessing the same.
3. Adjust Intervals to fetch desired amount of size in batches.
4. Choose right data format with compressions to optimize on storage.
5. Implement external storage to archive old data and reduce load on kafka storage
5. Can implement topic Partitions/Keys based on the data size and different transformations required to.
6. Utilize Spark Streaming for transformations and aggregrations  enabling batch processing efficiently.
6. Using Kubernetes with Docker to utilize its capability of processing for growing data sets.
7. Implementing Monitoring system enables us to understand the data loads and helps us optimizing the logic where necessary.
