ReadMe file that describes your entire final project. Your target audience for this final project deliverable would be a future interviewer. Don't include all your code, but include your overall steps - takeaways, appropriate links, etc. Make this a professional deliverable. 

## DATA 6360 Final Project
This project provided a comprehensive introduction to data warehousing. As consultants for Eco-Essentials, an eco-friendly cookware company, we took raw data sources and converted it into a functional data warehouse, enabling informed business decisions and analyze sales trends. We utilized DBeaver, FiveTran, DBT, and Snowflake. 

# Step 1- Enterprise Data Warehouse Design
We were given two raw data sources: a transactional database stored in an Amazon RDS Postgres Database and a marketing email events dataset stored in an AWS S3 Bucket. We identified the business process, the grain, dimensions, and facts. This allowed us to create a star schema entity relationship diagram (linked at bottom of page). This was used throughout the project to clearly show our datawarehouse. 

# Step 2- Extract, Load, Transform
Based on the ERD created in Step 1, we loaded the data into Snowflake, through the extract, transform, and load process. We used FiveTran to extract and load both data sources. Once the raw data was in Snowflake, we used dbt to transform the data and populate dimensional models. To connect fact and dimensions, we created surrogate keys based on the primary identifiers of each row. 

# Step 3- Testing & Scheduling
Although the database was already created, we needed to ensure they were tests in place to prevent future errors. New data will be added with the sources and needs to be consistently checked for consistency and quality errors. We created a job a job for each model that will refresh the datawarehouse once a day. Each times the jobs run the tests will be performed for each dimension and fact, to ensure the data is correct and our results are accurate. 

# Step 4- Data Visualization and Communication

# Takeaways
One takeaway is that throughout the process we needded to continually make changes and reflect on our past work. We received feedback after each step and made the appropriate changes to ensure our work was accurate moving forward. In step 2, 

# Links
[Star Schema ERD] (https://lucid.app/lucidchart/3594d33b-69ff-4354-b708-a6b542b259ca/edit?viewport_loc=100%2C117%2C832%2C1114%2C0_0&invitationId=inv_dcce761d-3c9f-4509-809c-3174d24cb030) 
