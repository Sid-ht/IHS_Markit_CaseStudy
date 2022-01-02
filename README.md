## IHS_Markit_CaseStudy

Below Given dataset is about Parts.
1. Given 4 datasets (country, status, File1, File2) load then into PySpark Dataframe
2. Process these files using PySpark and Merge File1 and File2
3. Parts Numbers should be Unique and not null.
4. Use Status and Country table to get corresponding country and Status details.
5. Save the Final Data into Parquet format in certain location.
6. Get count of parts by status and country and save them into two separate Parquet files.
7. Retain some details like (±) in data and do not remove.


#### Requirements:
1. Well-structured, object-oriented, documented, and maintainable code.
2. Robust and resilient code. The application should be able to scale if data volume increases.
3. Proper exception handling
4. Config management & Logging
5. Solution is deployable and we can run it (locally and on a cluster) – an iPython notebook is not sufficient.
6. Documentation (approach and commands for executing this code (add screenshots if possible)).


#### Project Structure

```
.
├── IHS_Markit_CaseStudy
│   ├── log4j.properties
│   ├── requirement.properties
│   ├── spark.conf
│   └── src
│   │   ├── analytics
│   │            ├──CaseAnalysis.py
│   │   ├──────output
│   │            └──countrybycount.parquet
│   │            └──output.parquet
│   │            └──statusbycount.parquet
│   │   ├── resources
│   │         └──country.csv
│   │         └──File1.xlsx
│   │         └──File2.xlsx
│   │         └──status.csv
│   └── utils
│       └── __init__.py
│       └── logger.py
│       └── schemas.py
│       └── utils.py
│
├── README.md

