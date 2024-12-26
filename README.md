# 🦸‍♂️ Marvel Movie Data Scraper

A data pipeline for scraping Marvel film data using Wikipedia and OMDB via REST API. This project extracts, cleans, merges, and uploads data to AWS S3 using **AWS Lambda**. The final output consists of three CSV files: `movies.csv`, `omdb_data_df_cleaned.csv`, and `merged_df.csv`.

---

## ✨ Features
1. **Data Extraction**:
   - **Wikipedia**: Scrapes Marvel movie data from tables on Wikipedia.
   - **OMDB API**: Fetches additional movie data via REST API.

2. **Data Cleaning**:
   - Extracted data from both sources is cleaned using **Pandas**.

3. **Function Modularity**:
   - Each step is implemented as a standalone function for better reusability and testing.

4. **Data Merging**:
   - Both datasets are merged on the `film_names` field.

5. **AWS Lambda Integration**:
   - Functions are deployed on AWS Lambda, ensuring scalability and automation.
   - Roles and environment variables are configured to support execution.

6. **S3 Upload**:
   - Final cleaned datasets are uploaded to the S3 bucket under `scrape_marvel_movie_data` folder:
     - `movies.csv`
     - `omdb_data_df_cleaned.csv`
     - `merged_df.csv`

---

## 📂 Folder Structure
```plaintext
.
├── marvel_data_extract.ipynb      # All Python function
├── lambda_function.py             # Main entry point for AWS Lambda
├── requirements.txt               # Python dependencies
└── README.md                      # Project documentation
```
---

## 🏗️ Architecture Overview:

### Workflow Diagram

![Airflow-AWS-SPotify-ETL-Pipeline](https://drive.google.com/uc?export=view&id=1VMri7GKqztxXm2Ay99LzMbdSwL5Kr4AX)

---

## 🚀 How It Works

1. Wikipedia Data Extraction:

   - Scrapes tables from Marvel's Wikipedia page using BeautifulSoup.
   - Converts tables into Pandas DataFrame (movies.csv).

2. OMDB Data Extraction:

   - Fetches movie details via OMDB API.
   - Converts API response into a cleaned DataFrame (omdb_data_df_cleaned.csv).

3. Data Merging:

   - Merges Wikipedia and OMDB data on film_names.
   - Produces a final cleaned DataFrame (merged_df.csv).

4. AWS Lambda Deployment:

   - Deploys the pipeline as a Lambda function with the required roles and environment variables.


5. Upload to S3:

   - Saves all cleaned and merged datasets as CSV files in the S3 bucket under scrape_marvel_movie_data folder.

## 🧰 Installation and Setup

1. Install Dependencies:
```
   %pip install pandas
   %pip install requests
   %pip install beautifulsoup4
```

2. Set Up AWS Lambda:

   - Assign appropriate IAM roles to Lambda for S3 access.
   - Ensure environment variables and packages are included in the Lambda function deployment.
  
## 📖 Requirements

- Python 3.8+
- AWS Lambda configured with:
  - S3 access permissions.
  - Required Python dependencies (pandas, requests, beautifulsoup4).
 
### 📲 Contact:

- Author: Sohail Sayyed
- Email:  ["**Gmail**"](jabmsohail@gmail.com)
- LinkedIn: ["**LinkedIn**"](https://www.linkedin.com/in/sohailsayyed09/)
- GitHub: ["**Github**"](https://github.com/Sohail-09)
