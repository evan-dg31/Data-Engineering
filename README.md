# Data Lakehouse and Analytics Project - Formula 1

Welcome to the Data Lakehouse and Analytics Project repository! 🚀

This project focuses on the design and implementation of a modern Data Lakehouse architecture and analytics solution, from building a data lakehouse to generating actionalbe insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics showcasing hands-on experience with:

- Building and managing a Data Lakehouse
- Data ingestion, transformation, and modeling
- Implementing ELT pipelines
- Creating insightful dashboards and reports
***
### 🏗️ Data Architecture
The data architecture for this project follows **Medallion Architecture Bronze, Silver, and Gold layers**:
<img width="1100" height="700" alt="image" src="https://github.com/user-attachments/assets/53d17d14-2116-44be-bd63-c502965d371f" />

1. **Raw Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV and JSON Files into Azure Data Lake Storage Gen2 (ADLS Gen2).
2. **Ingested Layer**: This layer includes data cleansing, transformation, standardization, and normalization processes to prepare data for analysis using Azure Databricks.
3. **Presentation Layer**: Aggregated data and houses business-ready data modeled required for reporting and analytics.


***

### 🚀 Project Requirements
**Building the Data Warehouse (Data Engineering)**
**Objective**
Develop a modern data lakehouse using Azure Data Lake to consolidate race data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources:** Import data from source systems (Ergast API) provided as CSV and JSON files
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope:** Focus on the historical dataset.
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics

---
### BI: Analytics & Reporting (Data Analytics)

#### Objective
Develop SQL - based analytics to deliver detailed insghts into: 
- **Dominant Drivers**
- **Dominant Teams/Constructors**
- **Driver Standings**
- **Constructor Standings**

These insighst allows us to indentify and find pattern about the drivers and teams


## 🛡️ License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me
Hi there! I'm Evan de Guzman. I’m an Data professional and passionate about learning aabout data YouTuber on a mission to share knowledge and make working with data enjoyable and engaging!
👋 About Me

Hi there! I'm Evan de Guzman, a Data Professional with a passion for learning all things data. I enjoy working with data to extract insights, build meaningful visualizations, and solve real-world problems through data-driven decision making.

I'm always looking to grow my skills in:

Data Analysis 📊

Data Engineering ⚙️

Machine Learning 🤖

Python, SQL, and Big Data Tools

Let's connect and explore the world of data together!
Feel free to connect with me on 🔗 LinkedIn https://www.linkedin.com/in/evan-de-guzman/ | 📧 Email evan.dg31@gmail.com 


