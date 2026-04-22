# Banks ETL Pipeline using Python 🏦
![IBM](https://img.shields.io/badge/IBM-052FAD?style=for-the-badge&logo=ibm&logoColor=white)
![Coursera](https://img.shields.io/badge/Coursera-0056D2?style=for-the-badge&logo=coursera&logoColor=white)

> A capstone project for the **IBM Data Engineering Professional Certificate** on Coursera

A complete ETL (Extract, Transform, Load) pipeline that extracts data of the world's largest banks from Wikipedia, transforms market capitalization from USD to INR, and loads the data into both CSV and SQLite database.

## 📌 Project Overview
This project demonstrates a full ETL workflow:
- **Extract**: Scrapes bank data from Wikipedia using BeautifulSoup
- **Transform**: Converts Market Cap from USD to INR using exchange rates from CSV
- **Load**: Saves processed data to `CSV` file and `SQLite` database
- **Log**: Records all ETL operations with timestamps in `code_log.txt`

## 🛠️ Tech Stack
- **Language**: Python 3
- **Libraries**: Pandas, NumPy, Requests, BeautifulSoup4, SQLite3
- **Database**: SQLite
- **Data Source**: [List of largest banks - Wikipedia](https://en.wikipedia.org/wiki/List_of_largest_banks)

## 📁 Project Structure
```
banks-ETL-pipeline-python/
│
├── banks_project.py              # Main ETL script
├── exchange_rate.csv             # Exchange rates for currency conversion
├── Largest_banks_data.csv        # Output: Transformed data in INR
├── Banks.db                      # Output: SQLite database
├── code_log.txt                  # ETL process logs with timestamps
└── README.md                     # Project documentation
```
## 🚀 How to Run

### 1. Clone the repository
```bash
git clone https://github.com/saniya-mansuri15/banks-ETL-pipeline-python.git
cd banks-ETL-pipeline-python
```
### 2. Install dependencies
```
pip install pandas numpy requests beautifulsoup4
```
### 3. Run the ETL script
```
python banks_project.py
```
#### 4. Check output
```
Largest_banks_data.csv  -  Final data with Market Cap in INR
Banks.db                -  SQLite database with bank details  
code_log.txt            -  Logs of ETL process
```
## 📊 Sample Output
| Name | Market Cap (US$ Billion) | Market Cap (INR Billion) |
| --- | --- | --- |
| JPMorgan Chase | 432.92 | 35932.36 |
| Bank of America | 231.52 | 19216.16 |
| Industrial and Commercial Bank of China | 194.56 | 16148.48 |

## 🎯 Key Learnings
- Web scraping with BeautifulSoup
- Data transformation using Pandas DataFrames
- Database operations with SQLite in Python
- ETL pipeline development and logging
- Working with CSV files and handling exchange rates

## 👩‍💻 Author
**Saniya Mansuri**  
Connect with me on [LinkedIn](https://www.linkedin.com/in/saniya-mansuri)

---
⭐ If you found this project helpful, please give it a star!
