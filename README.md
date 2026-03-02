# Loan Default Risk Analysis 

## Project Overview
A PySpark project built on Databricks that analyzes loan data
to identify high risk customers based on missed payments,
loan to income ratio and employment status.

## Tech Stack
- **Platform:** Databricks
- **Language:** PySpark (Python)
- **Dependencies:** See `requirements.txt`

## Project Structure
```
├── src/
│   └── loan_default_risk_analysis.py    # Main analysis code
├── README.md                             # Project documentation
├── requirements.txt                      # Project dependencies
└── LICENSE
```

## Dataset
Sample loan dataset with 20 records containing customer loan
information including loan amount, tenure, interest rate,
missed payments and employment status.

## Analysis Performed

### Transformations
| Transformation | Description |
|----------------|-------------|
| risk_category | High Risk / Medium Risk / Low Risk based on missed payments |
| loan_to_income_ratio | Loan amount divided by monthly income |
| Employment Filter | Kept only Employed and Self Employed customers |

### GroupBy Analysis
- Total loans per loan type
- Total loan amount per loan type
- Average interest rate per loan type
- Average missed payments per loan type

### Window Function
- Ranked customers by loan amount within each loan type

### Spark SQL
- Total loan amount and average interest rate by city

## Output Tables
| Table | Description |
|-------|-------------|
| loan_default_risk_clean | Cleaned and transformed loan data |
| loan_type_summary | Aggregated summary by loan type |

## Key Insights
- Unemployed customers have higher missed payments
- Personal loans have the highest average interest rate
- Home loans have the highest total loan amount

## How to Run
1. Open Databricks and create a new notebook
2. Copy the code from `src/loan_default_risk_analysis.py`
3. Run all cells sequentially
4. Check Delta tables in your catalog