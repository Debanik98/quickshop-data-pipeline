# Quickshop Order File Validator

## Project Overview
Quickshop is an online retail store operating in **Mumbai and Bangalore**. This project involves building an automated data engineering pipeline to validate daily transaction files generated from these cities before they are processed by the analytics team. 

The goal is to automate the entire lifecycle: **Read -> Validate -> Organize -> Log Rejections -> Notify**.

## Folder Structure
The pipeline manages data across the following directory hierarchy:
* **`quickshop-analytics/incoming_files/YYYYMMDD/`**: Input directory for daily order files.
* **`quickshop-analytics/success_files/YYYYMMDD/`**: Output directory for valid files.
* **`quickshop-analytics/rejected_files/YYYYMMDD/`**: Output directory for rejected files and error logs.

## Validation Rules
Every order within an incoming file must pass these five criteria:
1. **Product Integrity**: `product_id` must exist in the provided product master file.
2. **Financial Accuracy**: `total_sales_amount` must equal the calculation of $product\_price \times quantity$.
3. **Temporal Logic**: `order_date` cannot be in the future relative to the current date.
4. **Data Completeness**: No field (order_id, product_id, date, city, quantity, amount) should be empty.
5. **Geographic Constraint**: The `city` field must be either 'Mumbai' or 'Bangalore'.

## Pipeline Logic & Error Handling
* **File-Level Validation**: If even a single order in a file fails validation, the entire file is rejected.
* **Rejection Logging**: For every rejected file, a corresponding CSV named `error_{original_file_name}.csv` is created.
* **Detailed Errors**: Error files contain only the failed rows with an additional `rejection_reason` column; multiple reasons are separated by semicolons.
* **Business Notification**: After processing, an email is sent to the business team summarizing the total files processed, passed, and failed.

---
