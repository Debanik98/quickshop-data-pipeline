# Namaste Kart Order File Validator

## Project Overview
[cite_start]Namaste Kart is an online retail store operating in **Mumbai and Bangalore**[cite: 3]. [cite_start]This project involves building an automated data engineering pipeline to validate daily transaction files generated from these cities before they are processed by the analytics team[cite: 4, 5]. 

[cite_start]The goal is to automate the entire lifecycle: **Read -> Validate -> Organize -> Log Rejections -> Notify**[cite: 57, 58, 59, 60, 61, 62].

## Folder Structure
[cite_start]The pipeline manages data across the following directory hierarchy[cite: 6, 7]:
* [cite_start]**`NamasteKart/incoming_files/YYYYMMDD/`**: Input directory for daily order files[cite: 8, 9, 10].
* [cite_start]**`NamasteKart/success_files/YYYYMMDD/`**: Output directory for valid files[cite: 11, 12, 13].
* [cite_start]**`NamasteKart/rejected_files/YYYYMMDD/`**: Output directory for rejected files and error logs[cite: 14, 15, 16].

## Validation Rules
[cite_start]Every order within an incoming file must pass these five criteria[cite: 30]:
1. [cite_start]**Product Integrity**: `product_id` must exist in the provided product master file[cite: 31].
2. [cite_start]**Financial Accuracy**: `total_sales_amount` must equal the calculation of $product\_price \times quantity$[cite: 32].
3. [cite_start]**Temporal Logic**: `order_date` cannot be in the future relative to the current date[cite: 33].
4. [cite_start]**Data Completeness**: No field (order_id, product_id, date, city, quantity, amount) should be empty[cite: 34].
5. [cite_start]**Geographic Constraint**: The `city` field must be either 'Mumbai' or 'Bangalore'[cite: 35].

## Pipeline Logic & Error Handling
* [cite_start]**File-Level Validation**: If even a single order in a file fails validation, the entire file is rejected[cite: 39].
* [cite_start]**Rejection Logging**: For every rejected file, a corresponding CSV named `error_{original_file_name}.csv` is created[cite: 42, 43].
* [cite_start]**Detailed Errors**: Error files contain only the failed rows with an additional `rejection_reason` column; multiple reasons are separated by semicolons[cite: 44].
* [cite_start]**Business Notification**: After processing, an email is sent to the business team summarizing the total files processed, passed, and failed[cite: 45, 46, 50, 51, 53].

---