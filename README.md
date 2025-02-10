# Ticketmaster Analytics

An Analytics project showcasing the full ETL process using the TicketMaster API

## Table of Contents

-   [Introduction](#introduction)
-   [Installation](#installation)
-   [Usage](#usage)

## Introduction

This project demonstrates a complete ETL (Extract, Transform, Load) process using data from the TicketMaster API. The goal is to extract event data, transform it for analysis, and load it into a data warehouse for further processing.

## Installation

To get started with this project, follow these steps:

1. Clone the repository:
    ```sh
    git clone https://github.com/isaacrivas10/ticketmaster_analytics.git
    ```
2. Navigate to the project directory:
    ```sh
    cd ticketmaster_analytics
    ```
3. Set up a virtual environment:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
4. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```
5. Store the `service_account.json` credentials file in the root of the project directory.

## Usage

To run the Extraction process, execute the following command:

```sh
python loader/main.py # --skip-extraction --skip-loading optional flags
```

This will start the extraction of data from the TicketMaster API, transform it, and load it into the specified data warehouse.

> **Note:** The data extraction will start at `2020-01-01T00:00:00Z` and will load the data to a specific cloud storage bucket. The start date can be changed based on needs.

To handle dbt (data build tool) transformations, follow these steps:

> **Note:** Ensure that dbt is set up with the BigQuery instance that will be used for the transformations.

1. Install the required dbt dependencies:
    ```sh
    dbt deps
    ```
2. Run the dbt models:
    ```sh
    dbt run
    ```

This will execute the dbt models to transform the data within the data warehouse.
