# Real-Time Operational Intelligence Platform on Azure

## Project Overview

This project presents an automated, end-to-end data engineering pipeline built on Azure, designed to monitor operational processes across multiple regions and source systems in real time. The pipeline ingests a continuous stream of operational events from ERP, CRM, OMS, and monitoring tools via Azure Event Hubs, processes them through a Medallion Architecture (Bronze → Silver → Gold) in Azure Databricks, and delivers live business KPIs through a Power BI dashboard connected directly to Azure Synapse Analytics.

The project emphasizes enterprise practices: Slowly Changing Dimensions (SCD Type 2) for historical tracking, Delta MERGE for safe incremental loads, dirty data handling at the Silver layer, and credential-free security through Managed Identities and Azure Key Vault.

## Business Requirements

Organizations running on disparate systems (ERP, CRM, OMS) lack a centralized, real-time mechanism to observe the health of their operational processes. Events go unmonitored, SLA breaches go undetected, and bottlenecks across regions remain invisible until something breaks.

This project solves that by building a platform that:

- Tracks every operational event from Intake to Completion across EU, US, and APAC regions.
- Identifies which source systems and process stages cause the most failures and delays.    
- Flags SLA breaches automatically (processes exceeding 300 seconds).
- Delivers all insights through a live Power BI dashboard. No manual refreshes needed.