# bigquery-data-anonymization
Address challenges of maintaining GDPR compliance within data warehouse
The core objective is to ensure that personally identifiable information (PII) is anonymized systematically across datasets, safeguarding user privacy while retaining the integrity of the stored data. Note that this case scenario applies to data that has been previously anonymized at the source adnd extracted into bigquery tables. This tables have historical data that does not get automatically updated when there is a new request for anonymization. This solution allows to automate anonymization of such data within BigQuery tables using the following features:
- PII Detection and Anonymization: Automatically identifies columns containing PII and replaces this data with anonymized values, ensuring compliance with GDPR's privacy requirements.
- Audit Logging: Generates detailed logs of each anonymization action, providing a transparent record for internal audits and regulatory compliance checks.
- Error Handling: Designed to gracefully handle scenarios such as missing tables or schemas, ensuring the process is robust and reliable.
- Scalability: Can be integrated into existing data pipelines, allowing for seamless scalability and adaptability to various datasets and schemas within BigQuery.
