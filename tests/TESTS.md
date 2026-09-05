# Tests

Unit, integration, and end-to-end tests should be developed for each component of the project.

## data load tool (dlt)

### Unit Tests

Individual unit tests should be created for each step in the [dlt pipeline](https://dlthub.com/docs/reference/explainers/how-dlt-works#the-three-phases) (extract, normalize, load). Sources (e.g., APIs) should be mocked and loaded to local duckdb destinations for full offline support.

#### REST APIs

Mock endpoints should be used in test cases to validate pagination and incremental loading.

**Test Data:**

Test data is prepared in 3 files, typically leveraging example responses from API documentation. Record counts are consistent across sources but differ by files to allow effective validation of sample data loads in unit tests by row count.

File       | Contents
---------- | ------------
run1_page1 | 3 records with a link or similar indicator that more data exists (pagination)
run1_page2 | 2 records with an indicator that no more data exists
run2       | 1 record with a larger cursor value (e.g., date) to test pagination
