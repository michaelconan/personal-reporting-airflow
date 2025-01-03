# personal-reporting-airflow

Airflow server for personal data integration and experimentation.

## Overview

Dev container, requirements and constraints files used for local development prior to deployment.

## Resources

I have found a few resources covering deploying Airflow on Azure, none of which have been entirely usable for my purpose.

- [2018 Azure blog](https://azure.microsoft.com/es-es/blog/deploying-apache-airflow-in-azure-to-build-and-run-data-pipelines/) and [2022 Azure Quickstart](https://learn.microsoft.com/en-us/samples/azure/azure-quickstart-templates/airflow-postgres-app-services/) both use Puckel airflow image which does not support Airflow 2.0

## Data Sources

1. Google Contacts
2. HubSpot
3. Notion

## Airflow Notes

- DAGs should contain a number of related steps (e.g., extract-transform-load)
- DAGs can be linked to one another via datasets to enable triggering and dependency graph

## Python Setup (Attempt #2)

As an alternative to Docker I tried a standard Python configuration for easier deployment to App Service.

### Airflow Setup

### Azure Setup

1. Create Web App + PostgreSQL with Python
2. Turn on Application Insights, Logging

### Automated Deployment

1. Referenced [this workflow](https://learn.microsoft.com/en-us/azure/app-service/deploy-github-actions?tabs=applevel%2Cpython%2Cpythonn) to deploy Python app to App Service

## Docker Setup (Attempt #1)

This was working fine locally but deployment to App Service did not go as well. Goal to use a custom extended airflow Docker container

### Airflow Setup

1. Started with official [docker compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
2. Decided to [extend the image](https://airflow.apache.org/docs/docker-stack/build.html#extending-the-image) starting with the slim version due to limited packaged requirements
3. Switched to `LocalExecutor` 
4. Stripped down the Docker Compose to a single container to run webserver + scheduler and use for dev container

### Azure Setup

From the resources above I determined I can run the webserver and scheduler together in a single container with `LocalExecutor`. `CeleryExecutor` shouldn't be necessary unless scale increases and multiple workers are required.

1. Create PostgreSQL flexible server for database
2. Create App Service app with container (NOTE: I followed [this tutorial](https://learn.microsoft.com/en-us/azure/app-service/configure-custom-container?tabs=debian&pivots=container-linux#enable-ssh) to configure SSH access in app service)

### Automated Docker Deployment

1. Referenced [this workflow](https://docs.github.com/en/actions/use-cases-and-examples/publishing-packages/publishing-docker-images#publishing-images-to-github-packages) to build and publish to GitHub Container Registry
2. Referenced [this workflow](https://learn.microsoft.com/en-us/azure/app-service/deploy-best-practices#use-github-actions) to deploy updated image to Azure Web App