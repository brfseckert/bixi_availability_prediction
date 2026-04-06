# Summary

This project aims to predict the number available bikes per station within the city of Montreal every Sunday for the following 7 days with a 30 minutes window. The bike sharing system is operated and manged by Bixi. See the following link for more information about the company: https://bixi.com/en/.

# How to use this repository ?

Linux:
- create a new virtual python enviroment `python3 -m venv .venv`
- install dependencies `pip install requirments.txt`
- **OPTIONAL**: when using jupyter notebooks, it comes in handy to import pacakges developed under `src`. As such, navigate to the root project directory and then run `pip install -e ./`

# Methodology

## Baseline
Before attempting more complex methods, the first step of the project was to establish a naive baseline where the number of available bikes for a given station at time **t** is equal to the historical number available bikes at **t - 7 days**. The python library `skforecast` was used to perform this calculations.

## Time Series Forecasting
The next attempeted methodology will be done by leveraging the python library `skforecast` leveraging tree based models in a time series context. The library makes a good job at abstracting the complexity in this scenario.

# Data

The latest number of available bikes is made public through Bixi's open data API which can be accessed here:
https://gbfs.velobixi.com/gbfs/en/station_information.json

Historical data, on the other hand, is not publicly available. However, a project developed by Max Halford allows us to obtain historical data since early 2024 for the city of Montreal and other cities across the world. The data from the project was retrived and persisted for training purposes.***📝 [See blog post](https://maxhalford.github.io/blog/bike-sharing-forecasting-training-set/)***



# How to use this repository ?
Linux:
- create a new virtual python enviroment `python3 -m venv .venv`
- install dependencies `pip install requirments.txt`
- **OPTIONAL**: when using jupyter notebooks, it comes in handy to import pacakges developed under `src`. As such, navigate to the root project directory and then run `pip install -e ./`

# Architecture

The production deployment uses a containerized architecture on AWS, separating ingestion, training, and serving into independent components.

## AWS Services

| Purpose | Service | Role |
|---|---|---|
| Container registry | ECR | Stores Docker images |
| Prediction API | App Runner | Serves forecasts, scales to zero, built-in HTTPS |
| Scheduled ingestion | ECS Fargate + EventBridge | Runs ingestion container every 15 min |
| Scheduled retraining | ECS Fargate + EventBridge | Retrains model weekly (Sundays) |
| Object storage | S3 | Stores parquet data and trained model artifacts |
| CI/CD | GitHub Actions | Builds image, pushes to ECR, redeploys on merge |

## Diagram

```
                  EventBridge (every 15 min)
                         |
                         v
               +------------------+
               |  ECS Fargate     |
               |  (ingestion job) |---------->  S3 (data/)
               +------------------+
                                                  |
               EventBridge (weekly)               |
                         |                        v
                         v                  +-----------+
               +------------------+         |  S3       |
               |  ECS Fargate     |-------->|  models/  |
               |  (retrain job)   |         +-----------+
               +------------------+               |
                                                  v
  User -->  App Runner (FastAPI) <-------- loads model from S3
               GET /predict
```

## S3 Bucket Layout

```
s3://bixi-availability/
  +-- data/              # parquet snapshots from ingestion
  +-- models/            # trained model artifacts (.joblib)
```

## Deployment Steps

1. **Dockerize** the project with a single image that can run ingestion, training, or the API via entrypoint arguments
2. **Build a FastAPI serving layer** (`GET /predict?station_id=XXX`, `GET /health`)
3. **Set up S3** for data and model storage; update ingestion to write to S3 via `boto3`
4. **Push image to ECR** and create an App Runner service for the prediction API
5. **Schedule ingestion** with EventBridge (`rate(15 minutes)`) targeting an ECS Fargate task
6. **Schedule retraining** with EventBridge (`cron(0 6 ? * SUN *)`) targeting an ECS Fargate task
7. **Set up GitHub Actions** to build, push, and redeploy on merge to `main`