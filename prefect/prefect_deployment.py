# Capstone Project
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from script.etl_mongodb_to_bq import etl_mongodb_to_bq

# Fetch storage from GitHub
github_block = GitHub.load("airbnb-github-block")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow
gcs_git_dep = Deployment.build_from_flow(
    flow=etl_mongodb_to_bq,
    name="airbnb-flow",
    storage=github_block,
)

print("Successfully deployed Airbnb Github Block. Check app.prefect.cloud")

# Run main
if __name__ == "__main__":
    gcs_git_dep.apply()

# to deploy
# prefect deployment run etl-mongodb-to-bq/airbnb-flow --params '{"db_name":"sample_airbnb", "coll_name": "listingsAndReviews"}'




# format ONLY for params so I cannot forget :)
#  --params '{"years":[2019, 2020], "months": [4, 5, 6, 7, 8, 9, 10, 11, 12, 2, 3, 1]}'
