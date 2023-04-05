## Set-up

# Install gcloud CLI, https://cloud.google.com/sdk/docs/install
# Install Terraform, https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli

# You can Refresh service-account's auth-token for this session
gcloud auth application-default login

# initialized terraform  
terraform init

# Check changes to new infra plan
terraform plan

# Apply changes to infra plan, this will create the GCS Bucket and BQ dataset
terraform apply

# Delete infra AFTER your work, to avoid costs on any running services
terraform destroy