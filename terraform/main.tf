# 1. Define required providers (AWS in this case)
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Use a recent stable version
    }
  }
}

# 2. Configure the AWS provider
# Uses credentials configured via 'aws configure'
# Specify the AWS region you want to create resources in
provider "aws" {
  region = "ap-southeast-2" # Example: Sydney region
}

# 3. Define the S3 bucket resource
resource "aws_s3_bucket" "weather_data_raw" {
  # --- IMPORTANT: CHANGE THIS BUCKET NAME! ---
  # Bucket names must be globally unique across all AWS accounts.
  # Use lowercase letters, numbers, and hyphens.
  # Add your initials, a date, or random numbers.
  bucket = "b-surampudi-weather-pipeline-raw-12345" 

  # Optional: Add tags for organization
  tags = {
    Name        = "WeatherDataPipelineRaw"
    Project     = "WeatherPipelineProject"
    Environment = "Dev"
  }
}

# Optional: Define an output variable to easily see the bucket name after creation
output "s3_bucket_name" {
  description = "The name of the S3 bucket created for raw weather data."
  value       = aws_s3_bucket.weather_data_raw.bucket
}
