#!/bin/bash

# Load population CSV file to S3
# Source: IBGE, data treated and shared by Álvaro Justen/Brasil.IO

set -e

# Set credentials
AWS_KEY="$UDACITY_AWS_KEY"
AWS_SECRET="$UDACITY_AWS_SECRET"
AWS_PROFILE="$UDACITY_AWS_PROFILE"

# Configure S3 path
S3_BUCKET=$UDACITY_CAPSTONE_PROJECT_BUCKET
BASE_BUCKET_PATH="raw/population"
s3_uri="s3://$S3_BUCKET/$BASE_BUCKET_PATH/"

# Data source
URL="https://raw.githubusercontent.com/turicas/covid19-br/master/covid19br/data/populacao-por-municipio-2020.csv"
OUTPUT_FILENAME="population-by-city-2020.csv"

# Download locally
tmp_filepath="/tmp/$OUTPUT_FILENAME"
wget --output-document "$tmp_filepath" "$URL"

# Load into S3
aws s3 cp "$tmp_filepath" "$s3_uri" --profile "$AWS_PROFILE"

# Remove tmp file
rm $tmp_filepath
