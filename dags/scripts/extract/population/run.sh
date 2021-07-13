#!/bin/bash

# Load population CSV file to S3
# Source: IBGE, data treated and shared by Álvaro Justen/Brasil.IO
# Usage: 
#    - Extract and append to destination:     `bash run.sh`
#    - Extract and replace existing contents:  `bash run.sh replace`

set -e

LOAD_MODE=$1

# Set credentials
AWS_KEY="$UDACITY_AWS_KEY"
AWS_SECRET="$UDACITY_AWS_SECRET"

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

if [ "$LOAD_MODE" = "replace" ]
then
    # Clean up S3 destination
    remove_from_destination="$s3_uri"
    echo "Replace mode: Removing current file at $remove_from_destination"
    /opt/airflow/venvs/awscli/bin/aws s3 rm "$remove_from_destination" --include "*.csv*" --recursive
fi

# Load into S3
/opt/airflow/venvs/awscli/bin/aws s3 cp "$tmp_filepath" "$s3_uri"

# Remove tmp file
rm $tmp_filepath
