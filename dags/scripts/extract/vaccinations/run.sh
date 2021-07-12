#!/bin/bash

# Build tap and target configuration JSON files and run extraction
# Usage: 
#    - Extract and load empty destination:     `bash run.sh 2021-01-01 SC`
#    - Extract and replace existing contents:  `bash run.sh 2021-01-01 SC replace`

set -e

YEAR_MONTH=$1
STATE_ABBREV=$2
LOAD_MODE=$3

# Set credentials
AWS_KEY="$AWS_ACCESS_KEY_ID"
AWS_SECRET="$AWS_SECRET_ACCESS_KEY"

# Configure S3 path
S3_BUCKET=$UDACITY_CAPSTONE_PROJECT_BUCKET
BASE_BUCKET_PATH="raw/vaccinations"
s3_prefix="$BASE_BUCKET_PATH/year_month=$YEAR_MONTH/estabelecimento_uf=$STATE_ABBREV/"

# Setup paths
root_path="/opt/airflow"
extract_vaccinations_path="$root_path/dags/scripts/extract/vaccinations"
tap_config_json_filepath="$extract_vaccinations_path/opendatasus_config.json"
target_config_json_filepath="$extract_vaccinations_path/s3_csv_config.json"
target_config_json_filepath="$extract_vaccinations_path/s3_csv_config.json"
catalog_json_filepath="$extract_vaccinations_path/catalog.json"
state_json_filepath="$extract_vaccinations_path/state.json"

# Build configuration files
tap_config_json=$( jq -n \
                  --arg ym "$YEAR_MONTH" \
                  --arg sa "$STATE_ABBREV" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa}' )

# S3 CSV
target_config_json=$( jq -n \
                  --arg ak "$AWS_KEY" \
                  --arg ae "$AWS_SECRET" \
                  --arg sb "$S3_BUCKET" \
                  --arg sp "$s3_prefix" \
                  --arg qc '"' \
                  --arg dl "," \
                  --arg co "gzip" \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )


echo $tap_config_json > "$tap_config_json_filepath"
echo $target_config_json > "$target_config_json_filepath"

if [ "$LOAD_MODE" = "replace" ]
then
    # Clean up S3 destination
    remove_from_destination="s3://$S3_BUCKET/$s3_prefix"
    echo "Replace mode: Removing current file at $remove_from_destination"
    /opt/airflow/venvs/awscli/bin/aws s3 rm "$remove_from_destination" --include "*.csv*" --recursive
fi

# Run tap and target
/opt/airflow/venvs/tap-opendatasus/bin/tap-opendatasus --config "$tap_config_json_filepath" --catalog "$catalog_json_filepath" --state "$state_json_filepath" | /opt/airflow/venvs/target-s3-csv/bin/target-s3-csv --config "$target_config_json_filepath" >> "$state_json_filepath"
tail -1 "$state_json_filepath" > "$state_json_filepath.tmp" && mv "$state_json_filepath.tmp" "$state_json_filepath"
