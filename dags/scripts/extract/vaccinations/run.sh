#!/bin/bash

# Build tap and target configuration JSON files and run extraction
# Usage: 
#    - Extract and append to destination:     `bash run.sh 2021-01-01 SC`
#    - Extract and replace existing contents:  `bash run.sh 2021-01-01 SC replace`

set -e

year_month=$1
state_abbrev=$2
load_mode=$3

# Set credentials
aws_key="$AWS_ACCESS_KEY_ID"
aws_secret="$AWS_SECRET_ACCESS_KEY"

# Configure S3 path
s3_bucket=$UDACITY_CAPSTONE_PROJECT_BUCKET
base_bucket_path="raw/vaccinations"
s3_prefix="$base_bucket_path/year_month=$year_month/estabelecimento_uf=$state_abbrev"

# Setup paths
root_path="/opt/airflow"
extract_vaccinations_path="$root_path/dags/scripts/extract/vaccinations"

# Catalog and state won't change across tasks
catalog_json_filepath="$extract_vaccinations_path/catalog.json"
state_json_filepath="$extract_vaccinations_path/state.json"

# Configs will vary across tasks
config_path="$extract_vaccinations_path/$state_abbrev"
mkdir -p "$config_path"

tap_config_json_filepath="$config_path/opendatasus_config.json"
target_config_json_filepath="$config_path/s3_csv_config.json"
target_config_json_filepath="$config_path/s3_csv_config.json"

# Build configuration files
tap_config_json=$( jq -n \
                  --arg ym "$year_month" \
                  --arg sa "$state_abbrev" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa}' )

# S3 CSV
target_config_json=$( jq -n \
                  --arg ak "$aws_key" \
                  --arg ae "$aws_secret" \
                  --arg sb "$s3_bucket" \
                  --arg sp "$s3_prefix/" \
                  --arg qc '"' \
                  --arg dl "," \
                  --arg co "gzip" \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )

echo $tap_config_json > "$tap_config_json_filepath"
echo $target_config_json > "$target_config_json_filepath"

run_tap_target() {
    /opt/airflow/venvs/tap-opendatasus/bin/tap-opendatasus --config "$tap_config_json_filepath" --catalog "$catalog_json_filepath" --state "$state_json_filepath" | /opt/airflow/venvs/target-s3-csv/bin/target-s3-csv --config "$target_config_json_filepath" >> "$state_json_filepath"
    tail -1 "$state_json_filepath" > "$state_json_filepath.tmp" && mv "$state_json_filepath.tmp" "$state_json_filepath"
}

if [ "$load_mode" = "replace" ]
then
    # Move current files to trash that will be emptied at the end if execution succeeds
    remove_from_destination="s3://$s3_bucket/$s3_prefix"
    trash_destination="$remove_from_destination/trash/"
    echo "Replace mode: moving existing file(s) to $trash_destination"
    /opt/airflow/venvs/awscli/bin/aws s3 mv "$remove_from_destination" "$trash_destination" --include "*.csv*" --recursive

    # Run tap and target
    if run_tap_target
    then
        # Remove trash contents
        echo "Replace mode: emptying trash contents from $trash_destination"
        /opt/airflow/venvs/awscli/bin/aws s3 rm "$trash_destination" --recursive
    else
        # Recover from trash
        echo "Replace mode: abort removal due to execution error. Recovering from trash to $remove_from_destination"
        if /opt/airflow/venvs/awscli/bin/aws s3 mv "$trash_destination" "$remove_from_destination" --include "*.csv*" --recursive
        then
            # Remove trash folder
            /opt/airflow/venvs/awscli/bin/aws s3 rm "$trash_destination" --recursive
        fi
    fi
else
    # Run tap and target just adding new files to the destination
    run_tap_target
fi
