#!/bin/bash

# Load population CSV file to S3
# Source: IBGE, data treated and shared by Álvaro Justen/Brasil.IO
# Usage: 
#    - Extract and append to destination:     `bash run.sh`
#    - Extract and replace existing contents:  `bash run.sh replace`

set -e

load_mode=$1

# Configure S3 path
s3_bucket=$UDACITY_CAPSTONE_PROJECT_BUCKET
base_bucket_path="raw/population"
s3_uri="s3://$s3_bucket/$base_bucket_path/"

# Data source
source_url="https://raw.githubusercontent.com/turicas/covid19-br/master/covid19br/data/populacao-por-municipio-2020.csv"
output_filename="population-by-city-2020.csv"

# Download locally
extract_csv() {
    tmp_filepath="/tmp/$output_filename"
    wget --output-document "$tmp_filepath" "$source_url"
    /opt/airflow/venvs/awscli/bin/aws s3 cp "$tmp_filepath" "$s3_uri"
}

if [ "$load_mode" = "replace" ]
then
    # Move current files to trash that will be emptied at the end if execution succeeds
    remove_from_destination="$s3_uri"
    trash_destination="$remove_from_destination/trash/"
    echo "Replace mode: moving existing file(s) to trash at $trash_destination"
    /opt/airflow/venvs/awscli/bin/aws s3 mv "$remove_from_destination" "$trash_destination" --include "*.csv*" --recursive

    # Run tap and target
    if extract_csv
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
    extract_csv
fi

# Remove tmp file
rm $tmp_filepath
