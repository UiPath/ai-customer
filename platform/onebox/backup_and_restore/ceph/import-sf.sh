#!/bin/bash

: '
This scipt will import all data stored at a path to blob storage in target environments.
# $1 - json file with credentials, change the script to work with own credential manager
# $2 - Absolute path to import from
# $3 - Source tenant id.
# $4 - Target tenant id.
# $5 - Bucket name.
Script will look for folders like path/ceph/bucket1 path/ceph/bucket2 each containing data from 1 bucket and create bucket and upload
[Script Version -> 21.4]
'

red=$(tput setaf 1)
green=$(tput setaf 2)
yellow=$(tput setaf 3)
default=$(tput sgr0)

echo "$green $(date) Starting sync of object storage to local disk $default"

readonly CREDENTIALS_FILE=$1
readonly BASE_PATH=$2
readonly SOURCE_TENANT_ID=$3
readonly TARGET_TENANT_ID=$4
readonly BUCKET_NAME_INPUT=$5

# Validate file provided by user exists or not, It may be relative path or absolute path
# $1 - File path
function validate_file_path() {
  if [ ! -f "$1" ]; then
    echo "$red $(date) $1 file does not exist, Please check ... Exiting $default"
    exit 1
  fi
}

function initialize_variables() {
  # Validate file path
  validate_file_path $CREDENTIALS_FILE

  export AWS_HOST=$(cat $CREDENTIALS_FILE | jq -r 'select(.AWS_HOST != null) | .AWS_HOST')
  export AWS_ENDPOINT=$(cat $CREDENTIALS_FILE | jq -r 'select(.AWS_ENDPOINT != null) | .AWS_ENDPOINT')
  export AWS_ACCESS_KEY_ID=$(cat $CREDENTIALS_FILE | jq -r 'select(.AWS_ACCESS_KEY_ID != null) | .AWS_ACCESS_KEY_ID')
  export AWS_SECRET_ACCESS_KEY=$(cat $CREDENTIALS_FILE | jq -r 'select(.AWS_SECRET_ACCESS_KEY != null) | .AWS_SECRET_ACCESS_KEY')
  readonly DATA_FOLDER_NAME="ceph"
  readonly DATA_FOLDER_PATH=${BASE_PATH}/${DATA_FOLDER_NAME}/
}

function upload_blob() {
  BUCKET_NAME=${1}
  DIR_NAME=${2}
  TARGET_DIR_NAME=${3}
  # create bucket if not exists
  local check_bucket=$(s3cmd info --host=${AWS_ENDPOINT} --host-bucket= s3://${BUCKET_NAME} --no-check-certificate -q)
  if [ -z "$check_bucket" ]; then
    echo "$green $(date) Creating bucket ${BUCKET_NAME} $default"
    s3cmd mb --host=${AWS_ENDPOINT} --host-bucket= s3://${BUCKET_NAME} --no-check-certificate
  else
    echo "$yellow $(date)  Bucket exists: ${BUCKET_NAME}, skipping $default"
  fi

  # create folder if not exists

  # sync folder to bucket
  echo "$green $(date) Starting sync of object storage to local disk for bucket ${BUCKET_NAME} $default"

  ## Show failure, if training-tenantId bucket is not created already on S3.

  aws s3 --endpoint-url ${AWS_ENDPOINT} --no-verify-ssl --only-show-errors sync ${DATA_FOLDER_PATH}${DIR_NAME} s3://${BUCKET_NAME}/${TARGET_DIR_NAME}
  echo "$green $(date) Finsihed sync of object storage to local disk for bucket ${BUCKET_NAME} $default"
}

function update_cors_policy() {
  BUCKET_NAME=${1}
  DIR_NAME=${2}
  if [ ! -f "${DATA_FOLDER_PATH}${DIR_NAME}-cors.json" ]; then
    echo "$red $(date) ${DATA_FOLDER_PATH}${DIR_NAME}-cors.json file does not exist, Please check ... Skipping cors creation $default"
    return
  fi
  aws --endpoint-url $AWS_ENDPOINT --no-verify-ssl s3api put-bucket-cors --bucket ${BUCKET_NAME} --cors-configuration file://${DATA_FOLDER_PATH}${DIR_NAME}-cors.json
}

function _contains() {  # Check if space-separated list $1 contains item $2
  echo "$1" | tr ' ' '\n' | grep -F -x -q "$2"
}

function remove_unwanted_data_from_source_directory() {

  SOURCE_DIRECTORY=$1
  echo "$green $(date) Removing unwanted data from source directory $SOURCE_DIRECTORY $default"
  cd $DATA_FOLDER_PATH

  ## Fetch list of directories available in the ceph folder.
  DIRS=$(find . -maxdepth 1 -mindepth 1 -type d -printf '%f\n')

  ## Check if source tenant directory is present in the storage or not.
  if _contains "${DIRS}" "${SOURCE_DIRECTORY}"; then
    echo "$green $SOURCE_DIRECTORY present in the storage $default."
  else
    echo "$red $SOURCE_DIRECTORY not present in the storage $default."
    exit 1
  fi

  cd $DATA_FOLDER_PATH/$SOURCE_DIRECTORY

  ## Remove all unwanted directories from source folder.
  data=$(find . -maxdepth 1 -mindepth 1 -printf '%f\n')
  while read folder; do
    if [[ $folder =~ ^\{?[A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12}\}?$ ]]; then
      ## Only folder names which are in the form of guid are allowed, as they represent projectIds.
      echo "Pass $folder"
      continue;
    else
      echo "Deleting $folder"
      sudo rm -rf $folder
    fi
  done <<<"$data"

  cd $DATA_FOLDER_PATH
}

function change_source_to_target_tenant_id() {
  SOURCE_DIRECTORY=$1
  TARGET_DIRECTORY=$2

  echo "$green $(date) Changing source tenantId $SOURCE_DIRECTORY to target tenantId $TARGET_DIRECTORY $default"
  cd $DATA_FOLDER_PATH
  sudo mv $SOURCE_DIRECTORY $TARGET_DIRECTORY
}

function process_buckets() {

  cd $BASE_PATH
  SOURCE_DIRECTORY="training-"$SOURCE_TENANT_ID
  TARGET_DIRECTORY="training-"$TARGET_TENANT_ID
  remove_unwanted_data_from_source_directory $SOURCE_DIRECTORY
#  change_source_to_target_tenant_id $SOURCE_DIRECTORY $TARGET_DIRECTORY
  upload_blob ${BUCKET_NAME_INPUT} ${SOURCE_DIRECTORY} ${TARGET_DIRECTORY}
#  update_cors_policy ${BUCKET_NAME_INPUT} ${TARGET_DIRECTORY}
}

function process_ml_model_files() {

  echo "Inside process_ml_model_files $DATA_FOLDER_PATH, $"
  cd $BASE_PATH
  aws s3 --endpoint-url ${AWS_ENDPOINT} --no-verify-ssl --only-show-errors sync ${DATA_FOLDER_PATH}/ml-model-files/ s3://ml-model-files/
  cd -
}

# Validate dependency module
# $1 - Name of the dependency module
# $2 - Command to validate module
function validate_dependency() {
  eval $2
  # Next statement is checking last command success aws --version has some issue
  if [ $? -ne 0 ]; then
    echo "$red $(date) Please install ******** $1 ***********  ... Exiting $default"
    exit 1
  fi
}

# Validate required modules exits in target setup
function validate_setup() {
  validate_dependency "aws s3" "aws --version"
  validate_dependency s3cmd "s3cmd --version"
  echo "$(date) Successfully validated required dependencies"
}

# Validate Setup
validate_setup

# Update ENV Variables
initialize_variables

# Process data inside buckets
process_buckets

process_ml_model_files