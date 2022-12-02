#!/usr/bin/core bash

AWS_CRED_PATH='~/.aws'
AWS_PROFILE='default'

SOURCE_PATH="${BASH_SOURCE[0]}"
SOURCE_DIR="$( dirname -- "${SOURCE_PATH}" )"
PROJECT_DIR="$( dirname -- "${SOURCE_DIR}" )"
SH_PROJECT_DIR="$(cd -- "$PROJECT_DIR" &> /dev/null && pwd )"

echo "AWS_CRED_PATH  ${AWS_CRED_PATH}"
echo "SOURCE_PATH    ${SOURCE_PATH}"
echo "SOURCE_DIR     ${SOURCE_DIR}"
echo "PROJECT_DIR    ${PROJECT_DIR}"
echo "SH_PROJECT_DIR ${SH_PROJECT_DIR}"

if [[ $# -lt 1 ]]; then
    echo "No script provided";
    read -p "Press any key to resume ..."
    exit 1;
fi

SCRIPT_NAME="$1"
SCRIPT_DIR="$( dirname -- "$1" )"
SCRIPT_PATH=$( realpath "$1" )
SH_SCRIPT_DIR="$(cd -- "$SCRIPT_DIR" &> /dev/null && pwd )"
SH_SCRIPT_DIR=$(realpath --relative-to="$SH_PROJECT_DIR" "$SH_SCRIPT_DIR")
SH_SCRIPT_BASE=$(basename "$SCRIPT_NAME")
SH_SCRIPT_PATH=$SH_SCRIPT_DIR/$SH_SCRIPT_BASE

echo "SCRIPT_NAME    ${SCRIPT_NAME}"
echo "SCRIPT_DIR     ${SCRIPT_DIR}"
echo "SCRIPT_PATH    ${SCRIPT_PATH}"
echo "SH_SCRIPT_DIR  ${SH_SCRIPT_DIR}"
echo "SH_SCRIPT_BASE ${SH_SCRIPT_PATH}"
echo "SH_SCRIPT_PATH ${SH_SCRIPT_PATH}"

shift

echo "-------------------------"
echo "PROJECT: $PROJECT_DIR"
echo "NAME: glue_spark_submit"
echo "COMMAND: -"
echo "COMMAND: cp /home/glue_user/workspace/$SH_SCRIPT_PATH $SH_SCRIPT_BASE && spark-submit $SH_SCRIPT_BASE $*"
echo "-------------------------"
read -p "Press any key to resume ..."
#spark-submit /home/glue_user/workspace/$SCRIPT_FILE_NAME $@"

docker run -it \
    -v "$AWS_CRED_PATH://home/glue_user/.aws" \
    -v "$PROJECT_DIR://home/glue_user/workspace" \
    -v "$PROJECT_DIR\\jupyter_workspace://home/glue_user/workspace/jupyter_workspace" \
    -v "$PROJECT_DIR\\pynutrien://home/glue_user/workspace/jupyter_workspace/pynutrien" \
    -e AWS_PROFILE="$AWS_PROFILE" \
    -e DISABLE_SSL=true \
    --rm \
    -p 4040:4040 -p 18080:18080 -p 8989:8989 -p 8888:8888 \
    --name glue_spark_submit \
    amazon/aws-glue-libs:glue_libs_3.0.0_image_01 \
    "cp /home/glue_user/workspace/$SH_SCRIPT_PATH $SH_SCRIPT_BASE && spark-submit $SH_SCRIPT_BASE $*"

# set conf
# redirect output to local logfile


read -p "Press any key to resume ..."
