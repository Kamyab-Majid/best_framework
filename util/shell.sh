#!/usr/bin/core bash
AWS_CRED_PATH=~/.aws
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

shift
echo "-------------------------"
echo "PROJECT: $PROJECT_DIR"
echo "NAME: awsglue"
echo "COMMAND: -"
#echo "COMMAND: spark-submit \"$SH_SCRIPT_NAME\" $*"
echo "-------------------------"
read -p "Press any key to resume ..."
#spark-submit /home/glue_user/workspace/$SCRIPT_FILE_NAME $@"

docker run -it \
    -v $AWS_CRED_PATH:/home/glue_user/.aws \
    -v "$PROJECT_DIR:/home/glue_user/workspace" \
    -v "$PROJECT_DIR/jupyter_workspace/home/glue_user/workspace/jupyter_workspace" \
    -v "$PROJECT_DIR/pynutrien/home/glue_user/workspace/jupyter_workspace/pynutrien" \
    -e AWS_PROFILE="$AWS_PROFILE" \
    -e DISABLE_SSL=true \
    --rm \
    -p 4041:4041 -p 4040:4040 -p 18080:18080 -p 8989:8989 -p 8888:8888 \
    --name awsglue \
    amazon/aws-glue-libs:glue_libs_3.0.0_image_01

read -p "Press any key to resume ..."
