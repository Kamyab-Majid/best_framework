# running pytest using the docker

WORKSPACE_LOCATION=/home/ubuntu/insights-framework
PROFILE_NAME=default
docker run -t -d -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pytest amazon/aws-glue-libs:glue_libs_3.0.0_image_01 pyspark
docker exec -d  glue_pytest pip3 install --upgrade pip
docker exec glue_pytest pip3 install -r requirements_dev.txt
docker exec glue_pytest python3 -m run tests/ --abc=hi --env_file_path=hihi --cfg_file_path=hihihi
docker stop glue_pytest
# "pip install --upgrade pip" -c "pip install -r glue_requirements.txt "  -c "python3 -m run tests/ --abc=hi --env_file_path=hihi --cfg_file_path=hihihi"
