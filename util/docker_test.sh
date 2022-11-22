WORKSPACE_LOCATION=/home/ubuntu/insights-framework
PROFILE_NAME=default
docker run -it -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pytest amazon/aws-glue-libs:glue_libs_3.0.0_image_01 -c "python3 -m test.run test/ --abc=hi --env_file_path=hihi --cfg_file_path=hihihi"
