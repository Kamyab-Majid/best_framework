# this is used to run jupyter in docker container.

JUPYTER_WORKSPACE_LOCATION=/home/ubuntu/insights-framework/
WORKSPACE_LOCATION=/home/ubuntu/insights-framework

docker run -it  -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION/notebooks:/home/glue_user/workspace/jupyter_workspace -v $WORKSPACE_LOCATION/pynutrien:/home/glue_user/workspace/jupyter_workspace/pynutrien -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01  /home/glue_user/jupyter/jupyter_start.sh
