# this is used to run jupyter in docker container.

JUPYTER_WORKSPACE_LOCATION=/home/ubuntu/insights-framework/
WORKSPACE_LOCATION=/home/ubuntu/insights-framework
sudo cp requirements.txt jupyter_workspace/
sudo cp pynutrien jupyter_workspace/  -r
chmod 400 jupyter_workspace/pynutrien -R
docker run -it  -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION/jupyter_workspace:/home/glue_user/workspace/jupyter_workspace -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01  /home/glue_user/jupyter/jupyter_start.sh
sudo rm  jupyter_workspace/pynutrien  -rf 
sudo rm  jupyter_workspace/requirements.txt
