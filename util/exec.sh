
# various setup
docker exec awsglue python3 -m pip install -r requirements_dev.txt
docker exec awsglue python3 -m pip install pydevd-pycharm~=212.5080.64

# check if awsglue running, otherwise start it
 docker exec awsglue python3 $@
