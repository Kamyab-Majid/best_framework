FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

ENV AWS_PROFILE=default
ENV DISABLE_SSL=true

# https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/

# EXPOSE

RUN python3 -m pip install -r requirements.txt

# pull the lib whl file
# RUN python3 -m pip install -r requirements.txt

