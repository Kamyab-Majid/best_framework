FROM public.ecr.aws/lambda/python:3.7

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
COPY pynutrien-0.1.0-py2.py3-none-any.whl  .

RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN  pip3 install pynutrien-0.1.0-py2.py3-none-any.whl --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY sample_lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "sample_lambda_function.lambda_handler" ] 
