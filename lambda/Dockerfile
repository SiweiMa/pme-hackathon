FROM public.ecr.aws/lambda/python:3.12

# Install Python dependencies (boto3 is pre-installed in Lambda runtime)
COPY lambda/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r /tmp/requirements.txt

# Copy pme package (preserves package structure for "from pme.src.xxx" imports)
COPY pme/ ${LAMBDA_TASK_ROOT}/pme/

# Copy Lambda handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
