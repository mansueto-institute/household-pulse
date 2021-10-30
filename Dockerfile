FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Install local package
COPY setup.py .
COPY household_pulse/ household_pulse/
COPY README.md .
RUN pip3 install --upgrade pip wheel
RUN pip3 install --use-feature=in-tree-build --target "${LAMBDA_TASK_ROOT}" .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]