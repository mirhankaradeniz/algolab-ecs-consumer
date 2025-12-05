FROM public.ecr.aws/lambda/python:3.11
# Bağımlılıklar
RUN python -m pip install websocket-client boto3
# Kod
COPY consumer.py ${LAMBDA_TASK_ROOT}/
# Entrypoint
CMD ["consumer.run"]
