FROM public.ecr.aws/glue/aws-glue-libs:5

WORKDIR /home/glue user/jupyter/workspace/lakehouse-iceberg-glue

ENV PYTHONPATH="$PYTHONPATH:/home/glue user/jupyter/workspace/lakehouse-iceberg-glue"

COPY . .