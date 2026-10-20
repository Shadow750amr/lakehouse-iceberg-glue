FROM public.ecr.aws/glue/aws-glue-libs:5

WORKDIR /home/glue user/jupyter/workspace/lakehouse-iceberg-glue

COPY . .