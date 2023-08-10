#!bin/bash

JUPYTER_WORKSPACE_LOCATION=""

docker run -d \
    -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ \
    -e AWS_PROFILE=$AWS_PROFILE -e DISABLE_SSL=true --rm \
    -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab \
    amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh