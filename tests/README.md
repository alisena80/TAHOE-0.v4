# Configuring Jupyter Container for QA Data testing:

## Do each time Jupyter container is restarted

1. Run this command to start jupyer container with aws cli:

docker run -it -v ~/.aws:/home/glue_user/.aws -v ~/workspace:/home/glue_user/workspace/jupyter_workspace/ -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh

2. install supporting libs
pip install pydeequ
pip install pytest_check
pip install import-ipynb

### (Do this every time you restart the Kernal)
3. set SPARK_VERSION env variable
export SPARK_VERSION=3.1.1-amzn-0

## Config once:
in /home/glue_user/.aws

.aws/config
[default]
region = us-west-1
output = json

[aws_profile]
region = us-gov-west-1
output = json

