# aws cloudformation deploy --profile thinkport --region eu-central-1 --template-file ./cloudformation/vpc.yaml --stack-name KafkaDemoVPC

aws cloudformation deploy --profile thinkport --region eu-central-1 --template-file ./cloudformation/vpc_github.yaml --stack-name KafkaDemoVPC2

aws cloudformation deploy --profile thinkport --region eu-central-1 --template-file ./cloudformation/asg.yaml --stack-name KafkaDemoCluster --parameter-overrides UserData=$(cat ./dynamic_ips.sh | base64)

aws cloudformation deploy --profile thinkport --region eu-central-1 --template-file ./cloudformation/ec2_zipkin.yaml --stack-name ZipkinInstance --parameter-overrides UserData=$(cat ./zipkin_userdata.sh | base64)

# aws cloudformation deploy --profile thinkport --region eu-central-1 --template-file ./cloudformation/ec2.yaml --stack-name KafkaDemoSingleInstance2 --parameter-overrides UserData=$(cat ./userdata.sh | base64) --query 'Stacks[0].Outputs[?OutputKey==`KafkaInstance`].OutputValue' --output text

