sudo yum update -y
sudo amazon-linux-extras install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo docker run -d -p 9411:9411 openzipkin/zipkin