## Installing Kafka and Zookeeper Using Containers

Installing a Kafka Cluster using containers is a quick way to get up and running. It's portable and lightweight, so we can use this on any machine running Docker. You'll see in this lesson, it takes much less time to get to the point where we can create our first topic. See the below commands for easily copying and pasting into your own terminal:


### Add Docker to Your Package Repository


			curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

			sudo add-apt-repository    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
			   $(lsb_release -cs) \
			   stable"

### Update Packages and Install Docker


			sudo apt update

			sudo apt install -y docker-ce=18.06.1~ce~3-0~ubuntu



### Add Your User to the Docker Group

      sudo usermod -a -G docker cloud_user
      
      
### Install Docker Compose

			sudo -i

			curl -L https://github.com/docker/compose/releases/download/1.24.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose

			chmod +x /usr/local/bin/docker-compose



### Clone the Repository That Has Our Docker Compose File


      git clone https://github.com/linuxacademy/content-kafka-deep-dive.git


### Change Directory and Run the Compose YAML File


      cd content-kafka-deep-dive

      docker-compose up -d --build

### Install Java


      sudo apt install -y default-jdk


### Get the Kafka Binaries


			wget http://mirror.cogentco.com/pub/apache/kafka/2.2.0/kafka_2.12-2.2.0.tgz

			tar -xvf kafka_2.12-2.2.0.tgz



### Create Your First Topic

        ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic test --partitions 3 --replication-factor 1


###  Describe the Topic

      ./bin/kafka-topics.sh --zookeeper localhost:2181 --topic test --describe


-------------------------------------------------


Now that we've setup our Kafka cluster, let's explore some of the various commands for creating topics, and producing and consuming messages. In this lesson, we'll go over how to determine what flag to use, as well as how to use a combination of flags. Overall, the command line is friendly, giving verbose explanation when someone does something wrong.

### Detail for the topics command

        bin/kafka-topics.sh

### Creating a topic will all the required arguments

        bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --topic test1 --create --partitions 3 --replication-factor 3

### Creating a topic including all of the zookeeper servers (not required)

        bin/kafka-topics.sh --zookeeper zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/kafka --topic test1 --create --partitions 3 --replication-factor 3

### List all topics

    bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --list

### Describing a topic

     bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --topic test2 --describe

### Delete a topic

     bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --topic test2 --delete

### Detail for the producer command

       bin/kafka-console-producer.sh

### Detail for the consumer command

       bin/kafka-console-consumer.sh

### Detail for the consumer groups command

     bin/kafka-consumer-groups.sh



   ------------------
   
   
By using a Producer, you can publish messages to the Kafka cluster. In this lesson we'll produce some messages to the topics that we've created thus far. There are a few items to remember when creating topics and default partitions.

### Start a console producer to topic 'test'

		bin/kafka-console-producer.sh --broker-list kafka1:9092 --topic test

### Add the acks=all flag to your producer

		bin/kafka-console-producer.sh --broker-list kafka1:9092 --topic test --producer-property acks=all

### Create a topic with the console producer (not recommended)

		bin/kafka-console-producer.sh --broker-list kafka1:9092 --topic test4

### List the newly created topic

		bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --list

### View the partitions for a topic

		bin/kafka-topics.sh --zookeeper zookeeper1:2181/kafka --topic test5 --describe


   
