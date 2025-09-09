package main

import (
	"flag"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"github.com/tbcasoft/platform-kafka-util/core"
	"os"
)

/*
One time setup of the Go process includes:
- Reading command line args to determine mode [outbound | inbound | all] of xcmt.
- Reading command line args to Kafka broker
*/
func init() {
	flag.StringVar(&core.TopicName, "topic", "xcm_egress_syslogs_tbca", "Default is xcm_egress_syslogs_tbca")
	flag.IntVar(&core.RequestsPerSsec, "rps", 100, "The desired rps.")
	flag.StringVar(&core.InputFileName, "inputfile", "loadTestSimulatedMsg", "File name of message to enqueue.")
}

func main() {
	logHandle, err := os.OpenFile(core.LogFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		println("Error opening this process log file", err)
		os.Exit(1)
	}

	defer logHandle.Close()
	if !core.FileExists(core.LogFileName) {
		println("kafka util log file does not exist.", core.LogFileName)
		os.Exit(1)
	}

	logrus.SetOutput(logHandle)

	//== Read the msg to eqneuue to memory
	bytes, err := os.ReadFile(core.InputFileName)
	msg := &sarama.ProducerMessage{
		Topic: core.TopicName,
		Value: sarama.ByteEncoder(bytes),
	}

	//== get producer
	kafkaSyncProducer, err := core.NewSaramaSyncMessageProducer(core.Brokers)
	if err != nil {
		logrus.Errorf("Unable to get Kafka producer from broker (%s)", core.Brokers)
		panic(err)
	}

	//== TODO: loop through to achieve desired RPS
	for i := 0; i < 1000; i++ {
		partition, msgOffset, err := kafkaSyncProducer.SendMessage(msg)
		if err != nil {
			logrus.Error("Failed to enqueue received msg to topic %s.", core.TopicName)
			panic(err)
		}

		logrus.Infof("Msg enqueued to topic:partition (%s:%d), offset of enqueued (%d)",
			core.TopicName, partition, msgOffset)
	}

}
