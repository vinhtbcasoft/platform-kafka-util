package main

import (
	"flag"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"github.com/tbcasoft/platform-kafka-util/core"
	"os"
	"time"
)

/*
One time setup of the Go process includes:
- Reading command line args to determine mode [outbound | inbound | all] of xcmt.
- Reading command line args to Kafka broker
*/
func init() {
	flag.StringVar(&core.TopicName, "topic", "syslogs", "Default is syslogs, just like Vector.")
	flag.IntVar(&core.RequestsPerSsec, "rps", 1000, "The desired rps.")
	flag.StringVar(&core.InputFileName, "inputfile", "loadTestSimulatedMsg", "File name of message to enqueue.")
	flag.Var(&core.KafkaBrokers, "broker", "Argument (-broker) for host:port.  For multiple hosts, use argument for each host (e.g. -broker 10.8.9.1:9092 -broker 10.8.11.1:9092")
	flag.Parse()
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
	kafkaSyncProducer, err := core.NewSaramaSyncMessageProducer(core.KafkaBrokers)
	if err != nil {
		logrus.Errorf("Unable to get Kafka producer from broker (%s)", core.KafkaBrokers)
		panic(err)
	}

	start := time.Now()
	msgsEnqueued := 0
	for i := 0; i < core.RequestsPerSsec; i++ {
		partition, msgOffset, err := kafkaSyncProducer.SendMessage(msg)
		if err != nil {
			logrus.Error("Failed to enqueue received msg to topic %s.", core.TopicName)
			panic(err)
		}

		msgsEnqueued++

		logrus.Debugf("Msg enqueued to topic:partition (%s:%d), offset of enqueued (%d)", core.TopicName, partition, msgOffset)
	}

	//Note: elapse time unit is encapsulated, use its methods to get appropriate unit
	elapse := time.Since(start)
	logrus.Infof(" Goal of %d RPS, actually Enqueued %d. Cycle time (%.3f secs)", core.RequestsPerSsec, msgsEnqueued, elapse.Seconds())
}
