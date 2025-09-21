package core

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"os"
)

var (
	TopicName       string
	RequestsPerSsec int
	InputFileName   string
	LogFileName     = "platform_kafka_util.log"
)

type brokersList []string

//== Since variable is defined as interface Var (main.go), we need to implement its methods.

func (i *brokersList) String() string { // String is an implementation of the flag.Value interface.  It's called by any fmt.* mehtods to print its value.
	return fmt.Sprintf("%v", *i)
}

func (i *brokersList) Set(value string) error { // Set is an implementation of the flag.Value interface.  It will be called by flag.Parse()
	*i = append(*i, value)
	return nil
}

var KafkaBrokers brokersList

func NewSaramaSyncMessageProducer(brokerList []string) (sarama.SyncProducer, error) {
	/*
		For this type of message producer, we are looking for high throughput, low latency.
		The sacrifice is we fail fast.
	*/

	metrics.UseNilMetrics = true //required to remove not use metrics because it has memory leak, see jira plat-445 for details

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal //only just one commit has to be done before we proceed
	config.Producer.Retry.Max = 1                      // Retry up to 1 times to produce the message
	config.Producer.Return.Successes = true            //For sync producer, this must be set to true and you shall not read from the channels since the producer does this internally.
	config.Producer.Return.Errors = true               //For sync producer, this must be set to true and you shall not read from the channels since the producer does this internally.

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	return sarama.NewSyncProducer(brokerList, config)
}

func FileExists(name string) bool {
	info, err := os.Stat(name)
	if os.IsNotExist(err) || info == nil {
		return false
	}
	return !info.IsDir()
}
