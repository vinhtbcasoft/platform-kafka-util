module github.com/tbcasoft/platform-kafka-util

go 1.12

replace github.com/IBM/sarama v1.38.1 => github.com/tbcasoft/sarama v1.38.2-0.20231011194508-ef97fc844e75

require (
	github.com/IBM/sarama v1.38.1
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9
	github.com/sirupsen/logrus v1.9.3
)
