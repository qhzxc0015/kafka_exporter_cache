package main

import (
	"flag"
	"github.com/alecthomas/kingpin/v2"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/promlog"
	plogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/klog/v2"
	_ "net/http/pprof"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

const (
	INFO  = 0
	DEBUG = 1
	TRACE = 2
)

var (
	consumergroupLagSum  *prometheus.Desc
	consumergroupMembers *prometheus.Desc
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	mu                      sync.Mutex
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	offsetShowAll           bool
	topicWorkers            int
	allowConcurrent         bool
	sgMutex                 sync.Mutex
	sgWaitCh                chan struct{}
	sgChans                 []chan<- prometheus.Metric
	consumerGroupFetchAll   bool

	// cache
	allowCache      bool
	metricsMemCache map[string]float64
	metricsLagCache map[lagLables]float64
	cacheLock       sync.Mutex
}

type lagLables struct {
	consumergroup string
	topic         string
}

type kafkaOpts struct {
	uri                     []string
	kafkaVersion            string
	labels                  string
	metadataRefreshInterval string
	serviceName             string
	kerberosConfigPath      string
	realm                   string
	keyTabPath              string
	kerberosAuthType        string
	offsetShowAll           bool
	topicWorkers            int
	allowConcurrent         bool
	allowAutoTopicCreation  bool
	verbosityLogLevel       int

	openCache       bool
	cacheExpiration int
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot parse metadata refresh interval")
	}

	config.Metadata.RefreshFrequency = interval

	config.Metadata.AllowAutoTopicCreation = opts.allowAutoTopicCreation

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		return nil, errors.Wrap(err, "Error Init Kafka Client")
	}

	klog.V(TRACE).Infoln("Done Init Clients")

	allowCache := opts.openCache
	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
		offsetShowAll:           opts.offsetShowAll,
		topicWorkers:            opts.topicWorkers,
		allowConcurrent:         opts.allowConcurrent,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		allowCache:              allowCache,
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- consumergroupLagSum
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.sgMutex.Lock()
	e.sgChans = append(e.sgChans, ch)
	// Safe to compare length since we own the Lock
	if len(e.sgChans) == 1 {
		e.sgWaitCh = make(chan struct{})
		go e.collectChans(e.sgWaitCh)
	} else {
		klog.V(TRACE).Info("concurrent calls detected, waiting for first to finish")
	}
	// Put in another variable to ensure not overwriting it in another Collect once we wait
	waiter := e.sgWaitCh
	e.sgMutex.Unlock()
	// Released lock, we have insurance that our chan will be part of the collectChan slice
	<-waiter
	// collectChan finished
}

func (e *Exporter) collectChans(quit chan struct{}) {
	original := make(chan prometheus.Metric)
	container := make([]prometheus.Metric, 0, 100)
	go func() {
		for metric := range original {
			container = append(container, metric)
		}
	}()
	e.collect(original)
	close(original)
	// Lock to avoid modification on the channel slice
	e.sgMutex.Lock()
	for _, ch := range e.sgChans {
		for _, metric := range container {
			ch <- metric
		}
	}
	// Reset the slice
	e.sgChans = e.sgChans[:0]
	// Notify remaining waiting Collect they can return
	close(quit)
	// Release the lock so Collect can append to the slice again
	e.sgMutex.Unlock()
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) {
	if e.allowCache {
		e.cacheLock.Lock()
		defer e.cacheLock.Unlock()
		for key, value := range e.metricsMemCache {
			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, value, key,
			)
		}
		for key, value := range e.metricsLagCache {
			println("group: ", key.consumergroup, " value: ", int(value),"time",time.Now().Format("2006-01-02 15:04:05"))
			ch <- prometheus.MustNewConstMetric(
				consumergroupLagSum, prometheus.GaugeValue, value, key.consumergroup, key.topic,
			)
			time.Sleep(time.Microsecond*2)
		}
	} else {
		e.collectBegin(ch)
	}
}

func (e *Exporter) collectBegin(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		klog.V(DEBUG).Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			klog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	topics, err := e.client.Topics()
	if err != nil {
		klog.Errorf("Cannot get topics: %v", err)
		return
	}

	topicChannel := make(chan string)

	getTopicMetrics := func(topic string) {
		defer wg.Done()

		if !e.topicFilter.MatchString(topic) {
			return
		}

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			klog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
			return
		}
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {

			currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				klog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
			} else {
				e.mu.Lock()
				offset[topic][partition] = currentOffset
				e.mu.Unlock()
			}
		}
	}

	loopTopics := func() {
		ok := true
		for ok {
			topic, open := <-topicChannel
			ok = open
			if open {
				getTopicMetrics(topic)
			}
		}
	}

	minx := func(x int, y int) int {
		if x < y {
			return x
		} else {
			return y
		}
	}

	N := len(topics)
	if N > 1 {
		N = minx(N/2, e.topicWorkers)
	}

	for w := 1; w <= N; w++ {
		go loopTopics()
	}

	for _, topic := range topics {
		if e.topicFilter.MatchString(topic) {
			wg.Add(1)
			topicChannel <- topic
		}
	}
	close(topicChannel)

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			klog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			klog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			klog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			if group.State != "Stable" {
				continue
			}
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			if e.offsetShowAll {
				for topic, partitions := range offset {
					for partition := range partitions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			} else {
				for _, member := range group.Members {
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						klog.Errorf("Cannot get GetMemberAssignment of group member %v : %v", member, err)
						return
					}
					for topic, partions := range assignment.Topics {
						for _, partition := range partions {
							offsetFetchRequest.AddPartition(topic, partition)
						}
					}
				}
			}

			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
			)

			offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
			if err != nil {
				klog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
				continue
			}

			for topic, partitions := range offsetFetchResponse.Blocks {
				// If the topic is not consumGatherersed by that consumer group, skip it
				topicConsumed := false
				for _, offsetFetchResponseBlock := range partitions {
					// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
					if offsetFetchResponseBlock.Offset != -1 {
						topicConsumed = true
						break
					}
				}
				if !topicConsumed {
					continue
				}

				var currentOffsetSum int64
				var lagSum int64
				for partition, offsetFetchResponseBlock := range partitions {
					err := offsetFetchResponseBlock.Err
					if err != sarama.ErrNoError {
						klog.Errorf("Error for  partition %d :%v", partition, err.Error())
						continue
					}
					currentOffset := offsetFetchResponseBlock.Offset
					currentOffsetSum += currentOffset
					e.mu.Lock()
					if offset, ok := offset[topic][partition]; ok {
						// If the topic is consumed by that consumer group, but no offset associated with the partition
						// forcing lag to -1 to be able to alert on that
						var lag int64
						if offsetFetchResponseBlock.Offset == -1 {
							lag = -1
						} else {
							lag = offset - offsetFetchResponseBlock.Offset
							lagSum += lag
						}
					} else {
						klog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
					}
					e.mu.Unlock()
				}
				ch <- prometheus.MustNewConstMetric(
					consumergroupLagSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
				)
			}
		}
	}

	klog.V(DEBUG).Info("Fetching consumer group metrics")
	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
			break
		}
		wg.Wait()
	} else {
		klog.Errorln("No valid broker, cannot get consumer group metrics")
	}
}

func (e *Exporter) updateCache() {
	e.cacheLock.Lock()
	var wg = sync.WaitGroup{}
	offset := make(map[string]map[int32]int64)
	topics, err := e.client.Topics()
	if err != nil {
		klog.Errorf("Cannot get topics: %v", err)
		return
	}

	topicChannel := make(chan string)
	getTopicMetrics := func(topic string) {
		defer wg.Done()

		if !e.topicFilter.MatchString(topic) {
			return
		}

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			klog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
			return
		}
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {

			currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				klog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
			} else {
				e.mu.Lock()
				offset[topic][partition] = currentOffset
				e.mu.Unlock()
			}
		}
	}

	loopTopics := func() {
		ok := true
		for ok {
			topic, open := <-topicChannel
			ok = open
			if open {
				getTopicMetrics(topic)
			}
		}
	}

	minx := func(x int, y int) int {
		if x < y {
			return x
		} else {
			return y
		}
	}

	N := len(topics)
	if N > 1 {
		N = minx(N/2, e.topicWorkers)
	}

	for w := 1; w <= N; w++ {
		go loopTopics()
	}

	for _, topic := range topics {
		if e.topicFilter.MatchString(topic) {
			wg.Add(1)
			topicChannel <- topic
		}
	}
	close(topicChannel)

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			klog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			klog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			klog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		e.metricsMemCache = make(map[string]float64)
		e.metricsLagCache = make(map[lagLables]float64)
		for _, group := range describeGroups.Groups {
			if group.State != "Stable" {
				continue
			}
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			if e.offsetShowAll {
				for topic, partitions := range offset {
					for partition := range partitions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			} else {
				for _, member := range group.Members {
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						klog.Errorf("Cannot get GetMemberAssignment of group member %v : %v", member, err)
						return
					}
					for topic, partions := range assignment.Topics {
						for _, partition := range partions {
							offsetFetchRequest.AddPartition(topic, partition)
						}
					}
				}
			}
			e.metricsMemCache[group.GroupId] = float64(len(group.Members))
			offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
			if err != nil {
				klog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
				continue
			}

			for topic, partitions := range offsetFetchResponse.Blocks {
				// If the topic is not consumGatherersed by that consumer group, skip it
				topicConsumed := false
				for _, offsetFetchResponseBlock := range partitions {
					// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
					if offsetFetchResponseBlock.Offset != -1 {
						topicConsumed = true
						break
					}
				}
				if !topicConsumed {
					continue
				}

				var currentOffsetSum int64
				var lagSum int64
				for partition, offsetFetchResponseBlock := range partitions {
					err := offsetFetchResponseBlock.Err
					if err != sarama.ErrNoError {
						klog.Errorf("Error for  partition %d :%v", partition, err.Error())
						continue
					}
					currentOffset := offsetFetchResponseBlock.Offset
					currentOffsetSum += currentOffset
					e.mu.Lock()
					if offset, ok := offset[topic][partition]; ok {
						// If the topic is consumed by that consumer group, but no offset associated with the partition
						// forcing lag to -1 to be able to alert on that
						var lag int64
						if offsetFetchResponseBlock.Offset == -1 {
							lag = -1
						} else {
							lag = offset - offsetFetchResponseBlock.Offset
							lagSum += lag
						}

					} else {
						klog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
					}
					e.metricsLagCache[lagLables{consumergroup: group.GroupId, topic: topic}] = float64(lagSum)
				}
				e.mu.Unlock()
			}
		}
	}

	klog.V(DEBUG).Info("Fetching consumer group metrics")
	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
			break
		}
		wg.Wait()
	} else {
		klog.Errorln("No valid broker, cannot get consumer group metrics")
	}
	e.cacheLock.Unlock()
}

func init() {
	prometheus.DefaultRegisterer.Unregister(prometheus.NewGoCollector())
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

// hack around flag.Parse and klog.init flags
func toFlagString(name string, help string, value string) *string {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(value).String()
}

func toFlagBool(name string, help string, value bool, valueString string) *bool {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(valueString).Bool()
}

func toFlagStringsVar(name string, help string, value string, target *[]string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringsVar(target)
}

func toFlagStringVar(name string, help string, value string, target *string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringVar(target)
}

func toFlagBoolVar(name string, help string, value bool, valueString string, target *bool) {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).BoolVar(target)
}

func toFlagIntVar(name string, help string, value int, valueString string, target *int) {
	flag.CommandLine.Int(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).IntVar(target)
}

func main() {
	var (
		listenAddress = toFlagString("web.listen-address", "Address to listen on for web interface and telemetry.", ":9308")
		metricsPath   = toFlagString("web.telemetry-path", "Path under which to expose metrics.", "/metrics")
		topicFilter   = toFlagString("topic.filter", "Regex that determines which topics to collect.", ".*")
		groupFilter   = toFlagString("group.filter", "Regex that determines which consumer groups to collect.", ".*")
		logSarama     = toFlagBool("log.enable-sarama", "Turn on Sarama logging, default is false.", false, "false")

		opts = kafkaOpts{}
	)

	toFlagStringsVar("kafka.server", "Address (host:port) of Kafka server.", "kafka:9092", &opts.uri)
	toFlagStringVar("kafka.version", "Kafka broker version", sarama.V2_0_0_0.String(), &opts.kafkaVersion)
	toFlagStringVar("kafka.labels", "Kafka cluster name", "", &opts.labels)
	toFlagStringVar("refresh.metadata", "Metadata refresh interval", "30s", &opts.metadataRefreshInterval)
	toFlagBoolVar("offset.show-all", "Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups, default is true", true, "true", &opts.offsetShowAll)
	toFlagIntVar("topic.workers", "Number of topic workers", 100, "100", &opts.topicWorkers)
	toFlagIntVar("verbosity", "Verbosity log level", 0, "0", &opts.verbosityLogLevel)
	//cache
	toFlagBoolVar("cache.enable", "If true, collect data from cache", false, "false", &opts.openCache)
	toFlagIntVar("cache.expiration", "Cache expiration time", 60, "60", &opts.cacheExpiration)

	plConfig := plog.Config{}
	plogflag.AddFlags(kingpin.CommandLine, &plConfig)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	setup(*listenAddress, *metricsPath, *topicFilter, *groupFilter, *logSarama, opts, labels)
}

func setup(
	listenAddress string,
	metricsPath string,
	topicFilter string,
	groupFilter string,
	logSarama bool,
	opts kafkaOpts,
	labels map[string]string,
) {
	klog.InitFlags(flag.CommandLine)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Errorf("Error on setting logtostderr to true: %v", err)
	}
	err := flag.Set("v", strconv.Itoa(opts.verbosityLogLevel))
	if err != nil {
		klog.Errorf("Error on setting v to %v: %v", strconv.Itoa(opts.verbosityLogLevel), err)
	}
	defer klog.Flush()

	klog.V(DEBUG).Infoln("Build context", version.BuildContext())

	consumergroupLagSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_sum"),
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, topicFilter, groupFilter)
	if err != nil {
		klog.Fatalln(err)
	}
	defer exporter.client.Close()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	prometheus.MustRegister(exporter)

	http.Handle(metricsPath, promhttp.HandlerFor(prometheus.DefaultGatherer, (promhttp.HandlerOpts{EnableOpenMetrics: false})))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
		if err != nil {
			klog.Error("Error handle / request", err)
		}
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// need more specific sarama check
		_, err := w.Write([]byte("ok"))
		if err != nil {
			klog.Error("Error handle /healthz request", err)
		}
	})

	if opts.openCache {
		klog.V(INFO).Infoln("begin cache")
		exporter.updateCache()
		// 启动新的 Goroutine，更新缓存，每隔cacheExpiration秒更新一次
		go func() {
			ticker := time.NewTicker(time.Second * time.Duration(opts.cacheExpiration))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					klog.V(INFO).Infoln("update cache: "+time.Now().Format("2006-01-02 15:04:05"))
					exporter.updateCache()
				}
			}
		}()
	}
	klog.V(INFO).Infoln("Listening on HTTP", listenAddress)
	klog.Fatal(http.ListenAndServe(listenAddress, nil))
}
