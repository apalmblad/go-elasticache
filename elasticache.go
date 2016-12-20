package elasticache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hashicorp/go-version"
)

type Node struct {
	Host string
	IP   string
	Port int
}

type NodeList *[]Node

func (node Node) URL() string {
	if node.Host != "" {
		return fmt.Sprintf("%s:%d", node.Host, node.Port)
	} else {
		return fmt.Sprintf("%s:%d", node.IP, node.Port)
	}
}

// Item embeds the memcache client's type of the same name
type Item memcache.Item

// Client embeds the memcache client so we can hide those details away
type Client struct {
	*memcache.Client
}

type StatInformation struct {
	Version *version.Version
}

// Set abstracts the memcache client details away,
// by copying over the values provided by the user into the Set method,
// as coercing the custom Item type to the required memcache.Item type isn't possible.
// Downside is if memcache client fields ever change, it'll introduce a break
func (c *Client) Set(item *Item) error {
	return c.Client.Set(&memcache.Item{
		Key:        item.Key,
		Value:      item.Value,
		Expiration: item.Expiration,
	})
}

// New returns an instance of the memcache client
func New() (*Client, error) {
	nodes, err := clusterNodes()
	if err != nil {
		return nil, err
	}

	return &Client{Client: clientForNodes(nodes)}, nil
}

func clientForNodes(n NodeList) *memcache.Client {
	urls := []string{}
	for _, node := range *n {
		urls = append(urls, node.URL())
	}
	return memcache.New(urls...)
}

func remoteCommand(conn io.ReadWriter, command string) string {
	fmt.Fprintf(conn, command+"\r\n")
	var response string

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		if scanner.Text() == OUTPUT_END_MARKER {
			break
		}
		response += scanner.Text() + "\r\n"

	}

	return response
}

var STATS_COMMAND = "stats"
var NEW_COMMAND = "config get cluster"
var OLD_COMMAND = "get AmazonElastiCache:cluster"
var NEW_COMMAND_AVAILABLE_VERSION, _ = version.NewVersion("1.4.14")
var OUTPUT_END_MARKER = "END"
var VERSION_REGEX = regexp.MustCompile("(?m)^STAT version ([0-9.]+)\\s*$")
var NODE_SEPARATOR = " "

func getNodeData(conn io.ReadWriter) (*string, error) {
	stats, err := parseStats(remoteCommand(conn, STATS_COMMAND))
	if err != nil {
		return nil, err
	}
	var nodeInfo string
	if stats.Version.LessThan(NEW_COMMAND_AVAILABLE_VERSION) {
		nodeInfo = remoteCommand(conn, OLD_COMMAND)
	} else {
		nodeInfo = remoteCommand(conn, NEW_COMMAND)
	}
	return &nodeInfo, nil
}

func clusterNodes() (NodeList, error) {
	endpoint, err := elasticache()
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	nodeInfo, err := getNodeData(conn)
	if err != nil {
		return nil, err
	}
	nodes := []Node{}
	for _, line := range strings.Split(*nodeInfo, NODE_SEPARATOR) {
		n, err := parseNodeLine(line)
		if err != nil {
			return nil, err
		} else {
			nodes = append(nodes, n)
		}
	}
	return &nodes, nil
}

func elasticache() (string, error) {
	var endpoint string

	endpoint = os.Getenv("ELASTICACHE_ENDPOINT")
	if len(endpoint) == 0 {
		return "", errors.New("ElastiCache endpoint not set")
	}

	return endpoint, nil
}

func parseStats(stats string) (*StatInformation, error) {
	ver := VERSION_REGEX.FindStringSubmatch(stats)
	if ver == nil || len(ver) < 2 {
		return nil, errors.New("Did not find version information in results of STAT command")
	}
	rVal := StatInformation{}
	var err error
	rVal.Version, err = version.NewVersion(ver[1])
	return &rVal, err
}

func parseNodeLine(nodeData string) (Node, error) {
	fields := strings.Split(nodeData, "|")
	rVal := Node{}
	rVal.Host = fields[0]
	rVal.IP = fields[1]

	port, err := strconv.Atoi(fields[2])
	if err != nil {
		return rVal, err
	}
	rVal.Port = port
	return rVal, nil
}
