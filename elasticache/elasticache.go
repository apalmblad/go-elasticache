package elasticache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/integralist/go-findroot/find"
	"github.com/hashicorp/go-version"
)

// Node is a single ElastiCache node
type Node struct {
	URL  string
	Host string
	IP   string
	Port int
}
type Node struct {
	Host string
	IP string
	Port int
}
type NodeList *Node[]

// Item embeds the memcache client's type of the same name
type Item memcache.Item

// Client embeds the memcache client so we can hide those details away
type Client struct {
	*memcache.Client
}

type StatInformation struct {
	Version version.Version
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

	return &Client{Client: clientForNodes( nodes )}, nil
}

func clientForNodes( n NodeList )  Client {
	urls := []string{}
	for _, node := range( *n ) {
		urls = append( urls, fmt.Sprintf( "%s:%d", node.Host, node.Port ) )
	}
	memcache.New( urls... )
}

func remoteCommand( conn io.ReaderWriter, command string ) string {
	fmt.Fprintf(conn, command + "\r\n" )
	var response string

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		if scanner.Text() == OUTPUT_END_MARKER {
			break
		}
		response += scanner.Text()

	}

	return response
}
var STATS_COMMAND = "stats"
var NEW_COMMAND = "config get cluster"
var OLD_COMMAND = "get AmazonElastiCache:cluster"
var NEW_COMMAND_AVAILABLE_VERSION, _ = versions.NewVersion( "1.4.14" )
var OUTPUT_END_MARKER = "END"
var VERSION_REGEX = regexp.MustCompile( /^STAT version ([0-9.]+)\s*$/ )
var NODE_SEPARATOR = " "

func getNodeData( conn io.ReaderWriter ) string {
	stats, err := parseStats( remoteCommand( conn, STATS_COMMAND ) )
	if err != nil {
		return nil, err
	}
	var nodeInfo string
	if stats.Version.LessThan(  NEW_COMMAND_AVAILABLE_VERSION ) {
		nodeInfo = remoteCommand( conn, OLD_COMMAND );
	} else {
		nodeInfo = remoteCommand( conn, NEW_COMMAND )
	}
	return nodeInfo
}

func clusterNodes() ([]string, error) {
	endpoint, err := elasticache()
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	nodeInfo := getNodeData( conn )
	nodes := NodeList{}
	for _, line := range( strings.split( nodeInfo, NODE_SEPARATOR ) ) {
		n, err := parseNodeLine( line ) 
		if( err != nil ) {
			return nil, err
		} else {
			nodes = append( nodes, n)
		}
	}
	return nodes
}

func elasticache() (string, error) {
	var endpoint string

	endpoint = os.Getenv("ELASTICACHE_ENDPOINT")
	if len(endpoint) == 0 {
		return "", errors.New("ElastiCache endpoint not set")
	}

	return endpoint, nil
}




func parseStats( stats string ) ( *StatInformation, error ) {
	ver := VERSION_REGEX.FindStringSubmatch( string )
	if ver == nil {
		return nil, errors.New("Did not find version information in results of STAT command")
	}
	rVal := StatInformation {}
	rVal.Version, err = version.NewVersion( ver )
	return &rVal, err
}

func parseNodeLine(nodeData string) ([]string, error) {
	fields := strings.Split(nodeData, "|") // ["host", "ip", "port"]
	rVal := Node {}
	rVal.Host = fields[0]
	rVal.Ip = fields[1]

	port, err := strconv.Atoi(fields[2])
	if err != nil {
		return rVal, err
	}
	rVal.Port = port
	return rVal, nil
}
