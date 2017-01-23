package elasticache

import (
	"os"
	"testing"
)

func TestElastiCacheEndpoint(t *testing.T) {
	expectation := "foo"
	os.Setenv("ELASTICACHE_ENDPOINT", expectation)
	response, _ := elasticache()

	if response != expectation {
		t.Errorf("The response '%s' didn't match the expectation '%s'", response, expectation)
	}
}
func nodesEqual(n1 Node, n2 Node) bool {
	return n1.Host == n2.Host && n1.IP == n2.IP && n1.Port == n2.Port
}
func TestParseNodes(t *testing.T) {
	expectation := Node{Host: "localhost", IP: "127.0.0.1", Port: 11211}

	data := []string{"CONFIG cluster 0 25", "1", "localhost|127.0.0.1|11211", "", "END"}
	response, _ := parseNodeResult(&data)
	if len(*response) != 1 {
		t.Errorf("Unexpected number of nodes")
	}
	if !nodesEqual((*response)[0], expectation) {
		t.Errorf("The response '%s' didn't match the expectation '%s'", (*response)[0], expectation)
	}

}
func TestParseNodeLine(t *testing.T) {
	expectation := Node{Host: "host", IP: "foo", Port: 1}
	response, _ := parseNodeLine("host|foo|1")
	if !nodesEqual(expectation, response) {
		t.Errorf("Did not parse node information correctly.")
	}
}
func TestParseURLs(t *testing.T) {
	expectations := []Node{Node{Host: "host", IP: "foo", Port: 1},
		Node{Host: "host", IP: "bar", Port: 2},
		Node{Host: "host", IP: "baz", Port: 3}}

	data := []string{"host|foo|1 host|bar|2 host|baz|3"}
	response, _ := parseNodeResult(&data)

	if len(*response) != len(expectations) {
		t.Errorf("The response length '%d' didn't match the expectation '%d'", len(*response), len(expectations))
	}
	for i, n := range *response {
		if !nodesEqual(n, expectations[i]) {
			t.Errorf("Node at result in %d did not match expectation", i)
		}
	}
}
