package eventbus

import "strings"

// topicMatches implements MQTT-style wildcard matching: "+" matches exactly
// one segment, "#" matches the remainder of the topic. Shared by Memory and
// MQTT so routing behaves identically in tests and production.
func topicMatches(pattern, topic string) bool {
	pSegs := strings.Split(pattern, "/")
	tSegs := strings.Split(topic, "/")
	for i, p := range pSegs {
		if p == "#" {
			return true
		}
		if i >= len(tSegs) {
			return false
		}
		if p != "+" && p != tSegs[i] {
			return false
		}
	}
	return len(pSegs) == len(tSegs)
}
