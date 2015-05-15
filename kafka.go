package main

import (
    "fmt"
    "io/ioutil"
    "os/exec"
    "flag"
    "regexp"
    "os"
)

var zookeeper = flag.String("zookeeper", "", "Zookeeper host and port")
var describe = flag.Bool("describe", true, "Describe topics and their partitions in cluster")
var execute = flag.Bool("execute", false, "Execute command on directory of topics")

func main() {
    flag.Parse()
    if (*describe) {
        script := "bin/kafka-topics.sh"
        args := []string{"--zookeeper", *zookeeper, "--describe"}
        out, err := exec.Command(script, args...).Output()
        if err != nil {
            panic(err)
        }

        os.Mkdir("topics", 0777)

        regex := regexp.MustCompile("Topic:\\s+(\\S+?)\\s+Partition:\\s+(\\d+)\\s+Leader:\\s+(\\d+)\\s+Replicas:\\s+(\\d+)\\s+Isr:\\s+(\\d+)")
        submatches := regex.FindAllStringSubmatch(string(out), -1)
        for _, submatch := range submatches {
            jsonString := fmt.Sprintf("{\"Topic\": \"%s\", \"Partition\": \"%s\", \"Leader\": \"%s\", \"Replicas\": \"%s\", \"Isr\": \"%s\"}\n",
            submatch[1], submatch[2], submatch[3], submatch[4], submatch[5])
            ioutil.WriteFile(fmt.Sprintf("topics/%s-%s.json", submatch[1], submatch[2]), []byte(jsonString), 0777)
        }
    }
}
