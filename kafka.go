package main

import (
    "io/ioutil"
    "os/exec"
    "flag"
    "regexp"
    "encoding/json"
    "strconv"
    "strings"
    "fmt"
)

var zookeeper = flag.String("zookeeper", "", "Zookeeper host and port")

func main() {
    flag.Parse()
    script := "bin/kafka-topics.sh"
    args := []string{"--zookeeper", *zookeeper, "--describe"}
    out, err := exec.Command(script, args...).Output()
    if err != nil {
        panic(err)
    }

    regex := regexp.MustCompile("Topic:\\s+(\\S+?)\\s+Partition:\\s+(\\d+)\\s+Leader:\\s+(\\d+)\\s+Replicas:\\s+(\\d+)\\s+Isr:\\s+(\\d+)")
    submatches := regex.FindAllStringSubmatch(string(out), -1)
    partitions := &Partitions{
        Partitions: make([]*Partition, 0),
    }
    for _, submatch := range submatches {
        partitions.Partitions = append(partitions.Partitions, &Partition{
            Topic: submatch[1],
            Partition: mustBeInt(submatch[2]),
            Leader: mustBeInt(submatch[3]),
            Replicas: mustBeIntArr(submatch[4]),
            Isr: mustBeIntArr(submatch[5]),
        })

    }

    data, err := json.Marshal(partitions)
    if err != nil {
        panic(err)
    }

    ioutil.WriteFile("partitions.json", data, 0777)

    script = "bin/kafka-reassign-partitions.sh"
    args = []string{"--zookeeper", *zookeeper, "--execute", "--reassignment-json-file", "partitions.json"}
    out, err = exec.Command(script, args...).Output()
    if err != nil {
        panic(err)
    }
    fmt.Println(string(out))
}

func mustBeInt(value string) int {
    result, err := strconv.Atoi(value)
    if err != nil {
        panic(err)
    }

    return result
}

func mustBeIntArr(value string) []int {
    ids := make([]int, 0)
    for _, id := range strings.Split(value, ",") {
        ids = append(ids, mustBeInt(id))
    }

    return ids
}

type Partitions struct {
    Partitions []*Partition `json:"partitions"`
}

type Partition struct {
    Topic string `json:"topic"`
    Partition int `json:"partition"`
    Leader int `json:"leader"`
    Replicas []int `json:"replicas"`
    Isr []int `json:"isr"`
}
