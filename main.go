package main

import (
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("传参错误！")
	}

	nodeId := os.Args[1]
	if nodeMap[nodeId] == "" {
		log.Fatal("节点 ID 异常！")
	}
	node := NewNode(nodeId)

	listener, err := node.NewListener()
	if err != nil {
		log.Fatal("监听当前节点失败：", err.Error())
	}
	defer listener.Close()

	rpcServer := rpc.NewServer()
	rpcServer.Register(node)

	go rpcServer.Accept(listener)

	log.Println("等待系统启动……")

	time.Sleep(10 * time.Second)

	// 系统启动后首先进行心跳检测，确认系统中各个节点的活跃状态
	go node.HeartBeat()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<- ch
}