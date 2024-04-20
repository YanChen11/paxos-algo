package main

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	Live = true
	Dead = false
)

// 各节点以及监听地址的映射信息
var nodeMap = map[string]string{
	"node1":    "127.0.0.1:8081",
	"node2":    "127.0.0.1:8082",
	"node3":    "127.0.0.1:8083",
	"node4":    "127.0.0.1:8084",
	"node5":    "127.0.0.1:8085",
}

// 各节点当前的状态
var nodeStatus = map[string]bool{
	"node1":    Dead,
	"node2":    Dead,
	"node3":    Dead,
	"node4":    Dead,
	"node5":    Dead,
}

// 接受者最近一次接受的提案号以及提案值
var (
	preProposalId int64 = 0
	preProposalValue string = ""
)

var rpcClientPool = sync.Pool{
	New: func() any {
		return &rpc.Client{}
	},
}

// 节点信息
type Node struct {
	Id          string
	Addr        string
}

// 创建新的节点
func NewNode(id string) *Node {
	node := &Node{
		Id:        id,
		Addr:      nodeMap[id],
	}

	return node
}

// 监听当前节点
func (node *Node) NewListener() (net.Listener, error) {
	listener, err := net.Listen("tcp", node.Addr)

	return listener, err
}

// 与节点进行通信
func (node *Node) CommunicateWithSibling(nodeAddr string, message Message) (Message, error) {
	var response Message
	var err error

	rpcClient := rpcClientPool.Get().(*rpc.Client)
	defer rpcClientPool.Put(rpcClient)

	rpcClient, err = rpc.Dial("tcp", nodeAddr)

	if err != nil {
		log.Printf("与节点 %s:%s 建立连接失败：%s\n", message.To, nodeAddr, err)
		return response, err
	}

	err = rpcClient.Call("Node.RespondTheMessage", message, &response)

	if err != nil {
		log.Printf("与节点通信失败：%s\n", err)
	}

	return response, err
}

// 响应消息
func (node *Node) RespondTheMessage(message Message, response *Message) error {
	response.From = node.Id
	response.To = message.From

	switch message.Type {
	case Ping:
		response.Type = Pong
	case Prepare:
		if preProposalId == 0 {
			response.Type = Promise
			preProposalId = message.Id
		} else if preProposalId < message.Id {
			response.Type = Promise
			response.Id = preProposalId
			response.Value = preProposalValue
			preProposalId = message.Id
		} else {
			response.Type = Nack
		}
	case Accept:
		if preProposalId <= message.Id {
			response.Type = Accepted
			response.Id = message.Id
			response.Value = message.Value
			preProposalId = message.Id
			preProposalValue = message.Value
		} else {
			response.Type = Nack
		}
	case Accepted:
	//	todo learner implement
	}

	return nil
}

// 节点心跳检测
func (node *Node) HeartBeat() {
	message := Message{
		From: node.Id,
		Type: Ping,
	}
ping:
	for nodeId, nodeAddr := range nodeMap {
		if nodeId == node.Id {
			nodeStatus[node.Id] = Live
			// 不检测自身
			continue
		}

		message.To = nodeId
		response, err := node.CommunicateWithSibling(nodeAddr, message)

		if err != nil {
			log.Printf("检测节点 %s 的心跳失败：%s\n", message.To, err)
			nodeStatus[message.To] = Dead
			continue
		}

		log.Printf("节点 %s 心跳检测响应：%v\n", message.To, response)
		if response.Type == Pong {
			nodeStatus[message.To] = Live
		}
	}

	time.Sleep(5 * time.Second)
	goto ping
}

func (node *Node) majority() int {
	return len(nodeMap) / 2 + 1
}

// 判断系统中大多数节点是否仍处于活跃状态
func (node *Node) isMajorityNodeLived() bool {
	majority := node.majority()

	for _, status := range nodeStatus {
		if status {
			majority --
		}
	}

	return majority <= 0
}

/*********** proposer ***********/
// 发起提案
func (node *Node) Propose(val string, reply *Message) error {
	var response Message
	var err error
	proposal := Message{
		From:  node.Id,
		To:    "",
		Type:  Prepare,
		Id:    time.Now().UnixNano(),
		Value: val,
	}

retry:
	// 发送 prepare 消息
	if !node.isMajorityNodeLived() {
		log.Println("大多数节点已经失效")
		return nil
	}

	majority := node.majority()
	for nodeId, nodeAddr := range nodeMap {
		if !nodeStatus[nodeId] || nodeId == node.Id {
			// 跳过失效的节点
			continue
		}

		proposal.To = nodeId
		response, err = node.CommunicateWithSibling(nodeAddr, proposal)

		if err != nil {
			log.Printf("向节点 %s 发送 prepare 消息失败\n", proposal.To)
			continue
		}

		log.Printf("response %d from %s \n", response.Type, response.From)

		if response.Type == Promise {
			majority --
		}

		if majority <= 0 {
			// 大多数节点返回 promise
			proposal.Type = Promise
			break
		}
	}

	// 大多数节点没有返回 promise，500ms 之后重试
	if proposal.Type != Promise {
		time.Sleep(500 * time.Millisecond)
		proposal.Id = time.Now().UnixNano()
		goto retry
	}

	// 发送 accept 消息
	reply.Type = Promise
	proposal.Type = Accept
	if !node.isMajorityNodeLived() {
		log.Println("大多数节点已失效")
		return nil
	}

	majority = node.majority()
	for nodeId, nodeAddr := range nodeMap {
		if !nodeStatus[nodeId] || nodeId == node.Id {
			continue
		}

		proposal.To = nodeId
		response, err = node.CommunicateWithSibling(nodeAddr, proposal)

		if err != nil {
			log.Printf("向节点 %s 发送 accept 消息失败\n", nodeId)
			continue
		}

		log.Printf("response %d from %s\n", response.Type, response.From)

		if response.Type == Accepted {
			majority --
		}

		if majority <= 0 {
			// 大多数节点接受提案值
			proposal.Type = Accepted
			break
		}
	}

	if proposal.Type == Accepted {
		// todo 结果通知 learner
	}

	reply.Type = proposal.Type

	return nil
}
