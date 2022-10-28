package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var NumberOfBst = 0
var HashTime time.Duration
var HashWorkers = 1
var CompWorkers = 1
var HashMutex sync.RWMutex
var HashWaitGroup sync.WaitGroup
var ParallelWaitGroup sync.WaitGroup
var ComparisonWaitGroup sync.WaitGroup
var ComparisonGroupIdMutex sync.RWMutex
var ComparisonGroupMutex sync.RWMutex
var ComparisonUniqueTraversal sync.RWMutex
var BufferHasData sync.Cond
var BufferHasSpace sync.Cond
var BufferMutex sync.RWMutex
var PoisonNode node
var ParallelWaitGroupEnd sync.WaitGroup
var Items int

type grouping struct {
	groupId int
	bstIds  []int
}

type node struct {
	left  *node
	right *node
	value int
	index int
}

type hashChannelData struct {
	node *node
	hash int
}

type parallelChannelData struct {
	root    *node
	inorder string
	bst_id  int
}

type bufferQ struct {
	Elements []*node
}

func insertIntoBst(root *node, val int) *node {
	if root == nil {
		var new_node node
		new_node.value = val
		return &new_node
	}

	if val < root.value {
		root.left = insertIntoBst(root.left, val)
	} else {
		root.right = insertIntoBst(root.right, val)
	}

	return root
}

func computeHash(root *node, old_hash int) int {
	if root == nil {
		return old_hash
	}
	hash := computeHash(root.left, old_hash)
	new_value := root.value + 2
	hash = (hash*new_value + new_value) % 1000
	hash = computeHash(root.right, hash)
	return hash
}

func printHashGroups(m map[int][]*node) {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for _, key := range keys {
		nodes_pointer_list := m[key]
		if len(nodes_pointer_list) > 1 {
			fmt.Printf("%d: ", key)
			for _, node_pointer := range nodes_pointer_list {
				fmt.Printf("%d ", node_pointer.index)
			}
			fmt.Printf("\n")
		}
	}
}

func createInOrderHashString(root *node, str string) string {
	if root.left != nil {
		str = createInOrderHashString(root.left, str)
	}

	if root != nil {
		str += strconv.Itoa(root.value) + " "
	}

	if root.right != nil {
		str = createInOrderHashString(root.right, str)
	}

	return str
}

func sequentialCompareTreeWithIdenticalHashes(m map[int][]*node) []grouping {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	unique_group_id := 0
	var groups []grouping
	for _, key := range keys {
		unique_traversals := make(map[string]int)
		nodes_pointer_list := m[key]
		if len(nodes_pointer_list) > 1 {
			for _, node_pointer := range nodes_pointer_list {
				inorder_traversal := createInOrderHashString(node_pointer, "")
				if groupId, ok := unique_traversals[inorder_traversal]; ok {
					//Traversal already accounted for
					groups[groupId].bstIds = append(groups[groupId].bstIds, node_pointer.index)
				} else {
					//traversal not accounted for, add it in
					unique_traversals[inorder_traversal] = unique_group_id
					var new_group grouping
					new_group.groupId = unique_group_id
					new_group.bstIds = append(new_group.bstIds, node_pointer.index)
					groups = append(groups, new_group)
					unique_group_id++
				}
			}
		}
	}

	return groups
}

func createBuffer(buffer *bufferQ) {
	Items = 0
}

func placeInBuffer(buffer *bufferQ, root *node) {
	BufferMutex.Lock()
	for Items == CompWorkers {
		BufferHasSpace.Wait()
	}

	buffer.Elements = append(buffer.Elements, root)
	Items++

	if Items > 0 {
		BufferHasData.Signal()
	}

	BufferMutex.Unlock()
}

func removeFromBuffer(buffer *bufferQ) (root *node) {

	BufferMutex.Lock()
	for Items == 0 {
		BufferHasData.Wait()
	}

	Items--
	var ret *node
	ret, buffer.Elements = buffer.Elements[0], buffer.Elements[1:]

	if Items < CompWorkers {
		BufferHasSpace.Signal()
	}

	BufferMutex.Unlock()

	return ret
}

func createInOrderHashStringChannel(return_channel chan parallelChannelData, buffer *bufferQ, offset int) {

	for {
		root := removeFromBuffer(buffer)
		if root == nil {
			// A channel is a blocking call.
			return_channel <- parallelChannelData{nil, "", 0}
			return
		}
		ret := createInOrderHashString(root, "")
		return_channel <- parallelChannelData{root, ret, root.index}
	}
}

func feedNodeDataThroughChannel(m map[int][]*node, buffer *bufferQ) {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for _, key := range keys {
		nodes_pointer_list := m[key]
		if len(nodes_pointer_list) > 1 {
			for _, node_pointer := range nodes_pointer_list {
				placeInBuffer(buffer, node_pointer)
			}
		}
	}

	for i := 0; i < CompWorkers; i++ {
		placeInBuffer(buffer, nil)
	}
}

func parallelCompareTreeWithIdenticalHashes(m map[int][]*node) []grouping {
	unique_group_id := 0
	return_channel := make(chan parallelChannelData, CompWorkers)

	var buffer bufferQ
	createBuffer(&buffer)
	BufferHasSpace = *sync.NewCond(&BufferMutex)
	BufferHasData = *sync.NewCond(&BufferMutex)
	go feedNodeDataThroughChannel(m, &buffer)

	for i := 0; i < CompWorkers; i++ {
		go createInOrderHashStringChannel(return_channel, &buffer, i)
	}

	var groups []grouping
	//Traversal already accounted for
	//traversal not accounted for, add it in
	ParallelWaitGroupEnd.Add(1)
	go newFunction(return_channel, &groups, unique_group_id)
	ParallelWaitGroupEnd.Wait()
	return groups
}

func newFunction(return_channel chan parallelChannelData, groups *[]grouping, unique_group_id int) {
	unique_traversals := make(map[string]int)

	returned := 0
	for returned != CompWorkers {
		channel_data := <-return_channel

		if channel_data.root == nil {
			returned++
			continue
		}

		groupId, ok := unique_traversals[channel_data.inorder]
		if ok {
			(*groups)[groupId].bstIds = append((*groups)[groupId].bstIds, channel_data.bst_id)
		} else {

			unique_traversals[channel_data.inorder] = unique_group_id
			var new_group grouping
			new_group.groupId = unique_group_id
			new_group.bstIds = append(new_group.bstIds, channel_data.bst_id)
			(*groups) = append((*groups), new_group)
			unique_group_id++
		}
	}
	ParallelWaitGroupEnd.Done()

}

func printTreeComparisons(groups []grouping) {
	found := 0
	for _, group := range groups {
		if len(group.bstIds) > 1 {

			fmt.Printf("group %d: ", found)
			found++
			for _, tree_id := range group.bstIds {
				fmt.Printf("%d ", tree_id)
			}
			fmt.Println()
		}
	}
}

func main() {
	// Defining arguments
	var HashWorkersFlag = flag.Int("hash-workers", 1, "Number of threads")
	var DataWorkersFlag = flag.Int("data-workers", 0, "Number of threads")
	var CompWorkersFlag = flag.Int("comp-workers", 0, "Number of threads")
	var use_channels = flag.Bool("use-channels", false, "internal flag control")
	var input_flag = flag.String("input", "", "string path to an input file")
	flag.Parse()

	HashWorkers = *HashWorkersFlag
	CompWorkers = *CompWorkersFlag
	_ = *DataWorkersFlag

	//Reading Bsts
	read_file, _ := os.Open(*input_flag)
	file_scanner := bufio.NewScanner(read_file)
	file_scanner.Split(bufio.ScanLines)

	var bsts []node
	for file_scanner.Scan() {
		var root node
		// fileScanner.Text returns a single line in this case that would be a BST
		// Using the row number as the index, we can parse this string into a BST
		number_strings := strings.Split(file_scanner.Text(), " ")
		for index, number_string := range number_strings {
			number, _ := strconv.Atoi(number_string)
			if index != 0 {
				insertIntoBst(&root, number)
			} else {
				root.value = number
				root.index = NumberOfBst
			}
		}
		bsts = append(bsts, root)
		NumberOfBst++
	}
	read_file.Close()

	var hash_data []hashChannelData
	if HashWorkers == 1 {
		// fmt.Println("Hashing Sequentially")
		hash_start := time.Now()
		hash_data = sequentialHashing(bsts)
		HashTime += time.Since(hash_start)
	} else {
		// fmt.Println("Hashing in Parallel")
		if *use_channels {
			hash_start := time.Now()
			hash_data = goroutineHashingChannels(bsts)
			HashTime += time.Since(hash_start)
		} else {
			hash_start := time.Now()
			hash_data = goroutineHashingMutex(bsts)
			HashTime += time.Since(hash_start)
		}
	}
	fmt.Println("hashTime:", HashTime.Seconds())

	var hash_to_tree_map map[int][]*node

	if *DataWorkersFlag != 0 || CompWorkers != 0 {
		group_time := time.Now()
		hash_to_tree_map = findHashGroups(hash_data)
		fmt.Println("hashGroupTime:", time.Since(group_time).Seconds()+HashTime.Seconds())
		printHashGroups(hash_to_tree_map)
	}

	if CompWorkers != 0 {

		var comparison_grouping []grouping
		start := time.Now()
		if CompWorkers == 1 {
			// fmt.Println("Comparison Sequentially")
			comparison_grouping = sequentialCompareTreeWithIdenticalHashes(hash_to_tree_map)
		} else {
			// fmt.Println("Comparison N Parallel")
			comparison_grouping = parallelCompareTreeWithIdenticalHashes(hash_to_tree_map)
		}
		fmt.Println("compareTreeTime:", time.Since(start).Seconds())
		printTreeComparisons(comparison_grouping)
	}
}

func sequentialHashing(bsts []node) []hashChannelData {
	var hash_data []hashChannelData
	for i := 0; i < NumberOfBst; i++ {
		hash := computeHash(&bsts[i], 1)
		hash_channel := hashChannelData{&bsts[i], hash}
		hash_data = append(hash_data, hash_channel)
	}
	return hash_data
}

func findHashGroups(hash_data []hashChannelData) map[int][]*node {
	hash_to_tree_map := make(map[int][]*node)
	for _, data := range hash_data {
		hash_to_tree_map[data.hash] = append(hash_to_tree_map[data.hash], data.node)
	}
	return hash_to_tree_map
}

func goroutineHashingChannels(bsts []node) []hashChannelData {
	hashChannel := make(chan hashChannelData)
	var hash_channel_data_list []hashChannelData

	for i := 0; i < HashWorkers; i++ {
		go computeHashToChannel(i, bsts, hashChannel)
	}

	for i := 0; i < NumberOfBst; i++ {
		hash_channel_data := <-hashChannel
		hash_channel_data_list = append(hash_channel_data_list, hash_channel_data)
	}

	return hash_channel_data_list
}

func computeHashToChannel(offset int, bsts []node, result chan hashChannelData) {
	var hash_data hashChannelData
	for i := offset; i < NumberOfBst; i += HashWorkers {
		hash_data.hash = computeHash(&bsts[i], 1)
		hash_data.node = &bsts[i]
		result <- hash_data
	}
}

func goroutineHashingMutex(bsts []node) []hashChannelData {
	var hash_data_list []hashChannelData

	HashWaitGroup.Add(HashWorkers)
	for i := 0; i < HashWorkers; i++ {
		go computeHashUsingMutex(i, bsts, &hash_data_list)
	}

	HashWaitGroup.Wait()
	return hash_data_list
}

func computeHashUsingMutex(offset int, bsts []node, hash_data_list *[]hashChannelData) {
	var hash_data hashChannelData
	for i := offset; i < NumberOfBst; i += HashWorkers {
		hash_data.hash = computeHash(&bsts[i], 1)
		hash_data.node = &bsts[i]
		HashMutex.Lock()
		*hash_data_list = append(*hash_data_list, hash_data)
		HashMutex.Unlock()
	}
	HashWaitGroup.Done()
}
