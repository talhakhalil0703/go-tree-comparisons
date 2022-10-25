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
var HashGroupTime time.Duration
var HashWorkers = 1
var HashMutex sync.RWMutex
var HashWaitGroup sync.WaitGroup
var ComparisonWaitGroup sync.WaitGroup
var ComparisonGroupIdMutex sync.RWMutex
var ComparisonGroupMutex sync.RWMutex
var ComparisonUniqueTraversal sync.RWMutex

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
	inorder string
	bst_id  int
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

func printBst(root *node) {
	if root.left != nil {
		printBst(root.left)
	}

	if root != nil {
		fmt.Printf("%d ", root.value)
	}

	if root.right != nil {
		printBst(root.right)
	}
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

func printHashWithTrees(m map[int][]*node) {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for _, key := range keys {
		nodes_pointer_list := m[key]
		fmt.Printf("Hash: %d ", key)
		for _, node_pointer := range nodes_pointer_list {
			fmt.Printf("In-Order Tree: ")
			printBst(node_pointer)
		}
		fmt.Printf("\n")
	}
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

func createInOrderHashStringChannel(root *node, result chan parallelChannelData) {
	ret := createInOrderHashString(root, "")
	result <- parallelChannelData{ret, root.index}
}

func parallelCompareTreeWithIdenticalHashes(m map[int][]*node) []grouping {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	unique_group_id := 0
	parallel_channel := make(chan parallelChannelData)
	for _, key := range keys {
		nodes_pointer_list := m[key]
		if len(nodes_pointer_list) > 1 {
			for _, node_pointer := range nodes_pointer_list {
				go createInOrderHashStringChannel(node_pointer, parallel_channel)
			}
		}
	}

	var groups []grouping
	unique_traversals := make(map[string]int)
	var time_for_map time.Duration
	for _, key := range keys {
		nodes_pointer_list := m[key]
		if len(nodes_pointer_list) > 1 {
			for i := 0; i < len(nodes_pointer_list); i++ {
				channel_data := <-parallel_channel
				time_now := time.Now()
				groupId, ok := unique_traversals[channel_data.inorder]
				time_for_map += time.Since(time_now)
				if ok {
					//Traversal already accounted for
					groups[groupId].bstIds = append(groups[groupId].bstIds, channel_data.bst_id)
				} else {
					//traversal not accounted for, add it in
					unique_traversals[channel_data.inorder] = unique_group_id
					var new_group grouping
					new_group.groupId = unique_group_id
					new_group.bstIds = append(new_group.bstIds, channel_data.bst_id)
					groups = append(groups, new_group)
					unique_group_id++
				}
			}
		}
	}

	// fmt.Println("TimeMapCheck", time_for_map)
	return groups
}

func sequentialCompareTreeWithIdenticalHashesAdj(m map[int][]*node) []grouping {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	unique_group_id := 0
	var groups []grouping
	var adj_matrix [][]bool

	for i := 0; i < NumberOfBst; i++ {
		var inner []bool
		for j := 0; j < NumberOfBst; j++ {
			inner = append(inner, false)
		}
		adj_matrix = append(adj_matrix, inner)
	}

	for _, key := range keys {
		nodes_pointer_list := m[key]
		for i := 0; i < len(nodes_pointer_list); i++ {
			for j := i; j < len(nodes_pointer_list); j++ {
				result := true
				if createInOrderHashString(nodes_pointer_list[i], "") != createInOrderHashString(nodes_pointer_list[j], "") {
					result = false
				}
				adj_matrix[nodes_pointer_list[i].index][nodes_pointer_list[j].index] = result
				adj_matrix[nodes_pointer_list[j].index][nodes_pointer_list[i].index] = result
			}
		}
	}
	consumed := make(map[int]bool)

	for _, key := range keys {
		nodes_pointer_list := m[key]
		for j := 0; j < len(nodes_pointer_list); j++ {
			if consumed[nodes_pointer_list[j].index] {
				continue
			}

			var group grouping
			group.groupId = unique_group_id
			unique_group_id++
			group.bstIds = append(group.bstIds, nodes_pointer_list[j].index)
			consumed[nodes_pointer_list[j].index] = true

			for k := j; k < len(nodes_pointer_list); k++ {
				if consumed[nodes_pointer_list[k].index] {
					continue
				}

				if adj_matrix[nodes_pointer_list[j].index][nodes_pointer_list[k].index] {
					consumed[nodes_pointer_list[k].index] = true
					group.bstIds = append(group.bstIds, nodes_pointer_list[k].index)
				}
			}

			groups = append(groups, group)
		}
	}

	return groups
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
	// var DataWorkersFlag = flag.Int("data-workers", 1, "Number of threads")
	var CompWorkersFlag = flag.Int("comp-workers", 1, "Number of threads")
	var use_mutexes = flag.Bool("use-mutex", false, "internal flag control")
	var input_flag = flag.String("input", "", "string path to an input file")
	flag.Parse()

	HashWorkers = *HashWorkersFlag
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

	var hash_to_tree_map map[int][]*node
	if HashWorkers == 1 {
		// fmt.Println("Hashing Sequentially")
		hash_to_tree_map = sequentialHashing(bsts)
	} else {
		// fmt.Println("Hashing in Parallel")
		if *use_mutexes {
			hash_to_tree_map = goroutineHashingMutex(bsts)
		} else {
			hash_to_tree_map = goroutineHashingChannels(bsts)
		}
	}

	fmt.Println("hashTime:", HashTime.Seconds())
	fmt.Println("hashGroupTime:", HashGroupTime.Seconds())
	printHashGroups(hash_to_tree_map)
	var comparison_grouping []grouping
	start := time.Now()

	if *CompWorkersFlag == 1 {
		// fmt.Println("Comparison Sequentially")
		comparison_grouping = sequentialCompareTreeWithIdenticalHashes(hash_to_tree_map)
	} else {
		// fmt.Println("Comparison N Parallel")
		comparison_grouping = parallelCompareTreeWithIdenticalHashes(hash_to_tree_map)
	}
	fmt.Println("compareTreeTime:", time.Since(start).Seconds())
	printTreeComparisons(comparison_grouping)
}

func sequentialHashing(bsts []node) map[int][]*node {
	hash_to_tree_map := make(map[int][]*node)
	for i := 0; i < NumberOfBst; i++ {
		hash_start := time.Now()
		hash := computeHash(&bsts[i], 1)
		HashTime += time.Since(hash_start)
		hash_to_tree_map[hash] = append(hash_to_tree_map[hash], &bsts[i])
		HashGroupTime += time.Since(hash_start)
	}
	return hash_to_tree_map
}

func goroutineHashingChannels(bsts []node) map[int][]*node {
	hash_to_tree_map := make(map[int][]*node)
	hashChannel := make(chan hashChannelData)
	var hash_channel_data_list []hashChannelData

	hash_start := time.Now()
	for i := 0; i < HashWorkers; i++ {
		go computeHashToChannel(i, bsts, hashChannel)
	}

	for i := 0; i < NumberOfBst; i++ {
		hash_channel_data := <-hashChannel
		hash_channel_data_list = append(hash_channel_data_list, hash_channel_data)
	}
	HashTime = time.Since(hash_start)

	for i := 0; i < NumberOfBst; i++ {
		hash_channel_data := hash_channel_data_list[i]
		hash_to_tree_map[hash_channel_data.hash] = append(hash_to_tree_map[hash_channel_data.hash], hash_channel_data.node)
	}
	HashGroupTime = time.Since(hash_start)

	return hash_to_tree_map
}

func computeHashToChannel(offset int, bsts []node, result chan hashChannelData) {
	var hash_data hashChannelData
	for i := offset; i < NumberOfBst; i += HashWorkers {
		hash_data.hash = computeHash(&bsts[i], 1)
		hash_data.node = &bsts[i]
		result <- hash_data
	}
}

func goroutineHashingMutex(bsts []node) map[int][]*node {
	hash_to_tree_map := make(map[int][]*node)
	var hash_data_list []hashChannelData

	HashWaitGroup.Add(HashWorkers)
	hash_start := time.Now()
	for i := 0; i < HashWorkers; i++ {
		go computeHashUsingMutex(i, bsts, &hash_data_list)
	}

	HashWaitGroup.Wait()
	HashTime = time.Since(hash_start)

	for i := 0; i < NumberOfBst; i++ {
		hash_channel_data := hash_data_list[i]
		hash_to_tree_map[hash_channel_data.hash] = append(hash_to_tree_map[hash_channel_data.hash], hash_channel_data.node)
	}
	HashGroupTime = time.Since(hash_start)

	return hash_to_tree_map
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
