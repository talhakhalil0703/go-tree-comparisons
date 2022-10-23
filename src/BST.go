package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var numberOfBst = 0

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

func compareTreeWithIdenticalHashes(m map[int][]*node) []grouping {
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

func printTreeComparisons(groups []grouping) {
	for group_id, group := range groups {
		if len(group.bstIds) > 1 {

			fmt.Printf("group %d: ", group_id)
			for _, tree_id := range group.bstIds {
				fmt.Printf("%d ", tree_id)
			}
			fmt.Println()
		}
	}
}

func main() {
	// Defining arguments
	// var HashWorkersFlag = flag.Int("hash-workers", 1, "Number of threads")
	// var DataWorkersFlag = flag.Int("data-workers", 1, "Number of threads")
	// var CompWorkersFlag = flag.Int("comp-workers", 1, "Number of threads")
	var input_flag = flag.String("input", "", "string path to an input file")
	flag.Parse()

	read_file, _ := os.Open(*input_flag)
	file_scanner := bufio.NewScanner(read_file)
	file_scanner.Split(bufio.ScanLines)

	hash_to_tree_map := make(map[int][]*node)
	var hash_time time.Duration
	var hash_group_time time.Duration
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
				root.index = numberOfBst
			}
		}

		hash_start := time.Now()
		hash := computeHash(&root, 1)
		hash_time += time.Since(hash_start)
		hash_to_tree_map[hash] = append(hash_to_tree_map[hash], &root)
		hash_group_time += time.Since(hash_start)

		// str := createInOrderHashString(&root, " ")
		// fmt.Print(str)
		// fmt.Printf("%d \n", hash)
		numberOfBst++
	}

	// printHashWithTrees(hash_to_tree_map)
	fmt.Println("hashTime:", hash_time)
	fmt.Println("hashGroupTime:", hash_group_time)
	printHashGroups(hash_to_tree_map)
	start := time.Now()
	comparison_grouping := compareTreeWithIdenticalHashes(hash_to_tree_map)
	fmt.Println("compareTreeTime:", time.Since(start))
	printTreeComparisons(comparison_grouping)
	read_file.Close()
	// fmt.Println(*HashWorkersFlag)
}
