package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type node struct {
	left  *node
	right *node
	value int
}

type data struct {
	root *node
	hash int
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

func compute_hash(root *node, old_hash int) int {
	if root == nil {
		return old_hash
	}
	hash := compute_hash(root.left, old_hash)
	new_value := root.value + 2
	hash = (hash*new_value + new_value) % 1000
	hash = compute_hash(root.right, hash)
	return hash
}

func printHashWithTrees(m map[int][]*node) {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for _, key := range keys {
		list_m := m[key]
		fmt.Printf("Hash: %d ", key)
		for _, node_pointer := range list_m {
			fmt.Printf("In-Order Tree: ")
			printBst(node_pointer)
		}
		fmt.Printf("\n")
	}
}

func main() {
	// Defining arguments
	// var HashWorkersFlag = flag.Int("hash-workers", 1, "Number of threads")
	// var DataWorkersFlag = flag.Int("data-workers", 1, "Number of threads")
	// var CompWorkersFlag = flag.Int("comp-workers", 1, "Number of threads")
	var inputFlag = flag.String("input", "", "string path to an input file")
	flag.Parse()

	readFile, _ := os.Open(*inputFlag)
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	x := 0
	hashToTreeMap := make(map[int][]*node)

	for fileScanner.Scan() {
		var root node
		// fileScanner.Text returns a single line in this case that would be a BST
		// Using the row number as the index, we can parse this string into a BST
		number_strings := strings.Split(fileScanner.Text(), " ")
		for index, number_string := range number_strings {
			number, _ := strconv.Atoi(number_string)
			if index != 0 {
				insertIntoBst(&root, number)
			} else {
				root.value = number
			}
		}

		hash := compute_hash(&root, 1)
		hashToTreeMap[hash] = append(hashToTreeMap[hash], &root)

		printBst(&root)
		fmt.Printf("%d \n", hash)
		x++
	}

	printHashWithTrees(hashToTreeMap)

	readFile.Close()
	// fmt.Println(*HashWorkersFlag)
}
