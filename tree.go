package main

import (
	"encoding/binary"
	"fmt"
)

/*
	B+Tree Node Format
	- Fixed sixed header
		- Type of node (leaf or internal)
		- Number of keys
	- List of pointers to child nodes for internal nodes
	- List of KV pairs
	- List of offsets to KVs (used to binary search KVs)

	| type | nkeys |  pointers  |   offsets  | key-values | unused |
	| 2B   | 2B    | nkeys * 8B | nkeys * 2B |     ... 	  | 	   |

	Format of each KV pair

	| klen | vlen | key | val |
	| 2B   |  2B  | ... | ... |

*/

const HEADER = 4

// Size of one B Tree node = one disk page = one I/O
const BTREE_PAGE_SIZE = 4096
// Upper limits for key/val size to ensure they fit into one node
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// init() - runs automatically once before main() when package loads
// used to ensure constants and size limits fit in 4KiB page
func init() {
	// Total amount of space to store one maximum-sized key/value pair
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
        panic(fmt.Sprintf(
            "FATAL: B-Tree node size calculation failed. Max size (%d bytes) exceeds page size (%d bytes).",
            node1max,
            BTREE_PAGE_SIZE,
        ))
    }
}

// will just be a chunk of bytes to make it easier to move data from memory to disk
type BNode []byte

type BTREE struct {
	// pointer (nonzero page number)
	root uint64
	// callbacks to manage on-disk pages
	get func(uint64) []byte // reads a page from disk
	new func([]byte) uint64 // allocates and writes a new page (copy-on-write)
	del func(uint64) // deallocates a page
}

const (
	BNODE_NODE = 1 // internal nodes w/o values
	BNODE_LEAF = 2 // leaf nodes w/ values
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func main() {
	test_node := BNode(make([]byte, HEADER))

	test_node.setHeader(BNODE_LEAF, 5)

	fmt.Println(test_node.btype())
	fmt.Println(test_node.nkeys())
}