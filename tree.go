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

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("Index out of bounds")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("Index out of bounds")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:pos+9], val)
}

// offset list
// finds where the offset data is located in the node
func offsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.nkeys() {
		panic("Index out of bounds")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// retrieves the offset data
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) 

// key-values
// returns position of the nth KV pair relative to whole node
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("Index out of bounds")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("Index out of bounds")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func main() {
	test_node := BNode(make([]byte, HEADER))

	test_node.setHeader(BNODE_LEAF, 5)

	fmt.Println(test_node.btype())
	fmt.Println(test_node.nkeys())
}