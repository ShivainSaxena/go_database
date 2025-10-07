package main

import (
	"testing",
	"github.com/stretchr/testify/assert"
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
	assert(node1max <= BTREE_PAGE_SIZE)
}