# Synchronization

This package synchronizes graphs using libp2p

## Motivation and Use Cases

See more information about multi-writer/collaborative data structures and the need for a [multi-writer IPNS](./MultiWriterIPNS.md)

See also the [proposal](https://github.com/ipld/replication/pull/3) for supporting the above use case as part of the set of IPLD replication options

## Algorithmic Interface

The graph synchronization algorithm runs on various machines each with DAGs G<sub>1</sub>, G<sub>2</sub>, ... G<sub>N</sub> (that have a common root node) and results in each machine having the same resulting DAG G' which contains the edges and vertices from each of the input graphs.

## Current Implementation

The current graph synchronization approach works by having all users send their full graphs to each other and then merge the graphs on their own. Each node in the graph is the tuple `(parentCIDs, CID, childCIDs)` where the content the CID refers to is the tuple `(parentCIDs, dataCID)`. It's very easy to merge these graphs together since the either nodes that are new between graphs are simply added as new children of their appropriate parents, or if the node exists in both graphs, then any new child edges are added to the existing node.

Look at the test cases in [gsync_test.go](./gsync_test.go) for examples

There is also a basic pinning service for these graphs available at [pinner.go](./pinner.go) and example usage at [pinner_test.go](./pinner_test.go)