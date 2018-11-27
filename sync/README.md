# Synchronization

This package synchronizes graphs using libp2p

To get the package to compile please run setup.sh. See setup.sh for more information.

## Algorithmic Interface
The graph synchronization algorithm runs on various machines each with DAGs G<sub>1</sub>, G<sub>2</sub>, ... G<sub>N</sub> (that have a common root node) and results in each machine having the same resulting DAG G' which contains the edges and vertices from each of the input graphs.

## Current Implementation

The current graph synchronization approach works by having all users send their full graphs to each other and then merge the graphs client side. Each node in the graph is the tuple `(parentCIDs, CID, childCIDs)` where the content the CID refers to is the tuple `(parentCIDs, data)`. It's very easy to merge these graphs together since the either nodes that are new between graphs are simply added as new children of their appropriate parents, or if the node exists in both graphs, then any new child edges are added to the existing node.

Look at the test cases in gsync_test.go for examples