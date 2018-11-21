# Synchronization

This package synchronizes graphs using libp2p

To get the package to compile please run setup.sh. See setup.sh for more information.

The graph synchronization algorithm runs on various machines each with DAGs G<sub>1</sub>, G<sub>2</sub>, ... G<sub>N</sub> (that have a common root node) and results in each machine having the same resulting DAG G' which contains the edges and vertices from each of the input graphs.

Look at the test cases in gsync_test.go for examples