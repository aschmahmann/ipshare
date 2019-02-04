# IPShare - Sharing Utilities for IPFS

IPShare is an experimental set of packages that utilizes [libp2p](https://github.com/libp2p/libp2p) to assist users in sharing and collaborating on documents in real-time. Eventually it should make its way into the [peer-base](https://github.com/peer-base/peer-base) library.

IPShare currently has two components:

* [Synchronization](./sync/README.md)
  * Utilizes libp2p to synchronize a shared append-only DAG
* [Content Resolvers](./resolvers/README.md)
  * Builds shared data structures on top of the graph synchronization primitive