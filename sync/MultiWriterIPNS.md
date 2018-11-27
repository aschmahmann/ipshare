# Multi-Writer/Collaborative Data Strucutures

## Proposal

Currently, the systems for persisting data in the IP* universe are IPFS for static content and IPNS for dynamic content. However, IPNS has single writer semantics and will not retain changes from multiple editors. Therefore, we propose creating a new system for supporting the case of multiple editors collaborating on a shared data structure.

While the IPNS + IPFS combination has dictionary semantics equivalent to:

```go
type IPFS interface {
    GetContent(Cid) Content
    AdvertiseContent(Content) Cid
}
type IPNS interface {
   Constructor(IPNS Key, IPFS instance)

   SetContentID(Cid)
   GetContentID() Cid

   SetContent(Content) // Equivalent to SetContentID(ipfs.AdvertiseContent(Content))
   GetContent() Content // Equivalent to SetContentID(ipns.GetContent())
}
```

Instead a Multi-Writer IPNS would have semantics such as:

```go
type DagNode interface {
    GetParents() []DagNode
    GetChildren() []DagNode
    GetValue() Cid // Content format for the Cid is {ParentCids[], DataCid}
}

type MultiWriterIPNS interface {
   Constructor(IPNS Key, IPFS instance)

   AddNewVersion(prevCid, newCid)
   GetLatestVersionHistories() DagNode[] // Each DagNode returned represents one possible version of the data and the history leading up to it
}

type VersionedContentResolver interface {
   Constructor(MultiWriterIPNS)

   GetLatestVersionHistories() (Content[], DagNode[])
   // ContentOperation is an operation such that operation.apply(prevContent) returns newContent
   AddOperation(ContentOperation, prevDagNode) DagNode
}
```

Supporting this interface allows multiple editors to collaborate on a data structure that has a single identifier/name while not being very restrictive on the data structure itself. Note: this interface could also be modified to support an event-driven style (e.g. receiving updates and pushing out changes) as described in this issue: <https://github.com/ipfs/dynamic-data-and-capabilities/issues/50#issuecomment-441698511>

Some example data/operation structures to consider supporting include:

* Preservation of changes
  * e.g. Git requires tracking the changes to a repo
* Overwriting semantics
  * e.g. Like IPNS, but all branches/conflicting IPFS CIDs are returned
* Operation Transformation (OT): <https://en.wikipedia.org/wiki/Operational_transformation>)
  * OT is a form of automatic merging of changes. In order for OT to merge two branches it is generally required that the OT algorithm have access to the changes in each of the two branches.
* Commutative Replicated Data Types (CRDTs): <https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type>
  * CRDTs also perform automatic merging of changes. However, many do not require access to the underlying changes in the branches being merged in order to correctly perform the merge

## First Implementation

* [x] Basic gossip system to allow for MW-IPNS communication
* [x] Basic graph synchronization protocol (send all changes)
* [ ] Expand gossip system to account for multiple simultaneous MW-IPNS synchronizations
* [ ] Implement the 2 of the versioned content resolvers above
* [ ] Create basic MW-IPNS pinner
* [ ] Harden gossip system and get reviews from some of the libp2p team

## Future Implementations

* Allow for substitution of various different gossip-routing solutions.
  * If appropriate maybe abstract the pubsub routing options for use here
* Work on (looping in the IPLD team) more efficient synchronization algorithms
  * Look into developing a codec to allow users to easily upgrade their data/operations