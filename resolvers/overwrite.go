package resolvers

import (
	cid "github.com/ipfs/go-cid"

	gsync "github.com/aschmahmann/ipshare/sync"
)

// TODO: Not clear whether an overwriter/IPNS clone should need a wrapper around the CID or whether it's ok to be implied
// type IPNSAdd struct {
// 	nextEntry cid.Cid
// }

// OverwriteWithConflicts Allows for gossip based updating of a value where simultaneous updates are preserved as conflicts
type OverwriteWithConflicts struct {
	GraphManager gsync.GossipMultiWriterIPNS
}

// UpdateVersion Overwrites the current value with a new one
func (overwriter *OverwriteWithConflicts) UpdateVersion(newCid cid.Cid) {
	// Add new version that is based off of all versions we've seen so far
	parents := overwriter.GraphManager.GetLatestVersionHistories()
	parentCIDs := make([]*cid.Cid, len(parents))
	for i, p := range parents {
		pCid := p.GetNodeID()
		parentCIDs[i] = &pCid
	}

	overwriter.GraphManager.AddNewVersion(&newCid, parentCIDs...)
}

// GetLatestVersions Returns all of the conflicting values
func (overwriter *OverwriteWithConflicts) GetLatestVersions() []cid.Cid {
	branchLeaves := overwriter.GraphManager.GetLatestVersionHistories()
	cids := make([]cid.Cid, len(branchLeaves))
	for i, leaf := range branchLeaves {
		cids[i] = leaf.GetValue()
	}
	return cids
}
