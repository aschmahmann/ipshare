package resolvers

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	ipfsApi "github.com/ipfs/go-ipfs-api"

	gsync "github.com/aschmahmann/ipshare/sync"
)

// LockedIPFS Is a synchronized IPFS Shell
type LockedIPFS struct {
	Ipfs *ipfsApi.Shell
	mux  sync.Mutex
}

// Cat Returns data from IPFS at the given path
func (s *LockedIPFS) Cat(path string) (string, error) {
	const maxTries = 5
	s.mux.Lock()
	var err error
	var r io.ReadCloser

	// Try Ipfs.Cat a few times in case the provider is unable to respond
	for numTries := 0; numTries < maxTries; numTries++ {
		r, err = s.Ipfs.Cat(path)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	if err != nil {
		s.mux.Unlock()
		return "", err
	}

	// Return data from Cat as a string
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		r.Close()
		return "", err
	}
	elem := buf.String()
	r.Close()
	s.mux.Unlock()
	return elem, err
}

// Add data to IPFS
func (s *LockedIPFS) Add(r io.Reader, options ...ipfsApi.AddOpts) (string, error) {
	s.mux.Lock()
	ret, err := s.Ipfs.Add(r, options...)
	log.Printf("Added: %v", ret)
	s.mux.Unlock()
	return ret, err
}

// StringSetAdd Is the operation for adding a string to the set
type StringSetAdd struct {
	elem string
}

// NamedMultiWriterStringSetCRDT Is a string set CRDT that is updated by gossiping about the named structure (e.g. the structure is referenced by UUID or Hash(PublicKey))
type NamedMultiWriterStringSetCRDT struct {
	GraphManager gsync.GossipMultiWriterIPNS
	Set          map[string]struct{}
	Ipfs         *LockedIPFS
	mux          sync.Mutex
	ipfsMux      sync.Mutex
}

// AddElem adds an element to the set
func (mwSet *NamedMultiWriterStringSetCRDT) AddElem(elem string) error {
	mwSet.mux.Lock()
	mwSet.Set[elem] = struct{}{}
	mwSet.mux.Unlock()
	parents := mwSet.GraphManager.GetLatestVersionHistories()
	parentCIDs := make([]*cid.Cid, len(parents))
	for i, p := range parents {
		pCid := p.GetNodeID()
		parentCIDs[i] = &pCid
	}

	mwSet.ipfsMux.Lock()
	hashStr, err := mwSet.Ipfs.Add(bytes.NewBufferString(elem))
	mwSet.ipfsMux.Unlock()

	if err != nil {
		return err
	}

	id, err := cid.Decode(hashStr)
	if err != nil {
		return err
	}

	mwSet.GraphManager.AddNewVersion(&id, parentCIDs...)
	return nil
}

// GetSet returns the map containing the set elements. Do not modify the contents.
func (mwSet *NamedMultiWriterStringSetCRDT) GetSet() map[string]struct{} {
	root := mwSet.GraphManager.GetRoot()

	// Apply all of the CRDT operations
	// TODO: This can be optimized via caching
	var recSetAdd func(n gsync.DagNode)
	recSetAdd = func(n gsync.DagNode) {
		cidStr := n.GetValue().String()
		mwSet.ipfsMux.Lock()
		elem, err := mwSet.Ipfs.Cat(cidStr)
		mwSet.ipfsMux.Unlock()
		if err != nil {
			log.Fatal(err)
		}

		mwSet.mux.Lock()
		if _, ok := mwSet.Set[elem]; !ok {
			mwSet.Set[elem] = struct{}{}
		}
		mwSet.mux.Unlock()

		for _, child := range n.GetChildren() {
			recSetAdd(child)
		}
	}

	//TODO: Is root.GetValue better than NodeID?
	if root.GetValue() == mwSet.GraphManager.IPNSKey {
		for _, c := range root.GetChildren() {
			recSetAdd(c)
		}
	} else {
		recSetAdd(root)
	}
	return mwSet.Set
}
