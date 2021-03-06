package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	host "github.com/libp2p/go-libp2p-host"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

// IPFS bootstrapping parameters
var (
	ipfsPeers = convertPeers([]string{
		"/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
		"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
		"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
		"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
		"/ip6/2604:a880:1:20::203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
		"/ip6/2400:6180:0:d0::151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
		"/ip6/2604:a880:800:10::4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
		"/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
	})

	localPeerEndpoint = "http://localhost:5001/api/v0/id"
)

// IDOutput Borrowed from ipfs code to parse the results of the command `ipfs id`
type IDOutput struct {
	ID              string
	PublicKey       string
	Addresses       []string
	AgentVersion    string
	ProtocolVersion string
}

// quick and dirty function to get the local ipfs daemons address for bootstrapping
func getLocalPeerInfo() []pstore.PeerInfo {
	resp, err := http.Get(localPeerEndpoint)
	if err != nil {
		log.Fatalln(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	var js IDOutput
	err = json.Unmarshal(body, &js)
	if err != nil {
		log.Fatalln(err)
	}

	loopbackAddrs := make([]string, 0, 2)
	for _, addr := range js.Addresses {
		// For some reason, possibly NAT traversal, we need to grab the loopback ip address
		if addr[0:8] == "/ip4/127" || addr[0:8] == "/ip6/::1" {
			loopbackAddrs = append(loopbackAddrs, addr)
		}
	}
	return convertPeers(loopbackAddrs)
	// log.Fatalln(err)
	// return make([]pstore.PeerInfo, 1) // not reachable, but keeps the compiler happy
}

func convertPeers(peers []string) []pstore.PeerInfo {
	pinfos := make([]pstore.PeerInfo, len(peers))
	for i, peer := range peers {
		maddr := ma.StringCast(peer)
		p, err := pstore.InfoFromP2pAddr(maddr)
		if err != nil {
			log.Fatalln(err)
		}
		pinfos[i] = *p
	}
	return pinfos
}

// This code is borrowed from the go-ipfs bootstrap process
func bootstrapConnect(ctx context.Context, ph host.Host, peers []pstore.PeerInfo) error {
	if len(peers) < 1 {
		return errors.New("not enough bootstrap peers")
	}

	const retries = 10
	errs := make(chan error, len(peers)*retries)
	for i := 0; i < retries; i++ {
		if len(errs) != i*len(peers) && i != 0 {
			continue
		} else if i != 0 {
			time.Sleep(1000 * time.Millisecond)
		}
		var wg sync.WaitGroup
		for _, p := range peers {

			// performed asynchronously because when performed synchronously, if
			// one `Connect` call hangs, subsequent calls are more likely to
			// fail/abort due to an expiring context.
			// Also, performed asynchronously for dial speed.

			wg.Add(1)
			go func(p pstore.PeerInfo) {
				defer wg.Done()
				defer log.Println(ctx, "bootstrapDial", ph.ID(), p.ID)
				log.Printf("%s bootstrapping to %s", ph.ID(), p.ID)

				ph.Peerstore().AddAddrs(p.ID, p.Addrs, pstore.PermanentAddrTTL)
				if err := ph.Connect(ctx, p); err != nil {
					log.Println(ctx, "bootstrapDialFailed", p.ID)
					log.Printf("failed to bootstrap with %v: %v | %s", p.ID, p.Addrs[0], err)
					errs <- err
					return
				}
				log.Println(ctx, "bootstrapDialSuccess", p.ID)
				log.Printf("bootstrapped with %v", p.ID)
			}(p)
		}
		wg.Wait()
	}

	// our failure condition is when no connection attempt succeeded.
	// So drain the errs channel, counting the results.
	close(errs)
	count := 0
	allErr := make([]string, 0)
	var err error
	for err = range errs {
		if err != nil {
			internalErr := err
			allErr = append(allErr, fmt.Sprint(internalErr))
			count++
		}
	}
	if count == len(peers)*retries {
		return fmt.Errorf("failed to bootstrap. %s", allErr)
	}
	return nil
}
