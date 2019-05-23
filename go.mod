module github.com/aschmahmann/ipshare

require (
	github.com/gogo/protobuf v1.2.1

	github.com/ipfs/go-cid v0.0.1
	github.com/ipfs/go-datastore v0.0.5
	github.com/ipfs/go-ipfs v0.4.20
	github.com/ipfs/go-ipfs-api v0.0.1
	github.com/ipfs/go-ipld-cbor v0.0.1
	github.com/ipfs/go-ipns v0.0.1
	github.com/ipfs/go-path v0.0.3

	github.com/libp2p/go-libp2p v0.0.20
	github.com/libp2p/go-libp2p-blankhost v0.0.1
	github.com/libp2p/go-libp2p-crypto v0.0.2
	github.com/libp2p/go-libp2p-host v0.0.3
	github.com/libp2p/go-libp2p-kad-dht v0.0.10
	github.com/libp2p/go-libp2p-net v0.0.2
	github.com/libp2p/go-libp2p-peer v0.1.1
	github.com/libp2p/go-libp2p-peerstore v0.0.6
	github.com/libp2p/go-libp2p-protocol v0.0.1
	github.com/libp2p/go-libp2p-pubsub v0.0.1
	github.com/libp2p/go-libp2p-pubsub-router v0.0.3
	github.com/libp2p/go-libp2p-record v0.0.1
	github.com/libp2p/go-libp2p-swarm v0.0.6

	github.com/multiformats/go-multiaddr v0.0.4
	github.com/multiformats/go-multihash v0.0.5
	github.com/pkg/errors v0.8.1
)

replace github.com/libp2p/go-libp2p-pubsub => ../../libp2p/go-libp2p-pubsub
