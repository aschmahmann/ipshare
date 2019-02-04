# Content Resolvers

This package utilizes IPShare's append-only DAG synchronization schemes to implement example shared data structures.

The two currently implemented are:

* String G-Set (Grow-Only Set) CRDT
* A CID based register with conflict resolution (e.g. IPNS, but where if multiple changes occur contemporaneously all options are available to the user, instead of simply Last-Writer-Wins semantics)