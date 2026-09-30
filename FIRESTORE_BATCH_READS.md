# Batch transaction reads

This branch starts at Firebase Apple SDK 12.19.2. It adds
`Transaction.getDocuments(_:)` for Swift and `getDocuments:error:` for Objective-C.

The method sends all unique document keys to the existing C++ transaction lookup
in one request. The core records each document version. Existing documents and
missing documents keep their normal transaction checks. Reads must occur before
writes. All references must belong to the transaction's Firestore instance.

The result follows the input order. Missing documents return snapshots with
`exists == false`. Repeated references return repeated snapshots. An empty input
returns an empty array without a request. The single-document method uses the
same implementation.

The Swift package builds Firestore from source. The official prebuilt binary
does not contain this method. The first build must also compile gRPC and Abseil.

The patch includes Objective-C integration checks for input order, missing
documents, repeated references, empty input, reference ownership, reads after
writes, and transaction retries. The Swift compile check uses the new method.
Run the Firestore integration suite against the local Firestore emulator.
