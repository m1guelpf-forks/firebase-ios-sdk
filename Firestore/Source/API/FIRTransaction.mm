/*
 * Copyright 2017 Google
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#import "FIRTransaction.h"

#include <memory>
#include <set>
#include <utility>
#include <vector>

#import "Firestore/Source/API/FIRDocumentReference+Internal.h"
#import "Firestore/Source/API/FIRDocumentSnapshot+Internal.h"
#import "Firestore/Source/API/FIRFirestore+Internal.h"
#import "Firestore/Source/API/FIRTransaction+Internal.h"
#import "Firestore/Source/API/FSTUserDataReader.h"

#include "Firestore/core/src/core/transaction.h"
#include "Firestore/core/src/core/user_data.h"
#include "Firestore/core/src/model/document.h"
#include "Firestore/core/src/util/error_apple.h"
#include "Firestore/core/src/util/exception.h"
#include "Firestore/core/src/util/hard_assert.h"
#include "Firestore/core/src/util/status.h"
#include "Firestore/core/src/util/statusor.h"

using firebase::firestore::core::ParsedSetData;
using firebase::firestore::core::ParsedUpdateData;
using firebase::firestore::core::Transaction;
using firebase::firestore::model::Document;
using firebase::firestore::model::DocumentKey;
using firebase::firestore::util::MakeNSError;
using firebase::firestore::util::StatusOr;
using firebase::firestore::util::ThrowInvalidArgument;

NS_ASSUME_NONNULL_BEGIN

#pragma mark - FIRTransaction

@interface FIRTransaction ()

- (instancetype)initWithTransaction:(std::shared_ptr<Transaction>)transaction
                          firestore:(FIRFirestore *)firestore NS_DESIGNATED_INITIALIZER;

@property(nonatomic, strong, readonly) FIRFirestore *firestore;
@end

@implementation FIRTransaction (Internal)

+ (instancetype)transactionWithInternalTransaction:(std::shared_ptr<Transaction>)transaction
                                         firestore:(FIRFirestore *)firestore {
  return [[FIRTransaction alloc] initWithTransaction:std::move(transaction) firestore:firestore];
}

@end

@implementation FIRTransaction {
  std::shared_ptr<Transaction> _internalTransaction;
}

- (instancetype)initWithTransaction:(std::shared_ptr<Transaction>)transaction
                          firestore:(FIRFirestore *)firestore {
  self = [super init];
  if (self) {
    _internalTransaction = std::move(transaction);
    _firestore = firestore;
  }
  return self;
}

- (FIRTransaction *)setData:(NSDictionary<NSString *, id> *)data
                forDocument:(FIRDocumentReference *)document {
  return [self setData:data forDocument:document merge:NO];
}

- (FIRTransaction *)setData:(NSDictionary<NSString *, id> *)data
                forDocument:(FIRDocumentReference *)document
                      merge:(BOOL)merge {
  [self validateReference:document];
  ParsedSetData parsed = merge ? [self.firestore.dataReader parsedMergeData:data fieldMask:nil]
                               : [self.firestore.dataReader parsedSetData:data];
  _internalTransaction->Set(document.key, std::move(parsed));
  return self;
}

- (FIRTransaction *)setData:(NSDictionary<NSString *, id> *)data
                forDocument:(FIRDocumentReference *)document
                mergeFields:(NSArray<id> *)mergeFields {
  [self validateReference:document];
  ParsedSetData parsed = [self.firestore.dataReader parsedMergeData:data fieldMask:mergeFields];
  _internalTransaction->Set(document.key, std::move(parsed));
  return self;
}

- (FIRTransaction *)updateData:(NSDictionary<id, id> *)fields
                   forDocument:(FIRDocumentReference *)document {
  [self validateReference:document];
  ParsedUpdateData parsed = [self.firestore.dataReader parsedUpdateData:fields];
  _internalTransaction->Update(document.key, std::move(parsed));
  return self;
}

- (FIRTransaction *)deleteDocument:(FIRDocumentReference *)document {
  [self validateReference:document];
  _internalTransaction->Delete(document.key);
  return self;
}

- (void)getDocument:(FIRDocumentReference *)document
         completion:(void (^)(FIRDocumentSnapshot *_Nullable document,
                              NSError *_Nullable error))completion {
  [self getDocuments:@[ document ]
          completion:^(NSArray<FIRDocumentSnapshot *> *_Nullable snapshots,
                       NSError *_Nullable error) {
            completion(snapshots.firstObject, error);
          }];
}

- (void)getDocuments:(NSArray<FIRDocumentReference *> *)documents
          completion:(void (^)(NSArray<FIRDocumentSnapshot *> *_Nullable snapshots,
                               NSError *_Nullable error))completion {
  documents = [documents copy];
  std::vector<DocumentKey> keys;
  std::set<DocumentKey> seen;
  for (FIRDocumentReference *document in documents) {
    [self validateReference:document];
    if (seen.insert(document.key).second) {
      keys.push_back(document.key);
    }
  }
  if (keys.empty()) {
    completion(@[], nil);
    return;
  }
  _internalTransaction->Lookup(
      keys, [self, documents, completion](const StatusOr<std::vector<Document>> &maybe_documents) {
        if (!maybe_documents.ok()) {
          completion(nil, MakeNSError(maybe_documents.status()));
          return;
        }

        NSMutableDictionary<NSString *, FIRDocumentSnapshot *> *byPath =
            [NSMutableDictionary dictionary];
        for (const Document &internalDoc : maybe_documents.ValueOrDie()) {
          absl::optional<Document> snapshotDocument;
          if (internalDoc->is_found_document()) {
            snapshotDocument = internalDoc;
          } else {
            HARD_ASSERT(internalDoc->is_no_document(),
                        "BatchGetDocumentsRequest returned unexpected document type: %s",
                        internalDoc.ToString());
          }
          FIRDocumentSnapshot *snapshot =
              [[FIRDocumentSnapshot alloc] initWithFirestore:self.firestore
                                                 documentKey:internalDoc->key()
                                                    document:snapshotDocument
                                                   fromCache:false
                                            hasPendingWrites:false];
          byPath[snapshot.reference.path] = snapshot;
        }
        NSMutableArray<FIRDocumentSnapshot *> *snapshots =
            [NSMutableArray arrayWithCapacity:documents.count];
        for (FIRDocumentReference *document in documents) {
          FIRDocumentSnapshot *snapshot = byPath[document.path];
          HARD_ASSERT(snapshot != nil, "Document lookup did not return a requested document.");
          [snapshots addObject:snapshot];
        }
        completion(snapshots, nil);
      });
}

- (FIRDocumentSnapshot *_Nullable)getDocument:(FIRDocumentReference *)document
                                        error:(NSError *__autoreleasing *)error {
  return [self getDocuments:@[ document ] error:error].firstObject;
}

- (NSArray<FIRDocumentSnapshot *> *_Nullable)getDocuments:
                                                 (NSArray<FIRDocumentReference *> *)documents
                                                    error:(NSError *__autoreleasing *)error {
  dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
  __block NSArray<FIRDocumentSnapshot *> *result;
  __block NSError *outerError = nil;
  [self getDocuments:documents
          completion:^(NSArray<FIRDocumentSnapshot *> *_Nullable snapshots,
                       NSError *_Nullable innerError) {
            result = snapshots;
            outerError = innerError;
            dispatch_semaphore_signal(semaphore);
          }];
  dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
  if (error) {
    *error = outerError;
  }
  return result;
}

- (void)validateReference:(FIRDocumentReference *)reference {
  if (reference.firestore != self.firestore) {
    ThrowInvalidArgument("Provided document reference is from a different Cloud Firestore "
                         "instance.");
  }
}

@end

NS_ASSUME_NONNULL_END
