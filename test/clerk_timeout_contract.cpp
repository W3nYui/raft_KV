#include "clerk.h"

#include <cassert>

namespace {

void AssertFailed(const ClerkOperationResult& result) {
  assert(result.status == ClerkOperationStatus::Failed);
  assert(result.value.empty());
}

}  // namespace

int main() {
  Clerk clerk;

  AssertFailed(clerk.GetWithTimeout("missing", 10));
  AssertFailed(clerk.PutWithTimeout("key", "value", 10));
  AssertFailed(clerk.AppendWithTimeout("key", "value", 10));
  AssertFailed(clerk.GetWithTimeout("missing", 0));
  AssertFailed(clerk.PutWithTimeout("key", "value", -1));
  return 0;
}
