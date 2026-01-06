# ASF Release Management Guide for DataSketches

This guide outlines the technical steps for a Committer to act as a Release Manager (RM) for Apache DataSketches. It covers the workflow from preparing the code to signing the git tag and uploading the release candidate.

## Step 0: Define Environment Variables

Setting these variables at the start prevents typing errors and makes the subsequent commands reusable.

```bash
# Set your project name
export PROJECT="datasketches"

# Set the language/implementation (e.g., "go", "java", "cpp")
export LANGUAGE="go"

# Set the version you are releasing
export VERSION="1.2.3"

# Set the current Release Candidate number
export RC="rc1"

# Derived variable for convenience
export CANDIDATE_NAME="${VERSION}-${RC}"
```

## Prerequisites

**GPG Key**: You must have a GPG key associated with your @apache.org email.

**KEYS File**: Your public key must be present in the DataSketches KEYS files before you start:
- Development: https://dist.apache.org/repos/dist/dev/datasketches/KEYS
- Release: https://dist.apache.org/repos/dist/release/datasketches/KEYS

**Subversion (SVN)**: ASF releases are officially distributed via SVN.

### How to add your key to KEYS (if missing):

```bash
# Export your public key in ASCII armor format
(gpg --list-sigs <key ID> && gpg --armor --export <Key ID>) >> KEYS

# Then upload the updated KEYS file to the SVN repos listed above.
```

## Step 1: Prepare the Source and Sign the Tag

The RM must ensure the code is ready and create a signed git tag to mark the exact state of the release.

```bash
# 1. Ensure you are on the correct branch
git checkout main
git pull origin main

# 2. Create a SIGNED tag
# -s uses your default GPG key to sign the tag
git tag -s "v${CANDIDATE_NAME}" -m "Release Candidate ${RC} for version ${VERSION}"

# 3. Push the tag to the Apache remote
git push origin "v${CANDIDATE_NAME}"
```

## Step 2: Package the Source Release

ASF releases are primarily source code releases.

```bash
# 1. Export the source (avoids local uncommitted files)
git archive --format=tar.gz \
    --prefix="apache-${PROJECT}-${LANGUAGE}-${VERSION}/" \
    "v${CANDIDATE_NAME}" > "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz"

# 2. Create a SHA512 checksum
sha512sum "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz" > "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz.sha512"
```

## Step 3: Sign the Artifact

Use your GPG key to create an ASCII-armored signature (.asc) for the source package.

```bash
# Sign the artifact
# -b creates a detached signature, -a creates ASCII output
gpg --armor --detach-sig "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz"

# Verify the signature (test it yourself first)
gpg --verify "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz.asc" "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz"
```

## Step 4: Upload to the Dev Repository

Move your signed artifacts to the official "dev" area on Subversion for community review.

```bash
# 1. Checkout the dev distribution directory (contains all languages)
svn checkout "https://dist.apache.org/repos/dist/dev/${PROJECT}/" asf-dist-dev

# 2. Create a folder for the new RC (format: VERSION-RC with uppercase RC)
mkdir -p "asf-dist-dev/${LANGUAGE}/${VERSION}-RC${RC#rc}"
cp "apache-${PROJECT}-${LANGUAGE}-${VERSION}-src.tar.gz"* "asf-dist-dev/${LANGUAGE}/${VERSION}-RC${RC#rc}/"

# 3. Add and commit
cd asf-dist-dev
svn add "${LANGUAGE}/${VERSION}-RC${RC#rc}"
svn commit -m "Upload ${LANGUAGE}/${VERSION}-RC${RC#rc} for PMC review"
```

## Step 5: Call for a Vote

Send an email to dev@datasketches.apache.org with the subject `[VOTE] Release Apache DataSketches ${LANGUAGE} ${VERSION} ${RC}`.

The email must include:
- Link to the signed git tag: `https://github.com/apache/${PROJECT}/releases/tag/v${CANDIDATE_NAME}`
- Link to the artifacts: `https://dist.apache.org/repos/dist/dev/${PROJECT}/${LANGUAGE}/${VERSION}-RC${RC#rc}/`
- The Release Manager's GPG Fingerprint (run `gpg --fingerprint your_email@apache.org`).
- A 72-hour window for the vote.

## Step 6: Finalize (After Approval)

Once you receive at least three +1 binding votes from PMC members and no vetos:

1. **Move to Release**: Move the files from `dist/dev/${PROJECT}/${LANGUAGE}/${VERSION}-RC${RC#rc}` to `dist/release/${PROJECT}/${LANGUAGE}/${VERSION}` via SVN.
2. **Delete Old RCs**: Remove the candidate files from the dev directory.
3. **Promote Tag**: Create a final version tag `v${VERSION}` from the successful `v${CANDIDATE_NAME}` and push.
4. **Announce**: Wait 24 hours for mirrors to sync, then email announce@apache.org.
