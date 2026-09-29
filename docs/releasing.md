# Release process

English | [简体中文](releasing.zh-CN.md)

The maintainer publishes to Mooncakes manually. Preparing an archive, committing, pushing, creating a GitHub Release and running `moon publish` are separate steps. This guide does not automate publication.

## Before publication

1. Choose the version and check `moon.mod`, example dependencies and the candidate ZIP filename in CI. Before freezing the candidate, finalize the CHANGELOG version and intended release date and the installation instructions in both READMEs. Replace temporary “release pending” prose with version-specific instructions that tell readers to verify registry availability; do not claim publication has succeeded before it has. If the release date changes, create and validate a new candidate.
2. Complete the local checks and archive link validation in [Development and verification](testing.md). Inspect `moon package --list`; fixtures, historical reports, interoperability programs and scripts should not be in the library package.
3. Commit and push reviewed changes. Verify that the remote commit matches the candidate source and that all required CI jobs pass for that commit. Historical passes do not replace this check.
4. Build an external consumer from the candidate ZIP to verify dependency resolution and native compilation. Confirm the documented platform and Broker verification boundaries.
5. Verify that GitHub documentation links point to files included in the remote commit. The link checker checks corresponding local targets, not remote availability or Markdown anchors.

## Manual publication

From the confirmed, clean candidate commit, check the account and registered versions:

```sh
moon whoami
moon view pangbit/moonpulsar --versions --json
```

A 404 response alone does not establish publishing permission or version availability. After confirmation, the maintainer runs:

```sh
moon publish
```

After publication succeeds, query the registered version again, then install from the registry and build a new consumer outside the repository. Create the matching GitHub tag and Release against the exact validated candidate commit and record the publication result in the Release notes. Do not move the release tag to a later documentation commit. Any subsequent documentation correction is a separate commit and does not change the already published package. The archive, registry version and GitHub Release must trace back to the same candidate commit.
