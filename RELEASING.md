# Releasing FGBA

Owner-run steps to publish v0.1.0. Everything up to step 4 is already done at
HEAD; these steps require the owner's GitHub and crates.io credentials.

Prerequisites: Rust 1.91.0 or newer (the workspace MSRV), `gh` CLI authenticated
(`gh auth login`), a crates.io account with publish rights, and a clean working
tree on `main`.

## 1. Create the GitHub remote

```sh
gh repo create afterrealism/fgba --public --source . --push
```

(or create `afterrealism/fgba` manually on github.com, then
`git remote add origin git@github.com:afterrealism/fgba.git && git push -u origin main`)

The `repository` fields in the crate manifests already point at this URL, so
crates.io/docs.rs links resolve as soon as the repo exists.

## 2. Enable the website (GitHub Pages)

Settings → Pages → Build and deployment → Source: **GitHub Actions**.
The `pages.yml` workflow then deploys `docs/` on every push to `main`.

## 3. Log in to crates.io

```sh
cargo login   # paste an API token from https://crates.io/me
```

## 4. Publish the crates — library first

`fgba-cli` depends on `fgba = "0.1.0"`, so **fgba must be published and visible
in the index before fgba-cli can package** (cargo resolves the dependency
against crates.io during `cargo publish`; wait a minute for index propagation
between the two commands).

```sh
cargo publish -p fgba
cargo publish -p fgba-cli
```

Both manifests were verified with `cargo publish --dry-run` / packaged-source
compile+test runs before tagging; the dry-run of `fgba-cli` only passes once
`fgba 0.1.0` exists in the index.

## 5. Tag the release

```sh
git tag v0.1.0 && git push --tags
```

## 6. Post-publish verification

- **docs.rs**: <https://docs.rs/fgba> builds (the `http` feature is enabled via
  `[package.metadata.docs.rs]`, so `HttpRangeReader` appears in the docs).
- **crates.io**: <https://crates.io/crates/fgba> and
  <https://crates.io/crates/fgba-cli> show the README, description,
  keywords/categories, and the Apache-2.0 OR MIT license.
- **Install and smoke-test the CLI from crates.io**:

```sh
cargo install fgba-cli
fgba --help
```

- Flip the CHANGELOG entry from `## [0.1.0] - Unreleased` to
  `## [0.1.0] - <date>` in the first post-release commit.
