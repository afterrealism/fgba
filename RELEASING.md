# Releasing FGBA

Owner-run steps to publish v0.1.0. Run the complete native, actual-WASM, and
browser checks in the [integration guide](docs/tilify-integration.md#local-verification)
before release; these publish steps require the owner's GitHub and crates.io
credentials. The standalone `fgba-wasm` browser package is built from source
with wasm-pack and is not an additional registry release step.

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

## 4. Publish the crates — dependency order

`fgba-sql` depends on `fgba = "0.1.0"`, and `fgba-cli` depends on both
`fgba = "0.1.0"` and `fgba-sql = "0.1.0"`. Publish in exactly this order and
wait for each dependency to be visible in the crates.io index before packaging
the next crate:

1. `fgba`
2. `fgba-sql`
3. `fgba-cli`

```sh
cargo publish -p fgba
cargo info fgba@0.1.0       # retry until the new core is visible
cargo publish -p fgba-sql
cargo info fgba-sql@0.1.0   # retry until the new SQL crate is visible
cargo publish -p fgba-cli
```

Run `cargo publish --dry-run` and packaged-source compile+test checks before
tagging. The SQL and CLI dry-runs require their newly published dependencies to
be available in the index.

## 5. Tag the release

```sh
git tag v0.1.0 && git push --tags
```

## 6. Post-publish verification

- **docs.rs**: <https://docs.rs/fgba> builds (the `http` feature is enabled via
  `[package.metadata.docs.rs]`, so `HttpRangeReader` appears in the docs).
- **crates.io**: <https://crates.io/crates/fgba>,
  <https://crates.io/crates/fgba-sql>, and
  <https://crates.io/crates/fgba-cli> show the README, description,
  keywords/categories, and the Apache-2.0 OR MIT license.
- **Install and smoke-test the CLI from crates.io**:

```sh
cargo install fgba-cli
fgba --help
```

- Flip the CHANGELOG entry from `## [0.1.0] - Unreleased` to
  `## [0.1.0] - <date>` in the first post-release commit.
