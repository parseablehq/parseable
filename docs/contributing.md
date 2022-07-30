# Contributing

First, thank you for contributing to Parseable! The goal of this document is to provide everything you need to start contributing to Parseable.

Remember that there are many ways to contribute other than writing code: writing [tutorials or blog posts](https://github.com/parseablehq/parseable/tree/main/blog), improving [the documentation](https://github.com/parseablehq/parseable/tree/main/docs), and submitting [bug reports](https://github.com/parseablehq/parseable/issues/new).

## Table of Contents

- [Assumptions](#assumptions)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Git Guidelines](#git-guidelines)

## Assumptions

1. You're familiar with [GitHub](https://github.com) and the [Pull Requests](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)(PR) workflow.
2. You've read the Parseable [documentation](https://docs.parseable.io).
3. You've discussed the changes you plan to make, with [Parseable community].

## How to Contribute

1. Ensure your change has an issue! Find an
   [existing issue](https://github.com/parseablehq/parseable/issues/) or [open a new issue](https://github.com/parseablehq/parseable/issues/new).
2. Once approved, [fork the Parseable repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) in your own GitHub account.
3. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository)
4. Review the [Development Workflow](#development-workflow) section that describes the steps to maintain the repository.
5. Make your changes on your branch.
6. [Submit the branch as a Pull Request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) pointing to the `main` branch of the Parseable repository. A maintainer should comment and/or review your Pull Request within a few days.

## Development Workflow

We recomment a Linux or macOS as the development platform for Parseable currently.
### Setup and run Parseable

```bash
cargo run --release
```

We recommend using the `--release` flag to test the full performance of Parseable.

### Test

```bash
cargo test
```

This command will be triggered to each PR as a requirement for merging it.

If you get a "Too many open files" error you might want to increase the open file limit using this command:

```bash
ulimit -Sn 3000
```

If you get a "OpenSSL" related error while building Parseable, you might need to install the dependencies using this command:

```bash
sudo apt install build-essential
sudo apt-get install libssl-dev
sudo apt-get install pkg-config
```
## Git Guidelines

1. All changes must be made in a branch and submitted as PR.
2. We recommend commit message follows [the Chris Beams](https://chris.beams.io/posts/git-commit/) style.
3. The PR title should be accurate and descriptive of the changes.
4. The draft PRs are recommended when you want to show that you are working on something and make your work visible. [Convert your PR as a draft](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/changing-the-stage-of-a-pull-request) if your changes are a work in progress. Maintainers will review the PR once you mark your PR as ready for review.
5. The branch related to the PR must be **up-to-date with `main`** before merging. We use [Bors](https://github.com/bors-ng/bors-ng) to automatically enforce this requirement without the PR author having to rebase manually.
