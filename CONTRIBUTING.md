### Contributing

This document outlines how you can contribute to Parseable.

Thank you for considering to contribute to Parseable. The goal of this document is to provide everything you need to start your contribution. We encourage all contributions, including but not limited to:
- Code patches, bug fixes.
- Tutorials or blog posts.
- Improving the documentation.
- Submitting [bug reports](https://github.com/parseablehq/parseable/issues/new).

### Prerequisites

- Your PR has an associated issue. Find an [existing issue](https://github.com/parseablehq/parseable/issues) or open a new issue.
- You've discussed the with [Parseable community](https://logg.ing/community).
- You're familiar with GitHub Pull Requests(PR) workflow.
- You've read the [Parseable documentation](https://www.parseable.com/docs).

### Contribution workflow

- Fork the [Parseable repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) in your own GitHub account.
- [Create a new branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
- Review the Development Workflow section that describes the steps to maintain the repository.
- Make your changes on your branch.
- Submit the branch as a Pull Request pointing to the main branch of the Parseable repository. A maintainer should comment and/or review your Pull Request within a few hours.
- You’ll be asked to review & sign the [Parseable Contributor License Agreement (CLA)](https://github.com/parseablehq/.github/blob/main/CLA.md) on the GitHub PR. Please ensure that you review the document. Once you’re ready, please sign the CLA by adding a comment I have read the CLA Document and I hereby sign the CLA in your Pull Request.
- Please ensure that the commits in your Pull Request are made by the same GitHub user (which was used to create the PR and add the comment).

### Development workflow

We recommend Linux or macOS as the development platform for Parseable.

#### Setup and run Parseable

Parseable needs Rust 1.77.1 or above. Use the below command to build and run Parseable binary with local mode.

```sh
cargo run --release local-store
```

We recommend using the --release flag to test the full performance of Parseable.

#### Running Tests

```sh
cargo test
```

This command will be triggered to each PR as a requirement for merging it.

If you get a "Too many open files" error you might want to increase the open file limit using this command:

```sh
ulimit -Sn 3000
```

If you get a OpenSSL related error while building Parseable, you might need to install the dependencies using this command:

```sh
sudo apt install build-essential
sudo apt-get install libssl-dev
sudo apt-get install pkg-config
```

### Git Guidelines

- The PR title should be accurate and descriptive of the changes.
- The draft PRs are recommended when you want to show that you are working on something and make your work visible. Convert your PR as a draft if your changes are a work in progress. Maintainers will review the PR once you mark your PR as ready for review.
- The branch related to the PR must be up-to-date with main before merging. We use Bors to automatically enforce this requirement without the PR author having to rebase manually.
