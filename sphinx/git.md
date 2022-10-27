# git Documentation

In this document, we go through the best practices when using git.

## setting up the SSH keys

Use [GitHub SSH key setting](https://github.com/settings/keys) to set up your SSH key to use. After you have done setting it up, you should be able to clone, push, etc. Make sure to use the SSH link when you are interacting with GitHub repositories.

## Branching Strategy

Each user should work on their branch (other than main/master) and avoid conflicts, it is recommended that each person works on different files to avoid conflicts.

## Merge Strategy

No merging to master should be directly allowed. Each merging to master should be done through a pull request[^1]. In which the changes are reviewed by the lead[^2]. If the master is ahead of the branch, use git rebase first, and then merge into master.
[^1]:Please refer to [pull request tutorial](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) for more details about pull request.
 [^2]: Please look at [add reviewer tutorial](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/requesting-a-pull-request-review) for more details about adding reviewer to a pull request.

### Merging to master branch procedure

If you are using Visual Studio Code you can change git editor by

```bash
git config --global core.editor "code --wait"
```

In case you are using Pycharm consider using [Pycharm rebase documentation](https://www.jetbrains.com/help/pycharm/apply-changes-from-one-branch-to-another.html#rebase-branch). You can change `code` with the editor of your choice [^3]. The proposed merging procedure is given below:

```sh
git switch to_be_merged_branch
git pull
git rebase master -i
git push --force
```

[^3]: For more configuration of git please refer to [git configuration](https://git-scm.com/book/en/v2/Customizing-Git-Git-Configuration)

In the [rebase](https://git-scm.com/docs/git-rebase) step, try to clean the commit history of your code. In the next step open GitHub and open a pull request and add the respective reviewers.

## Commit Strategy

Commit messages should follow one of the followings in which \<jira-issue-key> can be found in Jira issues:

- \<jira-issue-key> **feat**: A new feature
- \<jira-issue-key> **fix**: A bug fix
- \<jira-issue-key> **style**: Additions or modifications related to styling only
- \<jira-issue-key> **refactor**: Code refactoring
- \<jira-issue-key> **test**: Additions or modifications to test cases
- \<jira-issue-key> **docs**: README, Architectural, or anything related to - documentation
- \<jira-issue-key> **chore**: Regular code maintenance

As an example:

```bash
git commit -m "DSIP-3758 dpcs: update git documentation"
```
