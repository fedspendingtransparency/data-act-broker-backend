## Public Domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

## Git Workflow

We use three main branches:

* `master` - stable code deployed to staging
* `development` - code in development that is periodically released to staging by merging into master
* `production` - Not currently in use since we're still in active development. Will be used to push code into production.

Only non-breaking, stable code should be merged into `master` and deployed to staging to prevent disruptions in other parts of the team. All code to be merged should be submitted via a pull request. Team members should _not_ merge their own pull requests but should instead request a code review first. The reviewing team member should merge the pull request after completing the review and ensuring it passes all continuous integration tests.

## Code Reviews

The Consumer Financial Protection Bureau has an excellent [code review guide](https://github.com/cfpb/front-end/blob/master/code-reviews.md). 

## Continuous Integration

Our project uses Jenkins to run test suites and builds. Pull requests to master will automatically trigger jenkins to run the tests. 

## Concluding a Sprint

At the conclusion of a sprint, all code for completed stories should be merged into `master` and deployed to staging. 
