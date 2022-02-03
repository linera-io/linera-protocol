+++
title = "GitHub Pages"
weight = 30
+++

By default, GitHub Pages uses Jekyll (a ruby based static site generator),
but you can also publish any generated files provided you have an `index.html` file in the root of a branch called
`gh-pages` or `master`. In addition you can publish from a `docs` directory in your repository. That branch name can
also be manually changed in the settings of a repository.

We can use any continuous integration (CI) server to build and deploy our site. For example:

 * [Github Actions](#github-actions)
 * [Travis CI](#travis-ci)

## Github Actions

Using *Github Actions* for the deployment of your Zola-Page on Github-Pages is pretty easy. You basically need three things:

1. A *Personal access token* to give the *Github Action* the permission to push into your repository.
2. Create the *Github Action*.
3. Check the *Github Pages* section in repository settings.

Let's start with the token.

For creating the token either click on [here](https://github.com/settings/tokens) or go to Settings > Developer Settings > Personal access tokens. Under the *Select Scopes* section, give it *repo* permissions and click *Generate token*. Then copy the token, navigate to your repository and add in the Settings tab the *Secret* `TOKEN` and paste your token in it.

Next we need to create the *Github Action*. Here we can make use of the [zola-deploy-action](https://github.com/shalzz/zola-deploy-action). Go to the *Actions* tab of your repository, click on *set up a workflow yourself* to get a blank workflow file. Copy the following script into it and commit it afterwards.

```yaml
# On every push this script is executed
on: push
name: Build and deploy GH Pages
jobs:
  build:
    name: shalzz/zola-deploy-action
    runs-on: ubuntu-latest
    steps:
    # Checkout
    - uses: actions/checkout@master
    # Build & deploy
    - name: shalzz/zola-deploy-action
      uses: shalzz/zola-deploy-action@v0.12.0
      env:
        # Target branch
        PAGES_BRANCH: gh-pages
        # Provide personal access token
        TOKEN: ${{ secrets.TOKEN }}
```

This script is pretty simple, because the [zola-deploy-action](https://github.com/shalzz/zola-deploy-action) is doing everything for you. You just need to provide some details. For more configuration options check out the [README](https://github.com/shalzz/zola-deploy-action/blob/master/README.md).

By commiting the action your first build is triggered. Wait until it's finished, then you should see in your repository a new branch *gh-pages* with the compiled *Zola* page in it.

Finally we need to check the *Github Pages* section of the repository settings. Click on the *Settings* tab and scroll down to the *Github Pages* section. Check if the source is set to *gh-pages* branch and the directory is */ (root)*. You should also see your *Github Pages* link.

There you can also configure a *custom domain* and *Enforce HTTPS* mode. Before configuring a *custom domains*, please check out [this](https://github.com/shalzz/zola-deploy-action/blob/master/README.md#custom-domain). 

## Travis CI

We are going to use [Travis CI](https://travis-ci.org) to automatically publish the site. If you are not using Travis
already, you will need to login with the GitHub OAuth and activate Travis for the repository.
Don't forget to also check if your repository allows GitHub Pages in its settings.

## Ensure that Travis can access your theme

Depending on how you added your theme, Travis may not know how to access
it. The best way to ensure that it will have full access to the theme is to use git
submodules. When doing this, ensure that you are using the `https` version of the URL.

```shell
$ git submodule add {THEME_URL} themes/{THEME_NAME}
```

## Allowing Travis to push to GitHub

Before pushing anything, Travis needs a Github private access key to make changes to your repository.
If you're already logged in to your account, just click [here](https://github.com/settings/tokens) to go to
your tokens page.
Otherwise, navigate to `Settings > Developer Settings > Personal Access Tokens`.
Generate a new token and give it any description you'd like.
Under the "Select Scopes" section, give it repo permissions. Click "Generate token" to finish up.

Your token will now be visible.
Copy it into your clipboard and head back to Travis.
Once on Travis, click on your project, and navigate to "Settings". Scroll down to "Environment Variables" and input a name of `GH_TOKEN` with a value of your access token.
Make sure that "Display value in build log" is off, and then click add. Now Travis has access to your repository.

## Setting up Travis

We're almost done. We just need some scripts in a .travis.yml file to tell Travis what to do.

**NOTE**: The script below assumes that we're taking the code from the `code` branch and will generate the HTML to be published in the `master` branch of the same repository. You're free to use any other branch for the Markdown files but if you want to use `<username>.github.io` or `<org>.github.io`, the destination branch **MUST** be `master`.

```yaml
language: minimal

before_script:
  # Download and unzip the zola executable
  # Replace the version numbers in the URL by the version you want to use
  - curl -s -L https://github.com/getzola/zola/releases/download/v0.9.0/zola-v0.9.0-x86_64-unknown-linux-gnu.tar.gz | sudo tar xvzf - -C /usr/local/bin

script:
  - zola build

# If you are using a different folder than `public` for the output directory, you will
# need to change the `zola` command and the `ghp-import` path
after_success: |
  [ $TRAVIS_BRANCH = code ] &&
  [ $TRAVIS_PULL_REQUEST = false ] &&
  zola build &&
  sudo pip install ghp-import &&
  ghp-import -n public -b master &&
  git push -fq https://${GH_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git master
```

If your site is using a custom domain, you will need to mention it in the `ghp-import` command:
`ghp-import -c vaporsoft.net -n public` for example.

Credits: this page is based on the article https://vaporsoft.net/publishing-gutenberg-to-github/
