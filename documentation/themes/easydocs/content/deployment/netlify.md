+++
title = "Netlify"
weight = 20
+++

Netlify provides best practices like SSL, CDN distribution, caching and continuous deployment
with no effort. This site is hosted by Netlify and automatically deployed on commits.

If you don't have an account with Netlify, you can [sign up](https://app.netlify.com) for one.


## Automatic deploys

Once you are in the admin interface, you can add a site from a Git provider (GitHub, GitLab or Bitbucket). At the end
 of this process, you can select the deploy settings for the project:

 - build command: `zola build` (replace the version number in the variable by the version you want to use)
 - publish directory: the path to where the `public` directory is
 - image selection: `Ubuntu Xenial 16.04 (default)`
 - Environment variables: `ZOLA_VERSION` with for example `0.8.0` as value

With this setup, your site should be automatically deployed on every commit on master.  For `ZOLA_VERSION`, you may
use any of the tagged `release` versions in the GitHub repository. Netlify will automatically fetch the tagged version
and use it to build your site.

However, if you want to use everything that Netlify gives you, you should also publish temporary sites for pull requests.

This is done by adding the following `netlify.toml` file in your repository and removing the build command/publish
directory in the admin interface.

```toml
[build]
# This assumes that the Zola site is in a docs folder. If it isn't, you don't need
# to have a `base` variable but you do need the `publish` and `command` variables.
base    = "docs"
publish = "docs/public"
command = "zola build"

[build.environment]
# Set the version name that you want to use and Netlify will automatically use it.
ZOLA_VERSION = "0.9.0"

# The magic for deploying previews of branches.
# We need to override the base url with whatever url Netlify assigns to our
# preview site.  We do this using the Netlify environment variable
# `$DEPLOY_PRIME_URL`.

[context.deploy-preview]
command = "zola build --base-url $DEPLOY_PRIME_URL"
```

## Manual deploys
If you would prefer to use a version of Zola that isn't a tagged release (for example, after having built Zola from
source and made modifications), then you will need to manually deploy your `public` folder to Netlify.  You can do
this through Netlify's web GUI or via the command line.

For a command-line manual deploy, follow these steps:
 1.  Generate a `Personal Access Token` from the settings section of your Netlify account (*not* an OAuth Application).
 2.  Build your site with `zola build`.
 3.  Create a zip folder containing the `public` directory.
 4.  Run the `curl` command below, filling in your values for PERSONAL_ACCESS_TOKEN_FROM_STEP_1, FILE_NAME.zip
 and SITE_NAME.
 5.  (Optional) delete the zip folder.

```bash
curl -H "Content-Type: application/zip" \
     -H "Authorization: Bearer PERSONAL_ACCESS_TOKEN_FROM_STEP_1" \
     --data-binary "@FILE_NAME.zip" \
     https://api.netlify.com/api/v1/sites/SITE_NAME.netlify.com/deploys
```
