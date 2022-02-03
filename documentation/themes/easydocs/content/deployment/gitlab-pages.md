+++
title = "GitLab Pages"
weight = 40
+++

We are going to use the GitLab CI runner to automatically publish the site (this CI runner is already included in your repository if you use GitLab.com).

## Repository setup

Your repository needs to be set up to be a user or group website. This means the name of the repository has to be in the correct format.

For example, assuming that the username is `john`, you have to create a project called `john.gitlab.io`. Your project URL will be `https://gitlab.com/john/john.gitlab.io`. Once you enable GitLab Pages for your project, your website will be published under `https://john.gitlab.io`.

Under your group `websites`, you created a project called `websites.gitlab.io`. Your project’s URL will be `https://gitlab.com/websites/websites.gitlab.io`. Once you enable GitLab Pages for your project, your website will be published under `https://websites.gitlab.io`.


This guide assumes that your Zola project is located in the root of your repository.

## Ensuring that the CI runner can access your theme

Depending on how you added your theme, your repository may not contain it. The best way to ensure that the theme will
be added is to use submodules. When doing this, ensure that you are using the `https` version of the URL.

```shell
$ git submodule add {THEME_URL} themes/{THEME_NAME}
```

For example, this could look like:
```shell
$ git submodule add https://github.com/getzola/hyde.git themes/hyde
```

## Setting up the GitLab CI/CD Runner

The second step is to tell the GitLab continuous integration runner how to create the GitLab page.

To do this, create a file called `.gitlab-ci.yml` in the root directory of your repository.

```yaml
image: alpine:latest
variables:
  # This variable will ensure that the CI runner pulls in your theme from the submodule
  GIT_SUBMODULE_STRATEGY: recursive  
  # Specify the zola version you want to use here
  ZOLA_VERSION: "v0.12.0"

pages:
  script:
    # Install the zola package from the alpine community repositories
    - apk add --update-cache --repository http://dl-3.alpinelinux.org/alpine/edge/community/ zola
    # Execute zola build
    - zola build
    
  artifacts:
    paths:
      # Path of our artifacts
      - public
      
  # This config will only publish changes that are pushed on the master branch
  only: 
    - master
```

Push this new file and ... Tada! You're done! If you navigate to `settings > pages`, you should be able to see
something like this:

> Congratulations! Your pages are served under:  
https://john.gitlab.io

More information on the process to host on GitLab pages and additional information like using a custom domain is documented 
[in this GitLab blog post](https://about.gitlab.com/2016/04/07/gitlab-pages-setup/).
