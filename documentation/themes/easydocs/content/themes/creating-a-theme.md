+++
title = "Creating a theme"
weight = 30
+++

Creating a theme is exactly like creating a normal site with Zola, except you
will want to use many [Tera blocks](https://tera.netlify.com/docs#inheritance) to
allow users to easily modify it.

## Getting started
As mentioned, a theme is just like any site; start by running `zola init MY_THEME_NAME`.

The only thing needed to turn that site into a theme is to add a `theme.toml` configuration file with the
following fields:

```toml
name = "my theme name"
description = "A classic blog theme"
license = "MIT"
homepage = "https://github.com/getzola/hyde"
# The minimum version of Zola required
min_version = "0.4.0"
# An optional live demo URL
demo = ""

# Any variable there can be overriden in the end user `config.toml`
# You don't need to prefix variables by the theme name but as this will
# be merged with user data, some kind of prefix or nesting is preferable
# Use snake_casing to be consistent with the rest of Zola
[extra]

# The theme author info: you!
[author]
name = "Vincent Prouillet"
homepage = "https://vincent.is"

# If this is porting a theme from another static site engine, provide
# the info of the original author here
[original]
author =  "mdo"
homepage = "http://markdotto.com/"
repo = "https://www.github.com/mdo/hyde"
```

A simple theme you can use as an example is [Hyde](https://github.com/Keats/hyde).

## Working on a theme
As a theme is just a site, you can simply use `zola serve` and make changes to your
theme, with live reload working as expected.

Make sure to commit every directory (including `content`) in order for other people
to be able to build the theme from your repository.

## Submitting a theme to the gallery

If you want your theme to be featured in the [themes](@/themes/_index.md) section
of this site, the theme will require two more things:

- `screenshot.png`: a screenshot of the theme in action with a max size of around 2000x1000
- `README.md`: a thorough README explaining how to use the theme and any other information
of importance

The first step is to make sure that the theme meets the following three requirements:

- have a `screenshot.png` of the theme in action with a max size of around 2000x1000
- have a thorough `README.md` explaining how to use the theme and any other information
of importance
- be of reasonably high quality

When your theme is ready, you can submit it to the [themes repository](https://github.com/getzola/themes)
by following the process in the README.

