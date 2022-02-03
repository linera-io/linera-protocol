+++
title = "Installing & using themes"
weight = 20
+++


## Installing a theme

The easiest way to install a theme is to clone its repository in the `themes`
directory.

```bash
$ cd themes
$ git clone THEME_REPO_URL
```

Cloning the repository using Git or another VCS will allow you to easily
update. Alternatively, you can download the files manually and place
them in a folder.

You can find a list of themes [here](@/themes/_index.md).

## Using a theme

Now that you have the theme in your `themes` directory, you need to tell
Zola to use it by setting the `theme` variable in the
[configuration file](@/getting-started/configuration.md). The theme
name has to be the name of the directory you cloned the theme in.
For example, if you cloned a theme in `themes/simple-blog`, the theme name to use
in the configuration file is `simple-blog`.

## Customizing a theme

Any file from the theme can be overridden by creating a file with the same path and name in your `templates` or `static`
directory. Here are a few examples of that, assuming that the theme name is `simple-blog`:

```plain
templates/pages/post.html -> replace themes/simple-blog/templates/pages/post.html
templates/macros.html -> replace themes/simple-blog/templates/macros.html
static/js/site.js -> replace themes/simple-blog/static/js/site.js
```

You can also choose to only override parts of a page if a theme defines some blocks by extending it. If we wanted
to only change a single block from the `post.html` page in the example above, we could do the following:

```
{% extends "simple-blog/templates/pages/post.html" %}

{% block some_block %}
Some custom data
{% endblock %}
```

Most themes will also provide some variables that are meant to be overridden. This happens in the `extra` section
of the [configuration file](@/getting-started/configuration.md).
Let's say a theme uses a `show_twitter` variable and sets it to `false` by default. If you want to set it to `true`,
you can update your `config.toml` like so:

```toml
[extra]
show_twitter = true
```

You can modify files directly in the `themes` directory but this will make updating the theme harder and live reload
won't work with these files.
