+++
title = "Feeds"
weight = 50
aliases = ["/documentation/templates/rss/"]
+++

If the site `config.toml` file sets `generate_feed = true`, then Zola will
generate a feed file for the site, named according to the `feed_filename`
setting in `config.toml`, which defaults to `atom.xml`. Given the feed filename
`atom.xml`, the generated file will live at `base_url/atom.xml`, based upon the
`atom.xml` file in the `templates` directory, or the built-in Atom template.

`feed_filename` can be set to any value, but built-in templates are provided
for `atom.xml` (in the preferred Atom 1.0 format), and `rss.xml` (in the RSS
2.0 format). If you choose a different filename (e.g. `feed.xml`), you will
need to provide a template yourself.

**Only pages with a date will be available.**

The feed template gets five variables:

- `config`: the site config
- `feed_url`: the full url to that specific feed
- `last_updated`: the most recent `updated` or `date` field of any post
- `pages`: see [page variables](@/templates/pages-sections.md#page-variables)
  for a detailed description of what this contains
- `lang`: the language code that applies to all of the pages in the feed,
  if the site is multilingual, or `config.default_language` if it is not

Feeds for taxonomy terms get two more variables, using types from the
[taxonomies templates](@/templates/taxonomies.md):

- `taxonomy`: of type `TaxonomyConfig`
- `term`: of type `TaxonomyTerm`, but without `term.pages` (use `pages` instead)

You can also enable separate feeds for each section by setting the
`generate_feed` variable to true in the respective section's front matter.
Section feeds will use the same template as indicated in the `config.toml` file.
Section feeds, in addition to the five feed template variables, get the
`section` variable from the [section template](@/templates/pages-sections.md).

Enable feed autodiscovery allows feed readers and browsers to notify user about a RSS or Atom feed available on your web site. So it is easier for user to subscribe.
As an example this is how it looks like using [Firefox](https://en.wikipedia.org/wiki/Mozilla_Firefox) [Livemarks](https://addons.mozilla.org/en-US/firefox/addon/livemarks/?src=search) addon.

![RSS feed autodiscovery example.](rss_feed.png)

You can enable posts autodiscovery modifying your blog `base.html` template adding the following code in between the `<head>` tags.
```html
{% block rss %}
  <link rel="alternate" type="application/rss+xml" title="RSS" href="{{/* get_url(path="rss.xml", trailing_slash=false) */}}">
{% endblock %}
```
You can as well use an Atom feed using `type="application/atom+xml"` and `path="atom.xml"`.

All pages on your site will refer to your post feed.

In order to enable the tag feeds as well, you can overload the `block rss` using the following code in your `tags/single.html` template.
```html
{% block rss %}
  {% set rss_path = "tags/" ~ term.name ~ "/rss.xml" %}
  <link rel="alternate" type="application/rss+xml" title="RSS" href="{{/* get_url(path=rss_path, trailing_slash=false) */}}">
{% endblock rss %}
```
Each tag page will refer to it's dedicated feed.
