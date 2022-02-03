+++
title = "Page"
weight = 30
+++

A page is any file ending with `.md` in the `content` directory, except files
named `_index.md`.

If a file ending with `.md` is named `index.md`, it will generate a page
with the name of its directory (for example, `/content/about/index.md` would
create a page at `[base_url]/about`). (Note the lack of an underscore; if the file
were named `_index.md`, then it would create a **section** at `[base_url]/about`, as
discussed in a previous part of this documentation.  In contrast, naming the file `index.md` will
create a **page** at `[base_url]/about`).

If the file is given any name *other* than `index.md` or `_index.md`, then it will
create a page with that name (without the `.md`). For example, naming a file in the root of your
content directory `about.md` would create a page at `[base_url]/about`.
Another exception to this rule is that a filename starting with a datetime (YYYY-mm-dd or [an RFC3339 datetime](https://www.ietf.org/rfc/rfc3339.txt)) followed by
an underscore (`_`) or a dash (`-`) will use that date as the page date, unless already set
in the front matter. The page name will be anything after `_`/`-`, so the file `2018-10-10-hello-world.md` will
be available at `[base_url]/hello-world`. Note that the full RFC3339 datetime contains colons, which is not a valid
character in a filename on Windows.

As you can see, creating an `about.md` file is equivalent to creating an
`about/index.md` file.  The only difference between the two methods is that creating
the `about` directory allows you to use asset co-location, as discussed in the
[overview](@/content/overview.md#asset-colocation) section.

## Output paths

For any page within your content folder, its output path will be defined by either:

- its `slug` frontmatter key
- its filename

Either way, these proposed path will be sanitized before being used.
If `slugify.paths` is set to `"on"` in the site's config - the default - paths are [slugified](https://en.wikipedia.org/wiki/Clean_URL#Slug). 
If it is set to `"safe"`, only sanitation is performed, with the following characters being removed: `<`, `>`, `:`, `/`, `|`, `?`, `*`, `#`, `\\`, `(`, `)`, `[`, `]` as well as newlines and tabulations. This ensures that the path can be represented on all operating systems.
Additionally, trailing whitespace and dots are removed and whitespaces are replaced by `_`.

If `slugify.paths` is set to `"off"`, no modifications are made.

If you want URLs containing non-ASCII characters, `slugify.paths` needs to be set to `"safe"` or `"off"`.

### Path from frontmatter

The output path for the page will first be read from the `slug` key in the page's frontmatter.

**Example:** (file `content/zines/mlf-kurdistan.md`)

```
+++
title = "Le mouvement des Femmes Libres, à la tête de la libération kurde"
slug = "femmes-libres-libération-kurde"
+++
This is my article.
```

This frontmatter will output the article to `[base_url]/zines/femmes-libres-libération-kurde` with `slugify.paths` set to `"safe"` or `"off"`, and to `[base_url]/zines/femmes-libres-liberation-kurde` with the default value for `slugify.paths` of `"on"`.

### Path from filename

When the article's output path is not specified in the frontmatter, it is extracted from the file's path in the content folder. Consider a file `content/foo/bar/thing.md`. The output path is constructed:
- if the filename is `index.md`, its parent folder name (`bar`) is used as output path
- otherwise, the output path is extracted from `thing` (the filename without the `.md` extension)

If the path found starts with a datetime string (`YYYY-mm-dd` or [a RFC3339 datetime](https://www.ietf.org/rfc/rfc3339.txt)) followed by an underscore (`_`) or a dash (`-`), this date is removed from the output path and will be used as the page date (unless already set in the front-matter). Note that the full RFC3339 datetime contains colons, which is not a valid character in a filename on Windows.

The output path extracted from the file path is then slugified or not, depending on the `slugify.paths` config, as explained previously.

**Example:**
The file `content/blog/2018-10-10-hello-world.md` will yield a page at `[base_url]/blog/hello-world`.

## Front matter

The TOML front matter is a set of metadata embedded in a file at the beginning of the file enclosed
by triple pluses (`+++`).

Although none of the front matter variables are mandatory, the opening and closing `+++` are required.

Note that even though the use of TOML is encouraged, YAML front matter is also supported to ease porting
legacy content. In this case the embedded metadata must be enclosed by triple minuses (`---`).

Here is an example page with all the available variables. The values provided below are the
default values.

```toml
title = ""
description = ""

# The date of the post.
# Two formats are allowed: YYYY-MM-DD (2012-10-02) and RFC3339 (2002-10-02T15:00:00Z).
# Do not wrap dates in quotes; the line below only indicates that there is no default date.
# If the section variable `sort_by` is set to `date`, then any page that lacks a `date`
# will not be rendered.
# Setting this overrides a date set in the filename.
date =

# The last updated date of the post, if different from the date.
# Same format as `date`.
updated =

# The weight as defined on the Section page of the documentation.
# If the section variable `sort_by` is set to `weight`, then any page that lacks a `weight`
# will not be rendered.
weight = 0

# A draft page is only loaded if the `--drafts` flag is passed to `zola build`, `zola serve` or `zola check`.
draft = false

# If set, this slug will be instead of the filename to make the URL.
# The section path will still be used.
slug = ""

# The path the content will appear at.
# If set, it cannot be an empty string and will override both `slug` and the filename.
# The sections' path won't be used.
# It should not start with a `/` and the slash will be removed if it does.
path = ""

# Use aliases if you are moving content but want to redirect previous URLs to the
# current one. This takes an array of paths, not URLs.
aliases = []

# When set to "true", the page will be in the search index. This is only used if
# `build_search_index` is set to "true" in the Zola configuration and the parent section
# hasn't set `in_search_index` to "false" in its front matter.
in_search_index = true

# Template to use to render this page.
template = "page.html"

# The taxonomies for this page. The keys need to be the same as the taxonomy
# names configured in `config.toml` and the values are an array of String objects. For example,
# tags = ["rust", "web"].
[taxonomies]

# Your own data.
[extra]
```

## Summary

You can ask Zola to create a summary if, for example, you only want to show the first
paragraph of the page content in a list.

To do so, add <code>&lt;!-- more --&gt;</code> in your content at the point
where you want the summary to end. The content up to that point will be
available separately in the
[template](@/templates/pages-sections.md#page-variables).

A span element in this position with a `continue-reading` id is created, so you can link directly to it if needed. For example:
`<a href="{{ page.permalink }}#continue-reading">Continue Reading</a>`.
