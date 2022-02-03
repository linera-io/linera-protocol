+++
title = "Section"
weight = 20
+++

A section is created whenever a directory (or subdirectory) in the `content` section contains an
`_index.md` file.  If a directory does not contain an `_index.md` file, no section will be
created, but Markdown files within that directory will still create pages (known as orphan pages).

The index page (i.e., the page displayed when a user browses to your `base_url`) is a section,
which is created whether or not you add an `_index.md` file at the root of your `content` directory.
If you do not create an `_index.md` file in your content directory, this main content section will
not have any content or metadata.  If you would like to add content or metadata, you can add an
`_index.md` file at the root of the `content` directory and edit it just as you would edit any other
`_index.md` file; your `index.html` template will then have access to that content and metadata.

Any non-Markdown file in a section directory is added to the `assets` collection of the section, as explained in the
[content overview](@/content/overview.md#asset-colocation). These files are then available in the
Markdown file using relative links.

## Drafting
Just like pages sections can be drafted by setting the `draft` option in the front matter. By default this is not done. When a section is drafted it's descendants like pages, subsections and assets will not be processed unless the `--drafts` flag is passed. Note that even pages that don't have a `draft` status will not be processed if one of their parent sections is drafted. 

## Front matter

The `_index.md` file within a directory defines the content and metadata for that section.  To set
the metadata, add front matter to the file.

The TOML front matter is a set of metadata embedded in a file at the beginning of the file enclosed by triple pluses (`+++`).

After the closing `+++`, you can add content, which will be parsed as Markdown and made available
to your templates through the `section.content` variable.

Although none of the front matter variables are mandatory, the opening and closing `+++` are required.

Note that even though the use of TOML is encouraged, YAML front matter is also supported to ease porting
legacy content. In this case the embedded metadata must be enclosed by triple minuses (`---`).

Here is an example `_index.md` with all the available variables. The values provided below are the
default values.


```toml
title = ""

description = ""

# A draft section is only loaded if the `--drafts` flag is passed to `zola build`, `zola serve` or `zola check`.
draft = false

# Used to sort pages by "date", "weight" or "none". See below for more information.
sort_by = "none"

# Used by the parent section to order its subsections.
# Lower values have higher priority.
weight = 0

# Template to use to render this section page.
template = "section.html"

# The given template is applied to ALL pages below the section, recursively.
# If you have several nested sections, each with a page_template set, the page
# will always use the closest to itself.
# However, a page's own `template` variable will always have priority.
# Not set by default.
page_template =

# This sets the number of pages to be displayed per paginated page.
# No pagination will happen if this isn't set or if the value is 0.
paginate_by = 0

# If set, this will be the path used by the paginated page. The page number will be appended after this path.
# The default is page/1.
paginate_path = "page"

# This determines whether to insert a link for each header like the ones you can see on this site if you hover over
# a header.
# The default template can be overridden by creating an `anchor-link.html` file in the `templates` directory.
# This value can be "left", "right" or "none".
insert_anchor_links = "none"

# If set to "true", the section pages will be in the search index. This is only used if
# `build_search_index` is set to "true" in the Zola configuration file.
in_search_index = true

# If set to "true", the section homepage is rendered.
# Useful when the section is used to organize pages (not used directly).
render = true

# This determines whether to redirect when a user lands on the section. Defaults to not being set.
# Useful for the same reason as `render` but when you don't want a 404 when
# landing on the root section page.
# Example: redirect_to = "documentation/content/overview"
redirect_to = 

# If set to "true", the section will pass its pages on to the parent section. Defaults to `false`.
# Useful when the section shouldn't split up the parent section, like
# sections for each year under a posts section.
transparent = false

# Use aliases if you are moving content but want to redirect previous URLs to the
# current one. This takes an array of paths, not URLs.
aliases = []

# If set to "true", a feed file will be generated for this section at the
# section's root path. This is independent of the site-wide variable of the same
# name. The section feed will only include posts from that respective feed, and
# not from any other sections, including sub-sections under that section.
generate_feed = false

# Your own data.
[extra]
```

Keep in mind that any configuration options apply only to the direct pages, not to the subsections' pages.

## Pagination

To enable pagination for a section's pages, set `paginate_by` to a positive number. See
[pagination template documentation](@/templates/pagination.md) for more information
on what variables are available in the template.

You can also change the pagination path (the word displayed while paginated in the URL, like `page/1`)
by setting the `paginate_path` variable, which defaults to `page`.

## Sorting
It is very common for Zola templates to iterate over pages or sections
to display all pages/sections in a given directory.  Consider a very simple
example: a `blog` directory with three files: `blog/Post_1.md`,
`blog/Post_2.md` and `blog/Post_3.md`.  To iterate over these posts and
create a list of links to the posts, a simple template might look like this:

```j2
{% for post in section.pages %}
  <h1><a href="{{ post.permalink }}">{{ post.title }}</a></h1>
{% endfor %}
```

This would iterate over the posts in the order specified
by the `sort_by` variable set in the `_index.md` page for the corresponding
section.  The `sort_by` variable can be given one of three values: `date`,
`weight` or `none`.  If `sort_by` is not set, the pages will be
sorted in the `none` order, which is not intended for sorted content.

Any page that is missing the data it needs to be sorted will be ignored and
won't be rendered. For example, if a page is missing the date variable and its
section sets `sort_by = "date"`, then that page will be ignored.
The terminal will warn you if this occurs.

If several pages have the same date/weight/order, their permalink will be used
to break the tie based on alphabetical order.

## Sorting pages
The `sort_by` front-matter variable can have the following values:

### `date`
This will sort all pages by their `date` field, from the most recent (at the
top of the list) to the oldest (at the bottom of the list).  Each page will
get `page.earlier` and `page.later` variables that contain the pages with
earlier and later dates, respectively.

### `weight`
This will be sort all pages by their `weight` field, from lightest weight
(at the top of the list) to heaviest (at the bottom of the list).  Each
page gets `page.lighter` and `page.heavier` variables that contain the
pages with lighter and heavier weights, respectively.

### Reversed sorting
When iterating through pages, you may wish to use the Tera `reverse` filter,
which reverses the order of the pages.  For example, after using the `reverse` filter,
pages sorted by weight will be sorted from lightest (at the top) to heaviest
(at the bottom); pages sorted by date will be sorted from oldest (at the top)
to newest (at the bottom).

`reverse` has no effect on `page.later`/`page.earlier` or `page.heavier`/`page.lighter`.

If the section is paginated the `paginate_reversed=true` in the front matter of the relevant section should be set instead of using the filter. 

## Sorting subsections
Sorting sections is a bit less flexible: sections can only be sorted by `weight`,
and do not have variables that point to the heavier/lighter sections.

By default, the lightest (lowest `weight`) subsections will be at
the top of the list and the heaviest (highest `weight`) will be at the bottom;
the `reverse` filter reverses this order.

**Note**: Unlike pages, permalinks will **not** be used to break ties between
equally weighted sections. Thus, if the `weight` variable for your section is not set (or if it
is set in a way that produces ties), then your sections will be sorted in
**random** order. Moreover, that order is determined at build time and will
change with each site rebuild.  Thus, if there is any chance that you will
iterate over your sections, you should always assign them a weight.
