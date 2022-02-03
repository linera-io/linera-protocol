+++
title = "Sitemap"
weight = 60
+++

Zola will look for a `sitemap.xml` file in the `templates` directory or
use the built-in one.

If your site has more than 30 000 pages, it will automatically split
the links into multiple sitemaps, as recommended by [Google](https://support.google.com/webmasters/answer/183668?hl=en):

> All formats limit a single sitemap to 50MB (uncompressed) and 50,000 URLs. 
> If you have a larger file or more URLs, you will have to break your list into multiple sitemaps. 
> You can optionally create a sitemap index file (a file that points to a list of sitemaps) and submit
> that single index file to Google.

In such a case, Zola will use a template called `split_sitemap_index.xml` to render the index sitemap.


The `sitemap.xml` template gets a single variable:

- `entries`: all pages of the site, as a list of `SitemapEntry`

A `SitemapEntry` has the following fields:

```ts
permalink: String;
updated: String?;
extra: Hashmap<String, Any>?;
```

The `split_sitemap_index.xml` also gets a single variable:

- `sitemaps`: a list of permalinks to the sitemaps
